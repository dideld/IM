package utils

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

var (
	KafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader
	kafkaQuit   chan struct{}
	kafkaOnce   sync.Once
)

// kafkaConf 读取 kafka.* 配置，缺省时回退本地开发默认值（保证删掉配置也能起服务）
func kafkaConf(key, def string) string {
	if v := viper.GetString("kafka." + key); v != "" {
		return v
	}
	return def
}

// ensureTopic 启动时幂等确保 topic 存在。
// 必须早于消费者启动：kafka-go 的 group 若在 topic 自动创建之前入组，
// 会拿到空分区分配且此后不触发再均衡，消费者将永久静默不消费。
func ensureTopic(broker, topic string) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		fmt.Println("Kafka ensureTopic 连接失败(非致命，broker 未就绪时消费端仍会重试):", err)
		return
	}
	defer conn.Close()

	c, err := conn.Controller()
	if err != nil {
		fmt.Println("Kafka ensureTopic 取 controller 失败:", err)
		return
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port)))
	if err != nil {
		fmt.Println("Kafka ensureTopic 连接 controller 失败:", err)
		return
	}
	defer cc.Close()

	if err := cc.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil && !strings.Contains(err.Error(), "already exists") {
		fmt.Println("Kafka ensureTopic 创建 topic 失败:", err)
		return
	}
	fmt.Println("Kafka topic ensured:", topic)
}

// InitKafka 初始化 Kafka 生产者（同步模式）
// 端到端可靠的基石：Async=false，WriteMessages 在 broker 确认后才返回，错误必须能被调用方感知
// （曾经的 Async 吞错造成"服务端看似正常、落库为零"的事故形态）。超上限仍失败由应用层重投队列兜底。
func InitKafka() {
	broker := kafkaConf("broker", "host.docker.internal:9092")
	topic := kafkaConf("topic", "chat_messages")
	ensureTopic(broker, topic)
	KafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Async:        false,
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 5 * time.Millisecond,
		MaxAttempts:  3,
		WriteTimeout: 5 * time.Second,
	}
	fmt.Println("Kafka Producer (sync writer) Initialized, broker:", broker, "topic:", topic)
}

// Produce 同步投递一条消息到 chat_messages。
// 返回非 nil 即"broker 未收下"，调用方必须走重投/失败回执，绝不静默丢弃。
func Produce(msgData []byte) error {
	if KafkaWriter == nil {
		return fmt.Errorf("kafka writer not initialized")
	}
	return KafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Value: msgData,
		},
	)
}

// SendMsgToKafka 兼容旧调用点的薄封装（忽略错误=仅限"无需回执"的旁路流量使用）
func SendMsgToKafka(msgData []byte) {
	if err := Produce(msgData); err != nil {
		fmt.Println("【严重警告】Kafka 投递失败:", err)
	}
}

// StartKafkaConsumer 启动消费者负责排队落盘 Redis。
// 手动提交（at-least-once）：handler 返回 nil 才 commit offset；返回 error 不提交、退避后由 Kafka 重投。
// handler 内部负责自身的有界重试/死信/失败 ACK，避免毒消息无限占住分区。
func StartKafkaConsumer(consumeHandler func([]byte) error) {
	kafkaQuit = make(chan struct{})
	kafkaOnce = sync.Once{}
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaConf("broker", "host.docker.internal:9092")},
		GroupID: kafkaConf("consumerGroup", "chat_redis_saver_group"), // 支持横向扩展，开再多服也是同一个组排队
		Topic:   kafkaConf("topic", "chat_messages"),
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  10 * time.Millisecond,
	})

	fmt.Println("Kafka Consumer (Reader) Started and Listening...")

	go func() {
		ctx := context.Background()
		for {
			select {
			case <-kafkaQuit:
				fmt.Println("Kafka consumer stopping, bye.")
				return
			default:
			}
			m, err := kafkaReader.FetchMessage(ctx)
			if err != nil {
				log.Println("Kafka Fetch Error (Retrying...):", err)
				select {
				case <-kafkaQuit:
					return
				default:
				}
				time.Sleep(3 * time.Second) // 避雷：没开 kafka 时防死循环暴搜
				continue
			}
			// 落库+分发+ACK 全部完成（handler 返回 nil）才提交 offset；
			// 返回 error 不提交，退避后 Kafka 重投（at-least-once，幂等由 handler 内部保证）
			if err := consumeHandler(m.Value); err != nil {
				log.Println("consume handler failed, offset 不提交, 1s 后重投:", err)
				select {
				case <-time.After(time.Second):
				case <-kafkaQuit:
					return
				}
				continue
			}
			if err := kafkaReader.CommitMessages(ctx, m); err != nil {
				log.Println("Kafka commit error(下轮重投由幂等兜底):", err)
			}
		}
	}()
}

// CloseKafka 优雅停机：停消费循环、关 reader 与 writer
func CloseKafka() {
	if kafkaQuit != nil {
		kafkaOnce.Do(func() { close(kafkaQuit) })
	}
	if kafkaReader != nil {
		kafkaReader.Close()
	}
	if KafkaWriter != nil {
		KafkaWriter.Close()
	}
}
