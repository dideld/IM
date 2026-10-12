package utils

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

var (
	KafkaWriter      *kafka.Writer
	kafkaReader      *kafka.Reader
	kafkaQuit        chan struct{}
	kafkaOnce        sync.Once
	kafkaStopping    atomic.Bool    // 停机信号：消费者入队前检查，模型层靠它放弃入队走重放
	kafkaConsumerDone chan struct{} // 消费者协程退出握手（A7：排空冷队列前必须确认无人再入队）
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
		Warn(context.Background(), "Kafka ensureTopic 连接失败(非致命，broker 未就绪时消费端仍会重试)", "error", err)
		return
	}
	defer conn.Close()

	c, err := conn.Controller()
	if err != nil {
		Warn(context.Background(), "Kafka ensureTopic 取 controller 失败", "error", err)
		return
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port)))
	if err != nil {
		Warn(context.Background(), "Kafka ensureTopic 连接 controller 失败", "error", err)
		return
	}
	defer cc.Close()

	if err := cc.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil && !strings.Contains(err.Error(), "already exists") {
		Error(context.Background(), "Kafka ensureTopic 创建 topic 失败", "error", err)
		return
	}
	Info(context.Background(), "Kafka topic ensured", "topic", topic)
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
	Info(context.Background(), "Kafka Producer (sync writer) Initialized", "broker", broker, "topic", topic)
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
		Fatal(context.Background(), "Kafka 投递失败", "error", err)
	}
}

// StartKafkaConsumer 启动消费者负责排队落盘 Redis。
// 手动提交（at-least-once）：handler 返回 nil 才 commit offset；返回 error 不提交、退避后由 Kafka 重投。
// handler 内部负责自身的有界重试/死信/失败 ACK，避免毒消息无限占住分区。
func StartKafkaConsumer(consumeHandler func([]byte) error) {
	kafkaQuit = make(chan struct{})
	kafkaOnce = sync.Once{}
	kafkaConsumerDone = make(chan struct{})
	kafkaStopping.Store(false)
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaConf("broker", "host.docker.internal:9092")},
		GroupID: kafkaConf("consumerGroup", "chat_redis_saver_group"), // 支持横向扩展，开再多服也是同一个组排队
		Topic:   kafkaConf("topic", "chat_messages"),
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  10 * time.Millisecond,
	})

	Info(context.Background(), "Kafka Consumer (Reader) Started and Listening...")

	go func() {
		defer close(kafkaConsumerDone) // 退出握手：A7 排空冷队列前必须等到这里
		ctx := context.Background()
		for {
			select {
			case <-kafkaQuit:
				Info(ctx, "Kafka consumer stopping, bye.")
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

// KafkaStopping 停机信号（模型层入队前检查：停摆等待中收到信号则放弃入队、offset 不提交走重放）
func KafkaStopping() bool { return kafkaStopping.Load() }

// StopKafkaConsumer 停机第一步：发信号并等待消费者协程真正退出（≤10s），再关 reader。
// 只有确认无人再向 coldCh 入队，StopColdWriter 的"排空"才是可终结的。
func StopKafkaConsumer() {
	if kafkaQuit == nil {
		return
	}
	kafkaStopping.Store(true)
	kafkaOnce.Do(func() { close(kafkaQuit) })
	if kafkaConsumerDone != nil {
		select {
		case <-kafkaConsumerDone:
		case <-time.After(10 * time.Second):
			Warn(context.Background(), "Kafka 消费者退出超时，强制继续停机")
		}
	}
	if kafkaReader != nil {
		kafkaReader.Close()
	}
}

// CloseKafkaProducer 停机最后一步：关生产者（须在冷 writer 排空之后——收尾期的分发/ACK 不再产生新投递需求）
func CloseKafkaProducer() {
	if KafkaWriter != nil {
		KafkaWriter.Close()
	}
}
