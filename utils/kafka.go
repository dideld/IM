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

// InitKafka 初始化 Kafka 生产者
func InitKafka() {
	broker := kafkaConf("broker", "host.docker.internal:9092")
	topic := kafkaConf("topic", "chat_messages")
	ensureTopic(broker, topic)
	KafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		// Core Advantage: Async writes!
		Async: true,
	}
	fmt.Println("Kafka Producer (Writer) Initialized, broker:", broker, "topic:", topic)
}

// SendMsgToKafka 发送消息到 Kafka 集群
func SendMsgToKafka(msgData []byte) {
	if KafkaWriter == nil {
		fmt.Println("Kafka is not initialized!")
		return
	}
	err := KafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Value: msgData,
		},
	)
	if err != nil {
		fmt.Println("【严重警告】Kafka 投递失败:", err)
	}
}

// StartKafkaConsumer 启动消费者负责排队落盘 Redis
func StartKafkaConsumer(consumeHandler func([]byte)) {
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
		for {
			select {
			case <-kafkaQuit:
				fmt.Println("Kafka consumer stopping, bye.")
				return
			default:
			}
			m, err := kafkaReader.ReadMessage(context.Background())
			if err != nil {
				log.Println("Kafka Read Error (Retrying...):", err)
				select {
				case <-kafkaQuit:
					return
				default:
				}
				time.Sleep(3 * time.Second) // 避雷：没开 kafka 时防死循环暴搜
				continue
			}
			// 将读到的消息扔给冷热持久化方法
			consumeHandler(m.Value)
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
