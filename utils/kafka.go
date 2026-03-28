package utils

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	KafkaWriter *kafka.Writer
)

const TopicChatMessages = "chat_messages"
const KafkaBroker = "127.0.0.1:9092"

// InitKafka 初始化 Kafka 生产者
func InitKafka() {
	KafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(KafkaBroker),
		Topic:    TopicChatMessages,
		Balancer: &kafka.LeastBytes{},
		// Core Advantage: Async writes!
		Async:    true,
	}
	fmt.Println("Kafka Producer (Writer) Initialized")
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
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		GroupID:  "chat_redis_saver_group", // 支持横向扩展，开再多服也是同一个组排队
		Topic:    TopicChatMessages,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  10 * time.Millisecond,
	})

	fmt.Println("Kafka Consumer (Reader) Started and Listening...")

	go func() {
		for {
			m, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Println("Kafka Read Error (Retrying...):", err)
				time.Sleep(3 * time.Second) // 避雷：没开 kafka 时防死循环暴搜
				continue
			}
			// 将读到的消息扔给 Redis 保存方法
			consumeHandler(m.Value)
		}
	}()
}
