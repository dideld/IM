package utils

import (
	"context"
	"errors"
	"fmt"
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
	KafkaWriter       *kafka.Writer
	kafkaReader       *kafka.Reader
	kafkaQuit         chan struct{}
	kafkaOnce         sync.Once
	kafkaStopping     atomic.Bool    // 停机信号：消费者入队前检查，模型层靠它放弃入队走重放
	kafkaConsumerDone chan struct{}  // 消费者协程退出握手：排空冷队列前必须确认无人再入队
)

// kafkaConf 读取 kafka.* 配置，缺省回退本地开发默认值。
func kafkaConf(key, def string) string {
	if v := viper.GetString("kafka." + key); v != "" {
		return v
	}
	return def
}

// kafkaPartitions topic 分区数（缺省 1；>1 前置条件是 Produce Key=convId）。
func kafkaPartitions() int {
	if n := viper.GetInt("kafka.partitions"); n > 0 {
		return n
	}
	return 1
}

// ensureTopic 启动时幂等确保 topic 存在。
// 必须早于消费者启动：kafka-go 的 group 在 topic 自动创建前入组会拿到空分区且不触发再均衡。
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
		Topic:           topic,
		NumPartitions:     kafkaPartitions(),
		ReplicationFactor: 1,
	}); err != nil && !strings.Contains(err.Error(), "already exists") {
		Error(context.Background(), "Kafka ensureTopic 创建 topic 失败", "error", err)
		return
	}
	Info(context.Background(), "Kafka topic ensured", "topic", topic)
}

// InitKafka 初始化 Kafka 生产者（同步模式，WriteMessages 在 broker 确认后才返回）。
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

// KafkaMsg 对外暴露 kafka.Message 别名（models 层携带 Partition/Offset 移交提交权）。
type KafkaMsg = kafka.Message

// ErrCommitDeferred handler 返回此哨兵：本条消息的落库/提交责任已移交冷 writer，消费者立即继续 fetch。
var ErrCommitDeferred = errors.New("commit deferred to cold writer")

// Produce 同步投递一条消息到 chat_messages。Key=convId 是硬约定（同会话恒落同分区）。
// 返回非 nil 即 broker 未收下，调用方必须走重投/失败回执。
func Produce(msgData []byte, key string) error {
	if KafkaWriter == nil {
		return fmt.Errorf("kafka writer not initialized")
	}
	return KafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: msgData,
		},
	)
}

// CommitKafka 冷 writer flush 后跨协程提交 offset（reader 内部互斥，安全）。
func CommitKafka(msgs ...KafkaMsg) error {
	if kafkaReader == nil {
		return fmt.Errorf("kafka reader not initialized")
	}
	return kafkaReader.CommitMessages(context.Background(), msgs...)
}

// SendMsgToKafka 兼容旧调用点的薄封装（无需回执的旁路流量）。
func SendMsgToKafka(msgData []byte, key string) {
	if err := Produce(msgData, key); err != nil {
		Fatal(context.Background(), "Kafka 投递失败", "error", err)
	}
}

// StartKafkaConsumer 启动消费者。handler 返回 nil → 消费者提交；
// 返回 ErrCommitDeferred → 不提交（batch 档移交冷 writer）；其他 error → 不提交，退避后继续。
func StartKafkaConsumer(consumeHandler func(KafkaMsg) error) {
	kafkaQuit = make(chan struct{})
	kafkaOnce = sync.Once{}
	kafkaConsumerDone = make(chan struct{})
	kafkaStopping.Store(false)
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaConf("broker", "host.docker.internal:9092")},
		GroupID: kafkaConf("consumerGroup", "chat_redis_saver_group"),
		Topic:   kafkaConf("topic", "chat_messages"),
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  10 * time.Millisecond,
	})

	Info(context.Background(), "Kafka Consumer (Reader) Started and Listening...")

	go func() {
		defer close(kafkaConsumerDone)
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
				Error(ctx, "Kafka Fetch Error (Retrying)", "error", err)
				select {
				case <-kafkaQuit:
					return
				default:
				}
				time.Sleep(3 * time.Second)
				continue
			}
			if err := consumeHandler(m); err != nil {
				if errors.Is(err, ErrCommitDeferred) {
					continue
				}
				Error(ctx, "consume handler failed, offset 不提交", "error", err)
				select {
				case <-time.After(time.Second):
				case <-kafkaQuit:
					return
				}
				continue
			}
			if err := kafkaReader.CommitMessages(ctx, m); err != nil {
				Error(ctx, "Kafka commit error", "error", err)
			}
		}
	}()
}

// KafkaStopping 停机信号（模型层入队前检查，收到信号则放弃入队走重放）。
func KafkaStopping() bool { return kafkaStopping.Load() }

// KafkaQuitChan 消费者停机通道（models 聚合器 select 退出用；未启动时为 nil，select nil 永不就绪）。
func KafkaQuitChan() <-chan struct{} { return kafkaQuit }

// StopKafkaConsumer 停机第一步：发信号并等待消费者协程退出（≤10s）。不关 reader（冷 writer 还要提交）。
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
}

// CloseKafka 停机最后一步：关 reader 与 writer。必须在 StopColdWriter 之后。
func CloseKafka() {
	if kafkaReader != nil {
		kafkaReader.Close()
	}
	if KafkaWriter != nil {
		KafkaWriter.Close()
	}
}
