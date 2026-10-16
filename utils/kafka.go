package utils

import (
	"context"
	"errors"
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

// kafkaPartitions topic 分区数（缺省 1；>1 的前置条件是 Produce Key=convId 已生效）
func kafkaPartitions() int {
	if n := viper.GetInt("kafka.partitions"); n > 0 {
		return n
	}
	return 1
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
		Topic: topic,
		// kafka.partitions 只影响"首次创建"（已存在 topic 改分区要手工 alter，练手期先给 1）。
		// ⚠️ 扩分区必须与 Produce Key=convId 同时生效：无 Key 时同会话跨分区=多消费者并发 INCR seq=乱序。
		NumPartitions:     kafkaPartitions(),
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

// KafkaMsg 对外暴露 kafka.Message 别名（models 层携带 Partition/Offset 移交提交权，无需直接依赖 kafka-go）
type KafkaMsg = kafka.Message

// ErrCommitDeferred handler 返回此哨兵：本条消息的落库/提交责任已移交冷 writer（batch 档流水线），
// 消费者立即继续 fetch——这是"拆 rec.done 同步等待"的协议面。
var ErrCommitDeferred = errors.New("commit deferred to cold writer")

// Produce 同步投递一条消息到 chat_messages。**Key=convId 是硬约定**：
// 同一会话的消息恒落同一分区，才允许多分区下"每会话单消费者"的 seq 发号前提。
// 返回非 nil 即"broker 未收下"，调用方必须走重投/失败回执，绝不静默丢弃。
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

// CommitKafka 允许冷 writer 在 flush 后跨协程提交 offset（group reader 的 offsets store 内部互斥，安全）。
// 批处理严格 FIFO ⇒ 提交进度天然是已处理前缀，不会越过未完成消息。
func CommitKafka(msgs ...KafkaMsg) error {
	if kafkaReader == nil {
		return fmt.Errorf("kafka reader not initialized")
	}
	return kafkaReader.CommitMessages(context.Background(), msgs...)
}

// SendMsgToKafka 兼容旧调用点的薄封装（忽略错误=仅限"无需回执"的旁路流量使用）
func SendMsgToKafka(msgData []byte, key string) {
	if err := Produce(msgData, key); err != nil {
		Fatal(context.Background(), "Kafka 投递失败", "error", err)
	}
}

// StartKafkaConsumer 启动消费者。提交协议（at-least-once，两种归宿）：
//   - handler 返回 nil               → 消费者提交（inline 档：落库+分发已在本调用内完成）；
//   - handler 返回 ErrCommitDeferred → 不提交、静默继续（batch 档：责任随 kafka.Message 移交冷 writer）；
//   - handler 返回其他 error         → 不提交、退避后继续（崩溃/停机时未提交 offset 由重放兜底）。
// 移交后消费者**不再等待落盘完成**——吞吐与冷写节拍解耦（原 rec.done 同步等待是全链串行瓶颈）。
func StartKafkaConsumer(consumeHandler func(KafkaMsg) error) {
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
			if err := consumeHandler(m); err != nil {
				if errors.Is(err, ErrCommitDeferred) {
					continue // 提交权已移交，静默前进
				}
				log.Println("consume handler failed, offset 不提交, 1s 后继续:", err)
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

// KafkaQuitChan 消费者停机通道（models 聚合器 select 退出用；未启动时为 nil——select nil 通道永不就绪，安全）
func KafkaQuitChan() <-chan struct{} { return kafkaQuit }

// StopKafkaConsumer 停机第一步：发信号并等待消费者协程真正退出（≤10s）。
// **不关 reader**——冷 writer 在排空期还要跨协程 CommitKafka；reader 由最后的 CloseKafka 关闭。
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

// CloseKafka 停机最后一步：关 reader 与 writer。
// 必须在 StopColdWriter 之后——冷 writer 的 flush 提交要活着 reader（见 A7 三步定序）。
func CloseKafka() {
	if kafkaReader != nil {
		kafkaReader.Close()
	}
	if KafkaWriter != nil {
		KafkaWriter.Close()
	}
}
