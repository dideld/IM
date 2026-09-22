package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"webapp/models"
	"webapp/router"
	"webapp/utils"

	"github.com/spf13/viper"
)

func main() {
	utils.InitConfig()
	utils.InitMysql()
	models.Migrate()            // 0. 同步表结构（含 message 冷存储表）
	utils.InitRedis()
	// 雪花机器码显式化：多实例部署必须为每个实例配置不同的 snowflake.workerId(0~31)，
	// 否则同毫秒撞 ID，冷存储主键冲突会丢消息
	if err := utils.InitSnowflake(viper.GetInt64("snowflake.workerId")); err != nil {
		log.Fatalf("Snowflake 初始化失败: %v（workerId 需在 0~31 且实例间唯一）", err)
	}
	models.StartColdWriter()    // 冷存储批量落盘 worker（须在 DB 就绪后、消费者启动前）
	utils.InitKafka()           // 1. 先初始化 Kafka Producer
	models.StartKafkaConsumer() // 2. 再挂载消费者监控和持久化存库逻辑
	go monitor()

	r := router.Router()
	srv := &http.Server{Addr: viper.GetString("port.server"), Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 监听失败: %v", err)
		}
	}()

	// 优雅停机：停接新请求 → 停消费（不再向冷队列积压）→ 排空 coldCh 落盘 → 关 Kafka
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("=====正在优雅停机=====")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Println("HTTP Shutdown:", err)
	}
	utils.CloseKafka()
	models.StopColdWriter()
	fmt.Println("=====停机完成，冷数据已 flush=====")
}
func monitor() {

	for {

		fmt.Println("=====服务器状态=====")

		//在线连接数
		fmt.Println("连接数:", models.WsHub.ClientCount())

		//goroutine数量
		fmt.Println("goroutine:", runtime.NumGoroutine())

		//内存
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		fmt.Println("内存(MB):", m.Alloc/1024/1024)

		fmt.Println("====================")

		time.Sleep(10 * time.Second)
	}
}
