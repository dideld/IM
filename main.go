package main

import (
	"context"
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
	utils.InitLogger(viper.GetString("log.level"))
	utils.InitMysql()
	models.Migrate()
	utils.InitRedis()
	if err := utils.InitSnowflake(viper.GetInt64("snowflake.workerId")); err != nil {
		utils.Fatal(context.Background(), "Snowflake 初始化失败", "error", err)
	}
	models.StartColdWriter()
	utils.InitKafka()
	models.StartKafkaConsumer()
	go monitor()

	r := router.Router()
	srv := &http.Server{Addr: viper.GetString("port.server"), Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			utils.Fatal(context.Background(), "HTTP 监听失败", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	utils.Info(context.Background(), "正在优雅停机")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		utils.Error(context.Background(), "HTTP Shutdown", "error", err)
	}
	utils.StopKafkaConsumer()
	models.StopColdWriter()
	utils.CloseKafkaProducer()
	utils.Info(context.Background(), "停机完成，冷数据已 flush")
}

func monitor() {
	for {
		cl, cc := models.ColdQueueLen()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		utils.Info(context.Background(), "服务器状态",
			"connections", models.WsHub.ClientCount(),
			"goroutines", runtime.NumGoroutine(),
			"memory_mb", m.Alloc/1024/1024,
			"cold_queue", cl, "cold_cap", cc)
		if cl > cc*8/10 {
			utils.Warn(context.Background(), "冷队列水位过高", "len", cl, "cap", cc)
		}
		time.Sleep(10 * time.Second)
	}
}

