package main

import (
	"webapp/router"
	"webapp/utils"
	"webapp/models"
	"github.com/spf13/viper"
	"fmt"
	"time"
	"runtime"
)

func main() {
	utils.InitConfig()
	utils.InitMysql()
	utils.InitRedis()
	utils.InitKafka()           // 1. 先初始化 Kafka Producer
	models.StartKafkaConsumer() // 2. 再挂载消费者监控和持久化存库逻辑
	go monitor()
	r := router.Router()
	r.Run(viper.GetString("port.server"))
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