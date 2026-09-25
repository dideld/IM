package utils

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	//"webapp/models"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)
var (DB *gorm.DB
	Red *redis.Client
	ctx =context.Background()
)
func InitConfig() {
	viper.SetConfigName("app")
	viper.AddConfigPath("config")
	err:=viper.ReadInConfig()
	if err!=nil{
		fmt.Println(err)
	}
	// 环境变量覆盖（优先级高于配置文件）：docker-compose 用 MYSQL_DNS/REDIS_ADDR/KAFKA_BROKER 等
	// 指向容器网络内的服务名，同一份 app.yml 宿主机直跑与容器部署互不改文件
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	fmt.Println("config app:",viper.Get("app"))
	fmt.Println("config mysql:",viper.Get("mysql"))

}
func InitMysql() {
	dns := viper.GetString("mysql.dns")
	if dns == "" {
		log.Fatalf("MySQL DNS is empty, please check config/app.yml")
	}

	//自定义日志模板
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold: time.Second, //慢SQL阈值
			LogLevel:      logger.Info,  //级别
			Colorful:      true,        //颜色
		},
	)

	var err error
	DB, err = gorm.Open(mysql.Open(dns), &gorm.Config{Logger: newLogger})
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	fmt.Println("MySQL connected successfully!")
}

func InitRedis() {
	addr := viper.GetString("redis.addr")
	if addr == "" {
		log.Fatalf("Redis address is empty, please check config/app.yml")
	}

	Red = redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     viper.GetString("redis.password"),
		DB:           viper.GetInt("redis.DB"),
		PoolSize:     viper.GetInt("redis.poolSize"),
		MinIdleConns: viper.GetInt("redis.minIdleConn"),
	})

	pong, err := Red.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Redis connected successfully!", pong)
}