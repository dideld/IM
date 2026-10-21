package utils

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	DB  *gorm.DB
	Red *redis.Client
	ctx = context.Background()
)

// InitConfig 读取 config/app.yml 并开启环境变量覆盖（env key 中 "." 替换为 "_"）。
func InitConfig() {
	viper.SetConfigName("app")
	viper.AddConfigPath("config")
	err := viper.ReadInConfig()
	if err != nil {
		Error(ctx, "读取配置失败", "error", err)
	}
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}

// InitMysql 初始化 GORM MySQL 连接（慢 SQL 阈值 1s，低于 WARN 级别静默）。
func InitMysql() {
	dns := viper.GetString("mysql.dns")
	if dns == "" {
		Fatal(ctx, "MySQL DNS is empty, please check config/app.yml")
	}

	newLogger := logger.New(
		&gormSlogLogger{},
		logger.Config{
			SlowThreshold: time.Second,
			LogLevel:      logger.Warn,
			Colorful:      false,
		},
	)

	var err error
	DB, err = gorm.Open(mysql.Open(dns), &gorm.Config{Logger: newLogger})
	if err != nil {
		Fatal(ctx, "Failed to connect to MySQL", "error", err)
	}
}

// InitRedis 初始化 Redis 客户端并 Ping 验证连通性。
func InitRedis() {
	addr := viper.GetString("redis.addr")
	if addr == "" {
		Fatal(ctx, "Redis address is empty, please check config/app.yml")
	}

	Red = redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     viper.GetString("redis.password"),
		DB:           viper.GetInt("redis.DB"),
		PoolSize:     viper.GetInt("redis.poolSize"),
		MinIdleConns: viper.GetInt("redis.minIdleConn"),
	})

	_, err := Red.Ping(ctx).Result()
	if err != nil {
		Fatal(ctx, "Failed to connect to Redis", "error", err)
	}
}
