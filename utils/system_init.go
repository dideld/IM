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
