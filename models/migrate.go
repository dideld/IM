package models

import "webapp/utils"

// Migrate 启动时同步表结构（依赖 utils.DB 已初始化）
// message 冷存储表的 conv_key 索引等结构以此处模型定义为准
func Migrate() {
	if err := utils.DB.AutoMigrate(&Message{}, &UserBasic{}, &Contact{}, &Community{}, &ReadCursor{}); err != nil {
		panic("AutoMigrate failed: " + err.Error())
	}
}
