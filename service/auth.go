package service

// HTTP 层会话鉴权（P1.5）：
// 前端 post()/upload 统一带 X-Token 头（页面 URL 的 token 一路透传），
// 本中间件解析出 userId 注入 context——**各接口一律以会话身份为准，不再信任表单里的 userId**。
// WS /chat 不走这里（models.Chat 在 Upgrade 前自行校验 token↔userId，语义相同）。

import (
	"net/http"
	"webapp/utils"

	"github.com/gin-gonic/gin"
)

// AuthRequired 校验 X-Token（兼容 query token），失败返回 401 JSON
func AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("X-Token")
		if token == "" {
			token = c.Query("token")
		}
		uid, ok := utils.UserIdByToken(token)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"code": -1, "msg": "未登录或会话已过期"})
			return
		}
		c.Set("authUid", uid)
		c.Next()
	}
}

// authUid 取当前会话身份（AuthRequired 注入）
func authUid(c *gin.Context) int64 {
	return c.GetInt64("authUid")
}
