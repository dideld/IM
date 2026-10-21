package service

import (
	"net/http"
	"webapp/utils"

	"github.com/gin-gonic/gin"
)

// AuthRequired HTTP 层会话鉴权中间件：从 X-Token 请求头或 token 查询参数提取 token，
// 校验失败返回 401，成功将 userId 注入 gin.Context（key=authUid）。
func AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("X-Token")
		if token == "" {
			token = c.Query("token")
		}
		uid, ok := utils.UserIdByToken(token)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, utils.Response{
				Code:    utils.ErrUnauthorized.Code,
				Message: utils.ErrUnauthorized.Message,
				Data:    nil,
				TraceID: utils.GetTraceID(c),
			})
			return
		}
		c.Set("authUid", uid)
		c.Next()
	}
}

// authUid 取当前会话用户 ID（由 AuthRequired 注入）。
func authUid(c *gin.Context) int64 {
	return c.GetInt64("authUid")
}
