package service

import (
	"net/http"
	"webapp/utils"

	"github.com/gin-gonic/gin"
)

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

func authUid(c *gin.Context) int64 {
	return c.GetInt64("authUid")
}

