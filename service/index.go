package service

import (
	"strconv"
	"webapp/models"

	"github.com/gin-gonic/gin"
)

// GetIndex 首页（渲染 HTML 页面）。
func GetIndex(c *gin.Context) {
	c.HTML(200, "login.html", nil)
}

// ToRegister 注册页面。
func ToRegister(c *gin.Context) {
	c.HTML(200, "register.html", "register")
}

// ToChat 聊天页面入口。
func ToChat(c *gin.Context) {
	userId, _ := strconv.Atoi(c.Query("userId"))
	token := c.Query("token")

	user := models.UserBasic{}
	user.ID = uint(userId)
	user.Identity = token

	c.HTML(200, "chat/index.html", user)
}

// Chat WebSocket 连接入口（Upgrade 前在 models.Chat 内自行校验 token）。
func Chat(c *gin.Context) {
	models.Chat(c.Writer, c.Request)
}
