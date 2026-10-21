package router

import (
	"webapp/middleware"
	"webapp/service"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	docs "webapp/docs"
)

// Router 初始化 Gin 引擎并注册全部路由。
func Router() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.TraceID())
	pprof.Register(r)

	docs.SwaggerInfo.BasePath = "/"
	swaggerURL := ginSwagger.URL("http://localhost:8080/swagger/doc.json")
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, swaggerURL))

	r.Static("/asset", "asset/")
	r.LoadHTMLGlob("views/**/*")

	// ===== 公开路由（无需登录）=====
	r.GET("/", service.GetIndex)
	r.GET("/index", service.GetIndex)
	r.GET("/toRegister", service.ToRegister)
	r.GET("/toChat", service.ToChat)
	// NOTE: WS 入口在 models.Chat 内自行校验 token，不走 HTTP 中间件
	r.GET("/chat", service.Chat)
	r.POST("/user/createUser", service.CreateUser)
	r.POST("/user/findUserByNameAndPwd", service.FindUserByNameAndPwd)

	// ===== 会话鉴权路由 =====
	authed := r.Group("")
	authed.Use(service.AuthRequired())
	{
		authed.POST("/searchFriends", service.SearchFriends)

		// 用户模块
		authed.POST("/user/getUserList", service.GetUserList)
		authed.DELETE("/user/deleteUser", service.DeleteUser)
		authed.POST("/user/updateUser", service.UpdateUser)

		// 文件上传
		authed.POST("/attach/upload", service.Upload)

		// 消息模块
		authed.POST("/user/redisMsg", service.RedisMsg)
		authed.POST("/user/historyMsg", service.HistoryMsg)
		authed.POST("/user/markRead", service.MarkRead)
		authed.POST("/user/unreadList", service.UnreadList)
		authed.POST("/user/readStatus", service.ReadStatus)

		// 好友模块
		authed.POST("/contact/addfriend", service.AddFriend)
		authed.POST("/contact/deletefriend", service.DeleteFriend)

		// 群模块
		authed.POST("/contact/createCommunity", service.CreateCommunity)
		authed.POST("/contact/loadCommunity", service.LoadCommunity)
		authed.POST("/contact/joinGroup", service.JoinGroup)
	}
	return r
}
