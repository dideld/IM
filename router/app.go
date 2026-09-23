package router

import (
	"webapp/service"

	"github.com/gin-contrib/pprof"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	docs "webapp/docs"
	"github.com/gin-gonic/gin"
)
func Router() *gin.Engine{
	r := gin.Default()
	pprof.Register(r)
	docs.SwaggerInfo.BasePath="/"
	//swagger
	swaggerURL := ginSwagger.URL("http://localhost:8080/swagger/doc.json")
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, swaggerURL))
	//静态资源
	r.Static("/asset","asset/")
	r.LoadHTMLGlob("views/**/*")
	//首页与页面
	r.GET("/",service.GetIndex)
	r.GET("/index",service.GetIndex)
	r.GET("/toRegister",service.ToRegister)
	r.GET("/toChat",service.ToChat)
	//WS 入口自带 Upgrade 前的 token↔userId 校验(models.Chat)，不走 HTTP 中间件
	r.GET("/chat",service.Chat)
	//登录注册（尚无会话，公开）
	r.POST("/user/createUser",service.CreateUser)
	r.POST("/user/findUserByNameAndPwd",service.FindUserByNameAndPwd)

	//——以下全部走会话鉴权：身份以 X-Token 解析为准，表单 userId 不再可信——
	authed := r.Group("")
	authed.Use(service.AuthRequired())
	{
		authed.POST("/searchFriends",service.SearchFriends)
		//用户模块
		authed.POST("/user/getUserList",service.GetUserList)
		authed.DELETE("/user/deleteUser",service.DeleteUser)
		authed.POST("/user/updateUser",service.UpdateUser)
		//上传文件
		authed.POST("/attach/upload",service.Upload)
		//读取热层消息（Redis）
		authed.POST("/user/redisMsg", service.RedisMsg)
		//读取冷层历史消息（MySQL convId+seq 游标分页）
		authed.POST("/user/historyMsg", service.HistoryMsg)
		//会话已读（清未读+记录seq游标）
		authed.POST("/user/markRead", service.MarkRead)
		//全部会话未读数
		authed.POST("/user/unreadList", service.UnreadList)
		//会话对端已读游标（私聊"已读"状态）
		authed.POST("/user/readStatus", service.ReadStatus)
		//添加好友
		//authed.GET("/contact/loadfriends",service.LoadFriends)
		authed.POST("/contact/addfriend",service.AddFriend)
		//删除好友
		authed.POST("/contact/deletefriend",service.DeleteFriend)
		//创建群
		authed.POST("/contact/createCommunity",service.CreateCommunity)
		//群列表
		authed.POST("/contact/loadCommunity",service.LoadCommunity)
		authed.POST("/contact/joinGroup",service.JoinGroup)
	}
	return r
}
