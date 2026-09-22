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
	//首页
	r.GET("/",service.GetIndex)
	r.GET("/index",service.GetIndex)
	r.GET("/toRegister",service.ToRegister)
	r.GET("/toChat",service.ToChat)
	r.GET("/chat",service.Chat)
	r.POST("/searchFriends",service.SearchFriends)

	//用户模块
	r.POST("/user/getUserList",service.GetUserList)
	r.POST("/user/createUser",service.CreateUser)
	r.DELETE("/user/deleteUser",service.DeleteUser)
	r.POST("/user/updateUser",service.UpdateUser)
	r.POST("/user/findUserByNameAndPwd",service.FindUserByNameAndPwd)
	//上传文件
	r.POST("/attach/upload",service.Upload)
	//读取热层消息（Redis）
	r.POST("/user/redisMsg", service.RedisMsg)
	//读取冷层历史消息（MySQL convId+seq 游标分页）
	r.POST("/user/historyMsg", service.HistoryMsg)
	//会话已读（清未读+记录seq游标）
	r.POST("/user/markRead", service.MarkRead)
	//全部会话未读数
	r.POST("/user/unreadList", service.UnreadList)
	//添加好友
	//r.GET("/contact/loadfriends",service.LoadFriends)
	r.POST("/contact/addfriend",service.AddFriend)
	//删除好友
	r.POST("/contact/deletefriend",service.DeleteFriend)
	//创建群
	r.POST("/contact/createCommunity",service.CreateCommunity)
	//群列表
	r.POST("/contact/loadCommunity",service.LoadCommunity)
	r.POST("/contact/joinGroup",service.JoinGroup)
	return r
}