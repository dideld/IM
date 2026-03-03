package router

import (
	"webapp/service"

	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	docs "webapp/docs"
	"github.com/gin-gonic/gin"
)
func Router() *gin.Engine{
	r := gin.Default()
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
	//发送消息
	r.GET("/user/sendMsg",service.SendMsg)
	//发送消息
	r.GET("/user/sendUserMsg",service.SendUserMsg)
	//上传文件
	r.POST("/attach/upload",service.Upload)
	//读取消息
	r.POST("/user/redisMsg", service.RedisMsg)
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