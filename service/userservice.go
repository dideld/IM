package service

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"webapp/models"
	"webapp/utils"
	"time"
	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// GetUserList
// @Tags 用户模块
// @Summary 所有用户
// @Produce json
// @Success 200 {string} json{"code"，"message"} // 修改返回类型
// @Router /user/getUserList [post]
func GetUserList(c *gin.Context){

	data:=models.GetUserList()
	c.JSON(200, gin.H{
			"code":    0, //  0成功   -1失败
			"message": "用户名已注册",
			"data":    data,
	})
}

// CreateUser
// @Tags 用户模块
// @Summary 新增用户
// @param name query string flase "用户名"
// @param password query string flase "密码"
// @param repassword query string flase "确认密码"
// @Produce json  
// @Success 200 {string} json{"code"，"message"} // 修改返回类型
// @Router /user/createUser [post]
func CreateUser(c *gin.Context){
	user:=models.UserBasic{}
	// user.Name=c.Query("name")
	// password:=c.Query("password")
	// repassword:=c.Query("repassword")
	user.Name=c.Request.FormValue("name")
	password:=c.Request.FormValue("password")
	repassword:=c.Request.FormValue("Identity")
	data:=models.FindUserByName(user.Name)

	salt:=fmt.Sprintf("%06d",rand.Int32())
	if user.Name == "" || password == "" || repassword == "" {
		c.JSON(200, gin.H{
			"code":    -1, //  0成功   -1失败
			"message": "用户名或密码不能为空！",
			"data":    user,
		})
		return
	}
	if data.Name!=""{
	c.JSON(200, gin.H{
			"code":    -1, //  0成功   -1失败
			"message": "用户已注册",
			"data":    user,
	})
	return
	}

	if password!=repassword{
	c.JSON(200, gin.H{
			"code":    -1, //  0成功   -1失败
			"message": "俩次密码不一致",
			"data":    user,
	})
	return
	}
	// user.PassWord=password
	user.PassWord=utils.MakePassword(password,salt)
	user.Salt=salt
	res := models.CreateUser(user)   // 接收返回值
	println("插入错误:", res.Error)
	println("影响行数:", res.RowsAffected)
	c.JSON(200, gin.H{
			"code":    0, //  0成功   -1失败
			"message": "新增用户成功",
			"data":    user,
	})
}

// DeleteUser
// @Tags 用户模块
// @Summary 删除用户
// @param id query string flase "id"
// @Produce json  
// @Success 200 {string} json{"code"，"message"} // 修改返回类型
// @Router /user/deleteUser [delete]
func DeleteUser(c *gin.Context){
	user:=models.UserBasic{}
	id,_:=strconv.Atoi(c.Query("id"))
	user.ID=uint(id)
	models.DeleteUser(user)
	c.JSON(200, gin.H{
			"code":    0, //  0成功   -1失败
			"message": "删除用户成功",
			"data":    user,
	})
}

// UpdateUser
// @Tags 用户模块
// @Summary 修改用户
// @param id formData string flase "id"
// @param name formData string flase "name"
// @param password formData string flase "password"
// @param phone formData string flase "phone"
// @param email formData string flase "email"
// @Produce json  
// @Success 200 {string} json{"code"，"message"} // 修改返回类型
// @Router /user/updateUser [put]
func UpdateUser(c *gin.Context){
	user:=models.UserBasic{}
	id,_:=strconv.Atoi(c.PostForm("id"))
	user.ID=uint(id)
	user.Name=c.PostForm("name")
	user.PassWord=c.PostForm("password")
	user.Phone=c.PostForm("phone")
	user.Email=c.PostForm("email")
	models.UpdateUser(user)
	_,err:=govalidator.ValidateStruct(user)
	if err!=nil{
		fmt.Println(err)
			c.JSON(200, gin.H{
			"code":    -1, //  0成功   -1失败
			"message": "修改参数失败",
			"data":    user,
	})
	}else{
			c.JSON(200, gin.H{
			"code":    0, //  0成功   -1失败
			"message": "修改用户成功",
			"data":    user,
	})
	}

}
// GetUserList
// @Tags 用户模块
// @Summary 登录
// @param name formData string flase "用户名"
// @param password formData string flase "密码"
// @Produce json
// @Success 200 {string} json{"code"，"message"} // 修改返回类型
// @Router /user/findUserByNameAndPwd [post]
func FindUserByNameAndPwd(c *gin.Context){

	data:=models.UserBasic{}
	// name:=c.Query("name")
	// password:=c.Query("password")
	name:=c.Request.FormValue("name")
	password:=c.Request.FormValue("password")
	fmt.Println(name, password)
	user:=models.FindUserByName(name)
	if user.Name==""{
		c.JSON(200, gin.H{
			"code":    -1, //  0成功   -1失败
			"message": "该用户不存在",
			"data":    data,
	})
	return
	}
	flag:=utils.ValidPassword(password,user.Salt,user.PassWord)
	if !flag{
			c.JSON(200, gin.H{
			"code":    -1, //  0成功   -1失败
			"message": "密码错误",
			"data":    data,
	})
	return
	}
	pwd:=utils.MakePassword(password,user.Salt)
	data=*models.FindUserByNameAndPwd(name,pwd)
		c.JSON(200, gin.H{
			"code":    0, //  0成功   -1失败
			"message": "登录成功",
			"data":    data,
	})
}
//防止跨域站点伪造请求
var upGrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}
func SendMsg(c *gin.Context){
	ws,err:=upGrader.Upgrade(c.Writer,c.Request,nil)
	if err!=nil{
		fmt.Println(err)
		return
	}
	defer func(ws *websocket.Conn){
		err=ws.Close()
		if err!=nil{
		fmt.Println(err)
		return
		}	
	}(ws)
	MsgHandler(ws,c)
}
func MsgHandler(ws *websocket.Conn,c *gin.Context){
	msg,err:=utils.Subscribe(c,utils.PublishKey)
	if err!=nil{
		fmt.Println(err)
		return
	}
	tm:=time.Now().Format("2006-01-02 15:04:05")
	m:=fmt.Sprintf("[ws][%s]:%s",tm,msg)
	err=ws.WriteMessage(1,[]byte(m))
	if err!=nil{
		fmt.Println(err)
	}
}
func SendUserMsg(c *gin.Context){
	models.Chat(c.Writer,c.Request)
	
}
func SearchFriends(c *gin.Context){
	id,_:=strconv.Atoi(c.Request.FormValue("userId"))
	users:=models.SearchFriend(uint(id))
	// c.JSON(200,gin.H{
	// 	"code":  0,
	// 	"message":"查询好友列表成功！",
	// 	"data": users,
	// })
	utils.RespOkList(c.Writer,users,len(users))
}

func AddFriend(c *gin.Context){
	userId,_:=strconv.Atoi(c.Request.FormValue("userId"))
	targetId,_:=strconv.Atoi(c.Request.FormValue("targetId"))
	code:=models.AddFriend(uint(userId),uint(targetId))
	if code == 0{
			utils.RespOk(c.Writer,code,"添加成功")

	}else if code==1{
		utils.RespFail(c.Writer,"不能添加自己")
	}else if code==-2{
		utils.RespFail(c.Writer,"没有找到此用户")
	}else if code==-3{
		utils.RespFail(c.Writer,"好友ID不能为空")
	}else if code==2{
		utils.RespFail(c.Writer,"不能添加已经添加过的好友")
	}else{
		utils.RespFail(c.Writer,"添加失败")
	}
}