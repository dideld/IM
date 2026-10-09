package service

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"webapp/models"
	"webapp/utils"
	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
)

// GetUserList
// @Tags 用户模块
// @Summary 所有用户
// @Produce json
// @Success 200 {string} json{"code"，"message"} //  修改返回类型
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
// @Success 200 {string} json{"code"，"message"} //  修改返回类型
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
// @Success 200 {string} json{"code"，"message"} //  修改返回类型
// @Router /user/deleteUser [delete]
func DeleteUser(c *gin.Context){
	user:=models.UserBasic{}
	id,_:=strconv.Atoi(c.Query("id"))
	if int64(id) != authUid(c) {
		utils.RespFail(c.Writer, "无权删除他人")
		return
	}
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
// @param avator formData string flase "icon"
// @param email formData string flase "email"
// @Produce json
// @Success 200 {string} json{"code"，"message"} //  修改返回类型
// @Router /user/updateUser [post]
func UpdateUser(c *gin.Context){
	user:=models.UserBasic{}
	id,_:=strconv.Atoi(c.PostForm("id"))
	if int64(id) != authUid(c) {
		utils.RespFail(c.Writer, "无权修改他人资料")
		return
	}
	user.ID=uint(id)
	user.Name=c.PostForm("name")
	newPwd:=c.PostForm("password")
	if newPwd != "" {
		// 加盐哈希后才落库（此前明文直写会破坏登录凭据体系）；沿用旧盐，无盐则轮换
		exist := models.FindUserById(user.ID)
		salt := exist.Salt
		if salt == "" {
			salt = fmt.Sprintf("%06d", rand.Int32())
			user.Salt = salt
		}
		user.PassWord = utils.MakePassword(newPwd, salt)
	}
	user.Phone=c.PostForm("phone")
	user.Avatar=c.PostForm("icon")
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
// @Success 200 {string} json{"code"，"message"} //  修改返回类型
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
func RedisMsg(c *gin.Context){
	convId := c.PostForm("convId")
	userIdA, _ := strconv.Atoi(c.PostForm("userIdA"))
	userIdB, _ := strconv.Atoi(c.PostForm("userIdB"))
	if convId == "" {
		// 兼容旧参数：无 convId 时按私聊双向归一化推导
		if userIdA == 0 || userIdB == 0 {
			utils.RespFail(c.Writer, "参数错误：需要 convId 或 userIdA/userIdB")
			return
		}
		convId = models.ConvIdPrivate(int64(userIdA), int64(userIdB))
	}
	if !models.ConvHasMember(convId, authUid(c)) {
		utils.RespFail(c.Writer, "无权读取该会话")
		return
	}
	start, _ := strconv.Atoi(c.PostForm("start"))
	end, _ := strconv.Atoi(c.PostForm("end"))
	isRev, _ := strconv.ParseBool(c.PostForm("isRev"))
	res := models.RedisMsg(convId, int64(start), int64(end), isRev)
	utils.RespOkList(c.Writer, "ok", res)
}
// HistoryMsg
// @Tags 消息模块
// @Summary 读取冷层历史消息（convId+seq 游标分页）
// @param convId formData string true "会话id：私聊 p_{小}_{大}（或 userIdA/userIdB）、群聊 g_{群id}"
// @param beforeSeq formData string false "游标：只取 seq 小于该值的消息，0=从最新开始"
// @param size formData string false "条数，默认20，最大100"
// @Produce json
// @Success 200 {string} json{"code"，"message"}
// @Router /user/historyMsg [post]
func HistoryMsg(c *gin.Context){
	convId := c.PostForm("convId")
	beforeSeq, _ := strconv.ParseInt(c.PostForm("beforeSeq"), 10, 64)
	if beforeSeq == 0 {
		// 兼容旧参数名 beforeId（语义已变为 seq）
		beforeSeq, _ = strconv.ParseInt(c.PostForm("beforeId"), 10, 64)
	}
	size, _ := strconv.Atoi(c.PostForm("size"))
	if convId == "" {
		userIdA, _ := strconv.Atoi(c.PostForm("userIdA"))
		userIdB, _ := strconv.Atoi(c.PostForm("userIdB"))
		if userIdA > 0 && userIdB > 0 {
			convId = models.ConvIdPrivate(int64(userIdA), int64(userIdB))
		}
	}
	if convId == "" {
		utils.RespFail(c.Writer, "参数错误：需要 convId")
		return
	}
	if !models.ConvHasMember(convId, authUid(c)) {
		utils.RespFail(c.Writer, "无权读取该会话")
		return
	}
	list := models.LoadHistory(convId, beforeSeq, size)
	utils.RespOkList(c.Writer, list, len(list))
}

// MarkRead
// @Tags 消息模块
// @Summary 打开会话即已读（清未读并记录seq游标；群聊同时按已读窗口逐条 read_count+1 并向成员广播 Type=5）
// @param userId formData string true "用户id(被忽略，身份取会话)"
// @param convId formData string true "会话id"
// @Produce json
// @Success 200 {string} json{"code"，"message"}
// @Router /user/markRead [post]
func MarkRead(c *gin.Context){
	userId := authUid(c)
	convId := c.PostForm("convId")
	if convId == "" {
		utils.RespFail(c.Writer, "参数错误")
		return
	}
	if !models.ConvHasMember(convId, userId) {
		utils.RespFail(c.Writer, "无权操作该会话")
		return
	}
	models.MarkRead(userId, convId)
	utils.RespOk(c.Writer, 0, "已读")
}

// ReadStatus
// @Tags 消息模块
// @Summary 会话对端已读游标（私聊气泡"已读"状态）
// @param convId formData string true "会话id"
// @Produce json
// @Success 200 {string} json{"code"，"message"}
// @Router /user/readStatus [post]
func ReadStatus(c *gin.Context){
	convId := c.PostForm("convId")
	if convId == "" || !models.ConvHasMember(convId, authUid(c)) {
		utils.RespFail(c.Writer, "参数错误")
		return
	}
	utils.RespOk(c.Writer, gin.H{"convId": convId, "readSeq": models.PeerReadSeq(convId, authUid(c))}, "ok")
}

// UnreadList
// @Tags 消息模块
// @Summary 全部会话未读数
// @param userId formData string true "用户id"
// @Produce json
// @Success 200 {string} json{"code"，"message"}
// @Router /user/unreadList [post]
func UnreadList(c *gin.Context){
	utils.RespOk(c.Writer, models.UnreadMap(authUid(c)), "ok")
}
func SearchFriends(c *gin.Context){
	//身份以会话为准，表单 userId 不再可信
	users:=models.SearchFriend(uint(authUid(c)))
	// c.JSON(200,gin.H{
	// 	"code":  0,
	// 	"message":"查询好友列表成功！",
	// 	"data": users,
	// })
	utils.RespOkList(c.Writer,users,len(users))
}

func AddFriend(c *gin.Context){
	targetId,_:=strconv.Atoi(c.Request.FormValue("targetId"))

	code:=models.AddFriend(uint(authUid(c)),uint(targetId))
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
//新建群
func CreateCommunity(c *gin.Context){
	Name:=c.Request.FormValue("name")
	icon:=c.Request.FormValue("icon")
	desc:=c.Request.FormValue("desc")
	community :=models.Community{}
	community.OwnerId=uint(authUid(c))
	community.Name = Name
	community.Img=icon
	community.Desc=desc
	code,msg :=models.CreateCommunity(community)

	if code == 0{
		// models.JoinGroup(uint(ownerId),)
		utils.RespOk(c.Writer,code,msg)

	}else {
		utils.RespFail(c.Writer, msg)
	}


}
//加载群
func LoadCommunity(c *gin.Context){
	data,msg :=models.LoadCommunity(uint(authUid(c)))

	if len(data)!=0{
		utils.RespList(c.Writer,0,data,msg)
	}else{
		utils.RespFail(c.Writer,msg)
	}

}

func JoinGroup( c *gin.Context){
	comId:=c.Request.FormValue("comId")

	data,msg:=models.JoinGroup(uint(authUid(c)),comId)
	if data ==0{
		utils.RespOk(c.Writer,data,msg)
	}else{
		utils.RespFail(c.Writer,msg)
	}
}

func DeleteFriend(c *gin.Context){
	targetId,_:=strconv.Atoi(c.Request.FormValue("targetId"))
	data, msg := models.DeleteFriend(uint(authUid(c)), uint(targetId))

	if data == 0 {
		utils.RespOk(c.Writer, data, msg)
	} else {
		utils.RespFail(c.Writer, msg)
	}
}
func LoadFriends( c *gin.Context){
	userId,_ := strconv.Atoi(c.Query("userId"))

	data,msg := models.LoadFriends(uint(userId))

	if len(data) != 0 {
		utils.RespList(c.Writer,0,data,msg)
	}else{
		utils.RespFail(c.Writer,msg)
	}
}
