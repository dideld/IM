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
// @Summary 获取所有用户列表
// @Produce json
// @Success 200 {object} utils.Response
// @Router /user/getUserList [post]
// @Security X-Token
func GetUserList(c *gin.Context) {
	data := models.GetUserList()
	utils.RespOK(c, data, "获取用户列表成功")
}

// CreateUser
// @Tags 用户模块
// @Summary 注册新用户
// @Produce json
// @Param name formData string true "用户名"
// @Param password formData string true "密码"
// @Param Identity formData string true "确认密码"
// @Success 200 {object} utils.Response
// @Router /user/createUser [post]
func CreateUser(c *gin.Context) {
	user := models.UserBasic{}
	user.Name = c.Request.FormValue("name")
	password := c.Request.FormValue("password")
	repassword := c.Request.FormValue("Identity")
	data := models.FindUserByName(user.Name)

	salt := fmt.Sprintf("%06d", rand.Int32())
	if user.Name == "" || password == "" || repassword == "" {
		utils.RespFail(c, utils.ErrParam, "用户名或密码不能为空")
		return
	}
	if data.Name != "" {
		utils.RespFail(c, utils.ErrUserExists, "")
		return
	}
	if password != repassword {
		utils.RespFail(c, utils.ErrParam, "两次密码不一致")
		return
	}
	user.PassWord = utils.MakePassword(password, salt)
	user.Salt = salt
	res := models.CreateUser(user)
	if res.Error != nil {
		utils.Error(c.Request.Context(), "创建用户失败", "error", res.Error)
		utils.RespFail(c, utils.ErrInternal, "")
		return
	}
	utils.RespOK(c, user, "新增用户成功")
}

// DeleteUser
// @Tags 用户模块
// @Summary 删除用户（仅能删除自己）
// @Produce json
// @Param id query int true "用户ID"
// @Success 200 {object} utils.Response
// @Router /user/deleteUser [delete]
// @Security X-Token
func DeleteUser(c *gin.Context) {
	user := models.UserBasic{}
	id, _ := strconv.Atoi(c.Query("id"))
	if int64(id) != authUid(c) {
		utils.RespFail(c, utils.ErrParam, "无权删除他人")
		return
	}
	user.ID = uint(id)
	models.DeleteUser(user)
	utils.RespOK(c, user, "删除用户成功")
}

// UpdateUser
// @Tags 用户模块
// @Summary 修改用户资料
// @Produce json
// @Param id formData int true "用户ID"
// @Param name formData string false "用户名"
// @Param password formData string false "新密码"
// @Param phone formData string false "手机号"
// @Param icon formData string false "头像URL"
// @Param email formData string false "邮箱"
// @Success 200 {object} utils.Response
// @Router /user/updateUser [post]
// @Security X-Token
func UpdateUser(c *gin.Context) {
	user := models.UserBasic{}
	id, _ := strconv.Atoi(c.PostForm("id"))
	if int64(id) != authUid(c) {
		utils.RespFail(c, utils.ErrParam, "无权修改他人资料")
		return
	}
	user.ID = uint(id)
	user.Name = c.PostForm("name")
	newPwd := c.PostForm("password")
	if newPwd != "" {
		exist := models.FindUserById(user.ID)
		salt := exist.Salt
		if salt == "" {
			salt = fmt.Sprintf("%06d", rand.Int32())
			user.Salt = salt
		}
		user.PassWord = utils.MakePassword(newPwd, salt)
	}
	user.Phone = c.PostForm("phone")
	user.Avatar = c.PostForm("icon")
	user.Email = c.PostForm("email")
	models.UpdateUser(user)
	_, err := govalidator.ValidateStruct(user)
	if err != nil {
		utils.Warn(c.Request.Context(), "修改用户参数校验失败", "error", err)
		utils.RespFail(c, utils.ErrParam, "修改参数失败")
		return
	}
	utils.RespOK(c, user, "修改用户成功")
}

// FindUserByNameAndPwd
// @Tags 用户模块
// @Summary 用户登录
// @Produce json
// @Param name formData string true "用户名"
// @Param password formData string true "密码"
// @Success 200 {object} utils.Response
// @Router /user/findUserByNameAndPwd [post]
func FindUserByNameAndPwd(c *gin.Context) {
	data := models.UserBasic{}
	name := c.Request.FormValue("name")
	password := c.Request.FormValue("password")
	user := models.FindUserByName(name)
	if user.Name == "" {
		utils.Warn(c.Request.Context(), "登录失败：用户不存在", "name", name)
		utils.RespFail(c, utils.ErrUserNotFound, "")
		return
	}
	flag := utils.ValidPassword(password, user.Salt, user.PassWord)
	if !flag {
		utils.Warn(c.Request.Context(), "登录失败：密码错误", "name", name)
		utils.RespFail(c, utils.ErrPassword, "")
		return
	}
	pwd := utils.MakePassword(password, user.Salt)
	data = *models.FindUserByNameAndPwd(name, pwd)
	utils.RespOK(c, data, "登录成功")
}

// RedisMsg
// @Tags 消息模块
// @Summary 读取热层消息（Redis缓存）
// @Produce json
// @Param convId formData string false "会话ID（私聊 p_{小}_{大}，群聊 g_{群id}）"
// @Param userIdA formData int false "私聊用户A的ID（无convId时使用）"
// @Param userIdB formData int false "私聊用户B的ID（无convId时使用）"
// @Param start formData int false "起始位置"
// @Param end formData int false "结束位置"
// @Param isRev formData bool false "是否倒序"
// @Success 200 {object} utils.Response
// @Router /user/redisMsg [post]
// @Security X-Token
func RedisMsg(c *gin.Context) {
	convId := c.PostForm("convId")
	userIdA, _ := strconv.Atoi(c.PostForm("userIdA"))
	userIdB, _ := strconv.Atoi(c.PostForm("userIdB"))
	if convId == "" {
		if userIdA == 0 || userIdB == 0 {
			utils.RespFail(c, utils.ErrParam, "需要 convId 或 userIdA/userIdB")
			return
		}
		convId = models.ConvIdPrivate(int64(userIdA), int64(userIdB))
	}
	if !models.ConvHasMember(convId, authUid(c)) {
		utils.RespFail(c, utils.ErrParam, "无权读取该会话")
		return
	}
	start, _ := strconv.Atoi(c.PostForm("start"))
	end, _ := strconv.Atoi(c.PostForm("end"))
	isRev, _ := strconv.ParseBool(c.PostForm("isRev"))
	res := models.RedisMsg(convId, int64(start), int64(end), isRev)
	utils.RespList(c, res, len(res))
}

// HistoryMsg
// @Tags 消息模块
// @Summary 读取冷层历史消息（MySQL，游标分页）
// @Produce json
// @Param convId formData string false "会话ID"
// @Param userIdA formData int false "私聊用户A的ID（无convId时使用）"
// @Param userIdB formData int false "私聊用户B的ID（无convId时使用）"
// @Param beforeSeq formData int false "游标：只取 seq 小于该值的消息，0=从最新开始"
// @Param size formData int false "条数，默认20，最大100"
// @Success 200 {object} utils.Response
// @Router /user/historyMsg [post]
// @Security X-Token
func HistoryMsg(c *gin.Context) {
	convId := c.PostForm("convId")
	beforeSeq, _ := strconv.ParseInt(c.PostForm("beforeSeq"), 10, 64)
	if beforeSeq == 0 {
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
		utils.RespFail(c, utils.ErrParam, "需要 convId")
		return
	}
	if !models.ConvHasMember(convId, authUid(c)) {
		utils.RespFail(c, utils.ErrParam, "无权读取该会话")
		return
	}
	list := models.LoadHistory(convId, beforeSeq, size)
	utils.RespList(c, list, len(list))
}

// MarkRead
// @Tags 消息模块
// @Summary 标记会话已读（清未读并记录seq游标，群聊同时广播已读回执）
// @Produce json
// @Param convId formData string true "会话ID"
// @Success 200 {object} utils.Response
// @Router /user/markRead [post]
// @Security X-Token
func MarkRead(c *gin.Context) {
	userId := authUid(c)
	convId := c.PostForm("convId")
	if convId == "" {
		utils.RespFail(c, utils.ErrParam, "")
		return
	}
	if !models.ConvHasMember(convId, userId) {
		utils.RespFail(c, utils.ErrParam, "无权操作该会话")
		return
	}
	models.MarkRead(userId, convId)
	utils.RespOK(c, nil, "已读")
}

// ReadStatus
// @Tags 消息模块
// @Summary 查询会话对端已读游标（私聊气泡"已读"状态）
// @Produce json
// @Param convId formData string true "会话ID"
// @Success 200 {object} utils.Response
// @Router /user/readStatus [post]
// @Security X-Token
func ReadStatus(c *gin.Context) {
	convId := c.PostForm("convId")
	if convId == "" || !models.ConvHasMember(convId, authUid(c)) {
		utils.RespFail(c, utils.ErrParam, "")
		return
	}
	utils.RespOK(c, gin.H{"convId": convId, "readSeq": models.PeerReadSeq(convId, authUid(c))}, "ok")
}

// UnreadList
// @Tags 消息模块
// @Summary 获取全部会话未读数
// @Produce json
// @Success 200 {object} utils.Response
// @Router /user/unreadList [post]
// @Security X-Token
func UnreadList(c *gin.Context) {
	utils.RespOK(c, models.UnreadMap(authUid(c)), "ok")
}

// SearchFriends
// @Tags 好友模块
// @Summary 获取好友列表
// @Produce json
// @Success 200 {object} utils.Response
// @Router /searchFriends [post]
// @Security X-Token
func SearchFriends(c *gin.Context) {
	users := models.SearchFriend(uint(authUid(c)))
	utils.RespList(c, users, len(users))
}

// AddFriend
// @Tags 好友模块
// @Summary 添加好友
// @Produce json
// @Param targetId formData int true "目标用户ID"
// @Success 200 {object} utils.Response
// @Router /contact/addfriend [post]
// @Security X-Token
func AddFriend(c *gin.Context) {
	targetId, _ := strconv.Atoi(c.Request.FormValue("targetId"))
	code := models.AddFriend(uint(authUid(c)), uint(targetId))
	if code == 0 {
		utils.RespOK(c, code, "添加成功")
	} else if code == 1 {
		utils.RespFail(c, utils.ErrParam, "不能添加自己")
	} else if code == -2 {
		utils.RespFail(c, utils.ErrUserNotFound, "")
	} else if code == -3 {
		utils.RespFail(c, utils.ErrParam, "好友ID不能为空")
	} else if code == 2 {
		utils.RespFail(c, utils.ErrParam, "不能添加已经添加过的好友")
	} else {
		utils.RespFail(c, utils.ErrInternal, "添加失败")
	}
}

// DeleteFriend
// @Tags 好友模块
// @Summary 删除好友
// @Produce json
// @Param targetId formData int true "目标用户ID"
// @Success 200 {object} utils.Response
// @Router /contact/deletefriend [post]
// @Security X-Token
func DeleteFriend(c *gin.Context) {
	targetId, _ := strconv.Atoi(c.Request.FormValue("targetId"))
	data, msg := models.DeleteFriend(uint(authUid(c)), uint(targetId))
	if data == 0 {
		utils.RespOK(c, data, msg)
	} else {
		utils.RespFail(c, utils.ErrInternal, msg)
	}
}

// LoadFriends
// @Tags 好友模块
// @Summary 加载好友列表
// @Produce json
// @Param userId query int true "用户ID"
// @Success 200 {object} utils.Response
// @Router /contact/loadfriends [get]
func LoadFriends(c *gin.Context) {
	userId, _ := strconv.Atoi(c.Query("userId"))
	data, _ := models.LoadFriends(uint(userId))
	utils.RespList(c, data, len(data))
}

// CreateCommunity
// @Tags 群模块
// @Summary 创建群
// @Produce json
// @Param name formData string true "群名称"
// @Param icon formData string false "群头像"
// @Param desc formData string false "群描述"
// @Success 200 {object} utils.Response
// @Router /contact/createCommunity [post]
// @Security X-Token
func CreateCommunity(c *gin.Context) {
	Name := c.Request.FormValue("name")
	icon := c.Request.FormValue("icon")
	desc := c.Request.FormValue("desc")
	community := models.Community{}
	community.OwnerId = uint(authUid(c))
	community.Name = Name
	community.Img = icon
	community.Desc = desc
	code, msg := models.CreateCommunity(community)
	if code == 0 {
		utils.RespOK(c, code, msg)
	} else {
		utils.RespFail(c, utils.ErrInternal, msg)
	}
}

// LoadCommunity
// @Tags 群模块
// @Summary 获取群列表
// @Produce json
// @Success 200 {object} utils.Response
// @Router /contact/loadCommunity [post]
// @Security X-Token
func LoadCommunity(c *gin.Context) {
	data, _ := models.LoadCommunity(uint(authUid(c)))
	utils.RespList(c, data, len(data))
}

// JoinGroup
// @Tags 群模块
// @Summary 加入群
// @Produce json
// @Param comId formData string true "群ID或群名称"
// @Success 200 {object} utils.Response
// @Router /contact/joinGroup [post]
// @Security X-Token
func JoinGroup(c *gin.Context) {
	comId := c.Request.FormValue("comId")
	data, msg := models.JoinGroup(uint(authUid(c)), comId)
	if data == 0 {
		utils.RespOK(c, data, msg)
	} else {
		utils.RespFail(c, utils.ErrInternal, msg)
	}
}
