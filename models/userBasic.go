package models

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"webapp/utils"

	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

var sg singleflight.Group

type UserBasic struct {
	gorm.Model
	Name          string
	PassWord      string
	Phone         string `valid:"matches(^1[3-9]{1}\\d{9}$)"`
	Email         string `valid:"email"`
	Avatar        string
	Identity      string
	Salt          string
	ClientIp      string
	ClientPort    string
	LoginTime     uint64
	HeratBeatTime uint64
	LoginOutTime  uint64
	IsLogOut      bool
	DeviceInfo    string
}

func (table *UserBasic) TableName() string {
	return "user_basic"
}

// GetUserList 获取全部用户列表。
func GetUserList() []*UserBasic {
	var data []*UserBasic
	utils.DB.Find(&data)
	for _, v := range data {
		utils.Debug(context.Background(), "GetUserList result", "user", v)
	}
	return data
}

// FindUserByNameAndPwd 校验用户名和密码，成功时签发 token 并回写 identity。
func FindUserByNameAndPwd(name string, pass_word string) *UserBasic {
	user := &UserBasic{}
	utils.DB.Where("name = ? and pass_word= ?", name, pass_word).First(&user)

	token := utils.IssueToken(user.ID)
	utils.DB.Model(&user).Where("id = ?", user.ID).Update("identity", token)
	user.Identity = token
	return user
}

// FindUserByName 按用户名查询（返回空 struct 表示未找到）。
func FindUserByName(name string) UserBasic {
	user := UserBasic{}
	utils.DB.Where("name = ?", name).First(&user)
	return user
}

// FindUserByPhone 按手机号查询。
func FindUserByPhone(phone string) *gorm.DB {
	user := UserBasic{}
	return utils.DB.Where("phone = ?", phone).First(&user)
}

// FindUserByEmail 按邮箱查询。
func FindUserByEmail(email string) *gorm.DB {
	user := UserBasic{}
	return utils.DB.Where("email = ?", email).First(&user)
}

// FindUserById 按 ID 查询，带 Redis 缓存 + singleflight 防击穿 + 空值防穿透。
func FindUserById(id uint) UserBasic {
	cacheKey := fmt.Sprintf("user_info:%d", id)
	ctx := context.Background()

	val, err := utils.Red.Get(ctx, cacheKey).Result()
	if err == nil {
		if val == "null" {
			return UserBasic{}
		}
		var user UserBasic
		json.Unmarshal([]byte(val), &user)
		return user
	}

	res, err, _ := sg.Do(cacheKey, func() (interface{}, error) {
		user := UserBasic{}
		err := utils.DB.Where("id = ?", id).First(&user).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				utils.Red.Set(ctx, cacheKey, "null", time.Second*60)
			}
			return user, err
		}
		userData, _ := json.Marshal(user)
		utils.Red.Set(ctx, cacheKey, userData, time.Hour)
		return user, nil
	})

	if err != nil {
		return UserBasic{}
	}
	return res.(UserBasic)
}

// CreateUser 创建用户。
func CreateUser(user UserBasic) *gorm.DB {
	return utils.DB.Create(&user)
}

// DeleteUser 硬删除用户。
func DeleteUser(user UserBasic) *gorm.DB {
	return utils.DB.Unscoped().Delete(&user)
}

// UpdateUser 更新用户资料（含延时双删缓存 + 会话撤销）。
func UpdateUser(user UserBasic) *gorm.DB {
	ctx := context.Background()
	cacheKey := fmt.Sprintf("user_info:%d", user.ID)

	// 延时双删 - 第一删：更新 DB 前先删缓存
	utils.Red.Del(ctx, cacheKey)

	upds := map[string]interface{}{
		"name":   user.Name,
		"phone":  user.Phone,
		"email":  user.Email,
		"avatar": user.Avatar,
	}
	if user.PassWord != "" {
		upds["pass_word"] = user.PassWord
	}
	if user.Salt != "" {
		upds["salt"] = user.Salt
	}
	db := utils.DB.Model(&user).Where("id = ?", user.ID).Updates(upds)

	// 延时双删 - 第二删：时间轮异步删除（500ms），确保并发读不到脏缓存
	utils.TW.AddTask(500*time.Millisecond, "double_delete_"+cacheKey, func() {
		utils.Red.Del(context.Background(), cacheKey)
		userData := FindUserById(user.ID)
		data, _ := json.Marshal(userData)
		utils.Red.Set(context.Background(), cacheKey, data, time.Hour)
	})

	// 资料/密码变更即撤销全部登录会话，强制重新登录
	utils.DropUserSessions(user.ID)

	return db
}
