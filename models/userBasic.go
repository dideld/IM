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

// 全局的 Singleflight 分组，用于保护并发查询
var sg singleflight.Group

type UserBasic struct {
	gorm.Model
	Name string
	PassWord string
	Phone string  `valid:"matches(^1[3-9]{1}\\d{9}$)"`
	Email string `valid:"email"`
	Avatar   string //头像
	Identity string
	Salt     string //随机数密码
	ClientIp string
	ClientPort string
	LoginTime uint64
	HeratBeatTime uint64
	LoginOutTime uint64
	IsLogOut bool
	DeviceInfo string
}
func (table *UserBasic)TableName()string{
	return "user_basic"
}
func GetUserList() []*UserBasic{
	var data []*UserBasic
	utils.DB.Find(&data)
	for _,v:=range data{
		fmt.Println(v)
	}
	return data
}
//登录
func FindUserByNameAndPwd(name string,pass_word string) *UserBasic{
	user := &UserBasic{}
	utils.DB.Where("name = ? and pass_word= ?", name, pass_word).First(&user)

	//token加密
	str := fmt.Sprintf("%d", time.Now().Unix())
	temp := utils.MD5Encode(str)
	utils.DB.Model(&user).Where("id = ?", user.ID).Update("identity", temp)
	// fmt.Println("登录查到的用户:>>>>>>>>>>>>", user)
	return user
}
// func FindUserByName(name string) *UserBasic{
// 	user:=UserBasic{}
// 	res:=utils.DB.Where("name = ?",name).First(&user)
// 	if res.Error!=nil{
// 		return nil
// 	}
// 	return &user
// }
func FindUserByName(name string) UserBasic {
	user := UserBasic{}
	utils.DB.Where("name = ?", name).First(&user)
	return user
}
func FindUserByPhone(phone string) *gorm.DB{
	user:=UserBasic{}
	return utils.DB.Where("phone = ?",phone).First(&user)
}
func FindUserByEmail(email string) *gorm.DB{
	user:=UserBasic{}
	return utils.DB.Where("email = ?",email).First(&user)
}
//cache优化
func FindUserById(id uint) UserBasic {
	cacheKey := fmt.Sprintf("user_info:%d", id)
	ctx := context.Background()

	// 1. 先尝试从 Redis 获取缓存
	val, err := utils.Red.Get(ctx, cacheKey).Result()
	if err == nil {
		// 判断是否是空值防穿透拦截标记
		if val == "null" {
			return UserBasic{} // 拦截！直接返回空对象
		}
		var user UserBasic
		json.Unmarshal([]byte(val), &user) // 反序列化返回
		return user
	}

	// 2. 如果缓存没命中（或者缓存正好过期），触发 Singleflight 保护机制
	// 第一个参数是资源 Key，如果有 10000 个请求同时走到这里，
	// 只有第一个能进匿名函数执行真正查库，其他 9999 个在这行代码阻塞等结果
	res, err, _ := sg.Do(cacheKey, func() (interface{}, error) {
		user := UserBasic{}
		err := utils.DB.Where("id = ?", id).First(&user).Error

		if err != nil {
			if err == gorm.ErrRecordNotFound {
				// 发生查询不到记录：空值进入缓存防[缓存穿透]
				// 缓存个 "null" 字符串，过期时间 60 秒
				utils.Red.Set(ctx, cacheKey, "null", time.Second * 60)
			}
			return user, err
		}

		// 查到了正常数据，写入 Redis，过期时间可按业务设定(例如 1 小时)
		userData, _ := json.Marshal(user)
		utils.Red.Set(ctx, cacheKey, userData, time.Hour)

		return user, nil
	})

	if err != nil {
		return UserBasic{} 
	}
	
	// 断言并返回
	return res.(UserBasic)
}


func CreateUser(user UserBasic) *gorm.DB{

	return utils.DB.Create(&user)
}

func DeleteUser(user UserBasic)*gorm.DB{
	return utils.DB.Unscoped().Delete(&user)  //硬删除：添加上Unscoped,但是表仍能看到
}
func UpdateUser(user UserBasic)*gorm.DB{
	    return utils.DB.Model(&user).Updates(map[string]interface{}{
        "name":      user.Name,
        "pass_word": user.PassWord, // GORM默认将PassWord转为pass_word
		"phone":user.Phone,
		"email":user.Email,
		"avatar":user.Avatar,
    })
}