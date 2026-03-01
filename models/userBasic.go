package models

import (
	"gorm.io/gorm"
	"webapp/utils"
	"fmt"
	"time"
)
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
func FindUserById(id uint) UserBasic{
	user:=UserBasic{}
	utils.DB.Where("id= ?",id).First(&user)
	return user
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