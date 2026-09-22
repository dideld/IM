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

	//签发登录会话 token（Redis 会话，WS 握手校验、可撤销），回写 identity 字段供前端携带
	token := utils.IssueToken(user.ID)
	utils.DB.Model(&user).Where("id = ?", user.ID).Update("identity", token)
	user.Identity = token
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
func UpdateUser(user UserBasic) *gorm.DB {
	ctx := context.Background()
	cacheKey := fmt.Sprintf("user_info:%d", user.ID)

	// 1. 【延时双删 - 第一删】更新数据库前，先删除缓存
	utils.Red.Del(ctx, cacheKey)

	// 2. 更新数据库（密码/盐为空时不写入，防止覆盖凭据；service 层负责加盐哈希后传入）
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

	// 3. 【延时双删 - 第二删】利用我们写的时间轮，进行异步延时删除 (例如 500 毫秒)
	// 完美解决了休眠阻塞主协程的问题！性能无损！
	utils.TW.AddTask(500*time.Millisecond, "double_delete_"+cacheKey, func() {
		// 500ms 后，底层自动执行第二删，确保这段时间内的所有脏数据全被清空
		utils.Red.Del(context.Background(), cacheKey)

		// 【异步重建】，可以直接在这里查库写入 Redis 
		userData := FindUserById(user.ID)
		data, _ := json.Marshal(userData)
		utils.Red.Set(context.Background(), cacheKey, data, time.Hour)
	})

	// 4. 资料/密码变更即撤销该用户全部登录会话，强制重新登录（新 WS 连接需重新过 /chat 鉴权）
	utils.DropUserSessions(user.ID)

	return db
}