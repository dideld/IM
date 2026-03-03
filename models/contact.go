package models

import (
	"webapp/utils"
	"gorm.io/gorm"
	"fmt"
)

//人员关系
type Contact struct{
	gorm.Model
	OwnerId   uint//谁的关系
	TargetId  uint//对应的谁
	Type      int //对应的类型 1好友,2群,3
	Desc      string 
}

func (tacle *Contact)TableName() string{
	return "contact"
}

func SearchFriend(userId uint) []UserBasic{
	contacts :=make([]Contact,0)
	objIds:=make([]uint64,0)
	utils.DB.Where("owner_id= ? and type=1",userId).Find(&contacts)
	for _,v:=range contacts{
		fmt.Println(">>>>>>>>>>>>",v)
		objIds=append(objIds,uint64(v.TargetId))
	}
	users:=make([]UserBasic,0)
	utils.DB.Where("id in ?",objIds).Find(&users)
	return users
}
func SearchUserByGroupId(communityId uint) []uint {
	contacts := make([]Contact, 0)
	objIds := make([]uint, 0)
	utils.DB.Where("target_id = ? and type=2", communityId).Find(&contacts)
	for _, v := range contacts {
		objIds = append(objIds, uint(v.OwnerId))
	}
	return objIds
}
func AddFriend(userId uint,targetId uint) int{
	user:=UserBasic{}
	if targetId!=0{
		user =FindUserById(targetId)
		if user.PassWord!=""{
			if userId==user.ID{
				return 1 //不能添加自己
			}
			contact0:=Contact{}
			utils.DB.Where("owner_id= ? and target_id = ? and type = 1",userId,targetId).Find(&contact0)
			if contact0.ID !=0{
				return 2
			}
			tx:=utils.DB.Begin()
			//事务一旦开始，不论什么异常最终都会rollback
			defer func (){
				if r:=recover();r!=nil{
					tx.Rollback()
				}
			}()
			contact :=Contact{}
			contact.OwnerId=userId
			contact.TargetId=targetId
			contact.Type=1
			if err:=utils.DB.Create(&contact).Error;err!=nil{
				tx.Rollback()
				return -1
			}

			contact1 :=Contact{}
			contact1.OwnerId=targetId
			contact1.TargetId=userId
			contact1.Type=1
			if err:=utils.DB.Create(&contact1).Error;err!=nil{
				tx.Rollback()
				return -1
			}
			tx.Commit()
			return 0 //查到
		}
		return -2
	}
	return -3
}
//删除好友
func DeleteFriend(ownerId uint,targetId uint) (int,string){
	if ownerId == 0 || targetId == 0 {
		return -1, "参数错误"
	}
	contact0:=Contact{}
	if err:=utils.DB.Where("owner_id= ? and target_id = ? and type = 1",ownerId,targetId).Delete(&contact0).Error;err!=nil{
		fmt.Println(err)
		return -1,""
	}
	if err:=utils.DB.Where("owner_id= ? and target_id = ? and type = 1",targetId,ownerId).Delete(&contact0).Error;err!=nil{
		fmt.Println(err)
		return -1,""
	}

	return 0, "删除成功"
}
//加载好友
func LoadFriends(userId uint) ([]UserBasic,string){

	var contacts []Contact
	var users []UserBasic

	utils.DB.Where("owner_id = ? AND type = 1", userId).
		Find(&contacts)

	if len(contacts) == 0 {
		return users,"暂无好友"
	}

	var ids []uint
	for _,v := range contacts{
		ids = append(ids,v.TargetId)
	}

	utils.DB.Where("id IN ?", ids).
		Find(&users)

	return users,"加载成功"
}