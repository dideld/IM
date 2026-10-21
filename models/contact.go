package models

import (
	"context"
	"webapp/utils"
	"gorm.io/gorm"
)

// Contact 用户关系表（好友/群成员）。
type Contact struct {
	gorm.Model
	OwnerId  uint   // 关系发起者
	TargetId uint   // 关系目标（用户 ID 或群 ID）
	Type     int    // 1=好友，2=群
	Desc     string
}

func (tacle *Contact) TableName() string {
	return "contact"
}

// SearchFriend 查询用户的好友列表（返回好友的 UserBasic）。
func SearchFriend(userId uint) []UserBasic {
	contacts := make([]Contact, 0)
	objIds := make([]uint64, 0)
	utils.DB.Where("owner_id= ? and type=1", userId).Find(&contacts)
	for _, v := range contacts {
		utils.Debug(context.Background(), "SearchFriend contact", "contact", v)
		objIds = append(objIds, uint64(v.TargetId))
	}
	users := make([]UserBasic, 0)
	utils.DB.Where("id in ?", objIds).Find(&users)
	return users
}

// SearchUserByGroupId 查询群的成员 ID 列表。
func SearchUserByGroupId(communityId uint) []uint {
	contacts := make([]Contact, 0)
	objIds := make([]uint, 0)
	utils.DB.Where("target_id = ? and type=2", communityId).Find(&contacts)
	for _, v := range contacts {
		objIds = append(objIds, uint(v.OwnerId))
	}
	return objIds
}

// AddFriend 添加好友（双向写入，事务保证原子性）。
// 返回码：0=成功，1=不能添加自己，2=已添加过，-1=DB 错误，-2=用户不存在，-3=参数为空。
func AddFriend(userId uint, targetId uint) int {
	user := UserBasic{}
	if targetId != 0 {
		user = FindUserById(targetId)
		if user.PassWord != "" {
			if userId == user.ID {
				return 1
			}
			contact0 := Contact{}
			utils.DB.Where("owner_id= ? and target_id = ? and type = 1", userId, targetId).Find(&contact0)
			if contact0.ID != 0 {
				return 2
			}
			tx := utils.DB.Begin()
			defer func() {
				if r := recover(); r != nil {
					tx.Rollback()
				}
			}()
			contact := Contact{}
			contact.OwnerId = userId
			contact.TargetId = targetId
			contact.Type = 1
			if err := utils.DB.Create(&contact).Error; err != nil {
				tx.Rollback()
				return -1
			}

			contact1 := Contact{}
			contact1.OwnerId = targetId
			contact1.TargetId = userId
			contact1.Type = 1
			if err := utils.DB.Create(&contact1).Error; err != nil {
				tx.Rollback()
				return -1
			}
			tx.Commit()
			return 0
		}
		return -2
	}
	return -3
}

// DeleteFriend 删除好友（双向删除）。
func DeleteFriend(ownerId uint, targetId uint) (int, string) {
	if ownerId == 0 || targetId == 0 {
		return -1, "参数错误"
	}
	contact0 := Contact{}
	if err := utils.DB.Where("owner_id= ? and target_id = ? and type = 1", ownerId, targetId).Delete(&contact0).Error; err != nil {
		utils.Error(context.Background(), "DeleteFriend failed", "ownerId", ownerId, "targetId", targetId, "error", err)
		return -1, ""
	}
	if err := utils.DB.Where("owner_id= ? and target_id = ? and type = 1", targetId, ownerId).Delete(&contact0).Error; err != nil {
		utils.Error(context.Background(), "DeleteFriend reverse failed", "ownerId", ownerId, "targetId", targetId, "error", err)
		return -1, ""
	}

	return 0, "删除成功"
}

// LoadFriends 加载好友列表（返回好友的 UserBasic）。
func LoadFriends(userId uint) ([]UserBasic, string) {
	var contacts []Contact
	var users []UserBasic

	utils.DB.Where("owner_id = ? AND type = 1", userId).Find(&contacts)

	if len(contacts) == 0 {
		return users, "暂无好友"
	}

	var ids []uint
	for _, v := range contacts {
		ids = append(ids, v.TargetId)
	}

	utils.DB.Where("id IN ?", ids).Find(&users)

	return users, "加载成功"
}
