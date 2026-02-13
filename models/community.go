package models

import (
	"webapp/utils"
	"fmt"
	"gorm.io/gorm"
)

type Community struct {
	gorm.Model
	Name string
	OwnerId uint
	Img string
	Desc string

}
func CreateCommunity(c Community) (int,string){
	if len(c.Name)==0{
		return -1,"群名称不能为空"
	}
	if c.OwnerId==0{
		return -1,"请先登录"
	}
	if err:=utils.DB.Create(&c).Error;err!=nil{
		return -1,"建群失败"
	}
	return 0,"建群成功"
}

// func LoadCommunity(ownerId uint)([] *Community,string){
// 	data :=make([]*Community ,10)
// 	utils.DB.Where("owner_id = ? ",ownerId).Find(&data)
// 	for _,v:=range data{
// 		fmt.Println(v)
// 	}
// 	return data,"查询成功"
// }
func LoadCommunity(ownerId uint) ([]*Community, string) {
	contacts := make([]Contact, 0)
	objIds := make([]uint64, 0)
	utils.DB.Where("owner_id = ? and type= 2", ownerId).Find(&contacts)
	for _, v := range contacts {
		objIds = append(objIds, uint64(v.TargetId))
	}

	fmt.Println("objIds:", objIds)

	data := make([]*Community, 0)
	utils.DB.Where("id in ?", objIds).Find(&data)
	for _, v := range data {
		fmt.Println(v)
	}
	//utils.DB.Where()
	return data, "查询成功"
}