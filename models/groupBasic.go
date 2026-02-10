package models

import "gorm.io/gorm"

type GroupBasic struct {
	gorm.Model
	Name string
	OwnerId uint
	Icon string
	Type int //类型
	Desc string

}
func (tacle *GroupBasic)TableName() string{
	return "gropuBasic"
}