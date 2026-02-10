package main

import (
	"webapp/router"
	"webapp/utils"
)

func main() {
	utils.InitConfig()
	utils.InitMysql()
	utils.InitRedis()
	r := router.Router()
	r.Run()
}