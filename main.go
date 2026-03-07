package main

import (
	"webapp/router"
	"webapp/utils"
	"webapp/models"
	"github.com/spf13/viper"

)

func main() {
	utils.InitConfig()
	utils.InitMysql()
	utils.InitRedis()
	go models.WsHub.Run()
	r := router.Router()
	r.Run(viper.GetString("port.server"))
}
