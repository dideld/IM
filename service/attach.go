package service

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
	"webapp/utils"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

//图片发送逻辑
func Upload(c *gin.Context) {
	w := c.Writer
	req:= c.Request
	srcFile,head,err:=req.FormFile("file")
	if err!=nil{
		utils.RespFail(w,err.Error())
		return
	}
	suffix :=".png"
	ofileName :=head.Filename
	tem:=strings.Split(ofileName,".")
	if len(tem)>1{
		suffix ="." + tem[len(tem)-1]
	}
	// 上传目录可配置，缺省 ./asset/upload；不存在则兜底创建
	dir := viper.GetString("upload.path")
	if dir == "" {
		dir = "./asset/upload"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		utils.RespFail(w, err.Error())
		return
	}
	fileName :=fmt.Sprintf("%d%04d%s",time.Now().Unix(),rand.Int31(),suffix)
	dstFile,err:=os.Create(filepath.Join(dir, fileName))
	if err!=nil{
		utils.RespFail(w,err.Error())
		return
	}
	_,err=io.Copy(dstFile,srcFile)
	if err!=nil{
		utils.RespFail(w,err.Error())
		return
	}
	url:=dir+"/"+fileName
	utils.RespOk(w,url,"发送图片成功！")
}