package service

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"webapp/utils"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

// 上传白名单：仅允许聊天用到的图片/音频类型，防止任意后缀文件落入 Nginx 直接托管的静态目录（.html/.svg 等是现成的存储型 XSS 面）
var allowedExt = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true, ".bmp": true,
	".mp3": true, ".wav": true, ".ogg": true, ".amr": true, ".m4a": true,
}

//图片发送逻辑
func Upload(c *gin.Context) {
	w := c.Writer
	req:= c.Request
	// 请求体硬上限（先于解析生效，防止超大上传打满内存/临时盘）
	maxMB := viper.GetInt64("upload.maxSizeMB")
	if maxMB <= 0 {
		maxMB = 10
	}
	req.Body = http.MaxBytesReader(w, req.Body, maxMB<<20)
	srcFile,head,err:=req.FormFile("file")
	if err!=nil{
		utils.RespFail(w,err.Error())
		return
	}
	suffix :=".png"
	ofileName :=head.Filename
	tem:=strings.Split(ofileName,".")
	if len(tem)>1{
		suffix =strings.ToLower("." + tem[len(tem)-1])
	}
	if !allowedExt[suffix] {
		utils.RespFail(w, "不支持的文件类型: "+suffix)
		return
	}
	if head.Size > maxMB<<20 {
		utils.RespFail(w, fmt.Sprintf("文件超过大小限制(%dMB)", maxMB))
		return
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