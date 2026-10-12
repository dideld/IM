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

var allowedExt = map[string]bool{
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true, ".bmp": true,
	".mp3": true, ".wav": true, ".ogg": true, ".amr": true, ".m4a": true,
}

// Upload 上传文件
//
//	@Summary	上传文件（图片/音频）
//	@Tags		文件模块
//	@Accept		multipart/form-data
//	@Produce	json
//	@Param		file	formData	file	true	"上传的文件（jpg/png/gif/webp/bmp/mp3/wav/ogg/amr/m4a）"
//	@Success	200	{object}	utils.Response{data=string}	"返回可访问的相对路径"
//	@Failure	400	{object}	utils.Response				"参数错误或文件类型不允许"
//	@Router		/attach/upload [post]
//	@Security	ApiKeyAuth
func Upload(c *gin.Context) {
	w := c.Writer
	req := c.Request
	maxMB := viper.GetInt64("upload.maxSizeMB")
	if maxMB <= 0 {
		maxMB = 10
	}
	req.Body = http.MaxBytesReader(w, req.Body, maxMB<<20)
	srcFile, head, err := req.FormFile("file")
	if err != nil {
		utils.Error(c.Request.Context(), "上传文件解析失败", "error", err)
		utils.RespFail(c, utils.ErrUpload, "")
		return
	}
	suffix := ".png"
	ofileName := head.Filename
	tem := strings.Split(ofileName, ".")
	if len(tem) > 1 {
		suffix = strings.ToLower("." + tem[len(tem)-1])
	}
	if !allowedExt[suffix] {
		utils.Warn(c.Request.Context(), "不支持的文件类型", "suffix", suffix)
		utils.RespFail(c, utils.ErrFileType, "")
		return
	}
	if head.Size > maxMB<<20 {
		utils.Warn(c.Request.Context(), "文件超过大小限制", "size", head.Size, "maxMB", maxMB)
		utils.RespFail(c, utils.ErrUpload, fmt.Sprintf("文件超过大小限制(%dMB)", maxMB))
		return
	}
	dir := viper.GetString("upload.path")
	if dir == "" {
		dir = "./asset/upload"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		utils.Error(c.Request.Context(), "创建上传目录失败", "error", err)
		utils.RespFail(c, utils.ErrUpload, "")
		return
	}
	fileName := fmt.Sprintf("%d%04d%s", time.Now().Unix(), rand.Int31(), suffix)
	dstFile, err := os.Create(filepath.Join(dir, fileName))
	if err != nil {
		utils.Error(c.Request.Context(), "创建文件失败", "error", err)
		utils.RespFail(c, utils.ErrUpload, "")
		return
	}
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		utils.Error(c.Request.Context(), "写入文件失败", "error", err)
		utils.RespFail(c, utils.ErrUpload, "")
		return
	}
	url := dir + "/" + fileName
	utils.RespOK(c, url, "发送图片成功")
}
