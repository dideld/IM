package utils

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Response 统一 API 响应结构。
// trace_id 为空时 omitempty 自动省略，不污染响应体。
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
	TraceID string      `json:"trace_id,omitempty"`
}

// GetTraceID 从 gin.Context 取当前请求的链路追踪 ID。
func GetTraceID(c *gin.Context) string {
	if v, exists := c.Get("trace_id"); exists {
		if tid, ok := v.(string); ok {
			return tid
		}
	}
	return ""
}

// RespOK 返回成功响应（code=0）。
func RespOK(c *gin.Context, data interface{}, msg string) {
	c.JSON(http.StatusOK, Response{
		Code:    OK.Code,
		Message: msg,
		Data:    data,
		TraceID: GetTraceID(c),
	})
}

// RespFail 返回业务错误响应。msg 非空时覆盖 err 的默认提示。
func RespFail(c *gin.Context, err ErrCode, msg ...string) {
	message := err.Message
	if len(msg) > 0 && msg[0] != "" {
		message = msg[0]
	}
	c.JSON(http.StatusOK, Response{
		Code:    err.Code,
		Message: message,
		Data:    nil,
		TraceID: GetTraceID(c),
	})
}

// RespList 返回分页列表响应，data 包装为 {list, total}。
func RespList(c *gin.Context, data interface{}, total int) {
	c.JSON(http.StatusOK, Response{
		Code:    OK.Code,
		Message: OK.Message,
		Data:    gin.H{"list": data, "total": total},
		TraceID: GetTraceID(c),
	})
}
