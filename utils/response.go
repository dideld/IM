package utils

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
	TraceID string      `json:"trace_id,omitempty"`
}

func GetTraceID(c *gin.Context) string {
	if v, exists := c.Get("trace_id"); exists {
		if tid, ok := v.(string); ok {
			return tid
		}
	}
	return ""
}

func RespOK(c *gin.Context, data interface{}, msg string) {
	c.JSON(http.StatusOK, Response{
		Code:    OK.Code,
		Message: msg,
		Data:    data,
		TraceID: GetTraceID(c),
	})
}

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

func RespList(c *gin.Context, data interface{}, total int) {
	c.JSON(http.StatusOK, Response{
		Code:    OK.Code,
		Message: OK.Message,
		Data:    gin.H{"list": data, "total": total},
		TraceID: GetTraceID(c),
	})
}
