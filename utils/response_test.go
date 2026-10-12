package utils

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestErrCodeValues(t *testing.T) {
	if OK.Code != 0 {
		t.Errorf("OK.Code = %d, want 0", OK.Code)
	}
	if ErrUnauthorized.Code != 1002 {
		t.Errorf("ErrUnauthorized.Code = %d, want 1002", ErrUnauthorized.Code)
	}
	if ErrParam.Code != 1001 {
		t.Errorf("ErrParam.Code = %d, want 1001", ErrParam.Code)
	}
}

func TestRespOK(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Set("trace_id", "abc123")

	RespOK(c, map[string]string{"name": "test"}, "成功")

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
	if resp.Message != "成功" {
		t.Errorf("message = %q, want %q", resp.Message, "成功")
	}
	if resp.TraceID != "abc123" {
		t.Errorf("trace_id = %q, want %q", resp.TraceID, "abc123")
	}
}

func TestRespFail(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	c.Set("trace_id", "def456")

	RespFail(c, ErrUnauthorized, "")

	var resp Response
	json.Unmarshal(w.Body.Bytes(), &resp)

	if resp.Code != 1002 {
		t.Errorf("code = %d, want 1002", resp.Code)
	}
	if resp.Message != "未登录或会话已过期" {
		t.Errorf("message = %q, want %q", resp.Message, "未登录或会话已过期")
	}
	if resp.Data != nil {
		t.Errorf("data = %v, want nil", resp.Data)
	}
}

func TestRespFailCustomMsg(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	RespFail(c, ErrParam, "用户名不能为空")

	var resp Response
	json.Unmarshal(w.Body.Bytes(), &resp)

	if resp.Code != 1001 {
		t.Errorf("code = %d, want 1001", resp.Code)
	}
	if resp.Message != "用户名不能为空" {
		t.Errorf("message = %q, want %q", resp.Message, "用户名不能为空")
	}
}

func TestRespList(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	items := []string{"a", "b", "c"}
	RespList(c, items, 3)

	var resp Response
	json.Unmarshal(w.Body.Bytes(), &resp)

	if resp.Code != 0 {
		t.Errorf("code = %d, want 0", resp.Code)
	}
	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("data is not map, got %T", resp.Data)
	}
	if int(data["total"].(float64)) != 3 {
		t.Errorf("total = %v, want 3", data["total"])
	}
}

func TestRespNoTraceID(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)

	RespOK(c, nil, "ok")

	body := w.Body.Bytes()
	if string(body) == "" {
		t.Fatal("empty body")
	}
	var raw map[string]interface{}
	json.Unmarshal(body, &raw)
	if _, exists := raw["trace_id"]; exists {
		t.Errorf("trace_id should be omitted when empty, got %v", raw["trace_id"])
	}
}

func TestLoggerInit(t *testing.T) {
	InitLogger("info")
	if slogLogger == nil {
		t.Fatal("logger not initialized")
	}
	Info(context.Background(), "test info", "key", "value")
	Warn(context.Background(), "test warn")
	Error(context.Background(), "test error")
}

func TestWithTrace(t *testing.T) {
	ctx := context.Background()
	ctx = WithTrace(ctx, "trace-xyz")
	got := traceFromCtx(ctx)
	if got != "trace-xyz" {
		t.Errorf("traceFromCtx = %q, want %q", got, "trace-xyz")
	}
}

func TestTraceFromCtxEmpty(t *testing.T) {
	ctx := context.Background()
	got := traceFromCtx(ctx)
	if got != "" {
		t.Errorf("traceFromCtx = %q, want empty", got)
	}
}
