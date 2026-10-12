package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestTraceIDGenerates(t *testing.T) {
	r := gin.New()
	r.Use(TraceID())
	r.GET("/test", func(c *gin.Context) {
		tid, _ := c.Get("trace_id")
		c.JSON(200, gin.H{"trace_id": tid})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	header := w.Header().Get("X-Trace-ID")
	if header == "" {
		t.Error("X-Trace-ID header not set")
	}
	if len(header) != 32 {
		t.Errorf("trace_id length = %d, want 32", len(header))
	}

	var body map[string]string
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["trace_id"] != header {
		t.Errorf("body trace_id = %q, header = %q", body["trace_id"], header)
	}
}

func TestTraceIDPassthrough(t *testing.T) {
	r := gin.New()
	r.Use(TraceID())
	r.GET("/test", func(c *gin.Context) {
		tid, _ := c.Get("trace_id")
		c.JSON(200, gin.H{"trace_id": tid})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Trace-ID", "upstream-trace-123")
	r.ServeHTTP(w, req)

	header := w.Header().Get("X-Trace-ID")
	if header != "upstream-trace-123" {
		t.Errorf("trace_id = %q, want %q", header, "upstream-trace-123")
	}
}

func TestTraceIDUniqueness(t *testing.T) {
	r := gin.New()
	r.Use(TraceID())
	r.GET("/test", func(c *gin.Context) {
		tid, _ := c.Get("trace_id")
		c.String(200, tid.(string))
	})

	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/test", nil)
		r.ServeHTTP(w, req)
		tid := w.Body.String()
		if seen[tid] {
			t.Fatalf("duplicate trace_id after %d iterations: %s", i, tid)
		}
		seen[tid] = true
	}
}
