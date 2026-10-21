// Package utils 结构化日志（基于 slog）与 GORM 日志适配器。
//
// 日志策略：仅记录异常与关键事件，正常请求零输出。
// 日志自动携带 trace_id（从 context 提取）和 source（文件名:行号）。

package utils

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var slogLogger *slog.Logger

type ctxKey string

const traceIDKey ctxKey = "trace_id"

// InitLogger 初始化全局 slog 日志，level 支持 debug/info/warn/error。
func InitLogger(level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	slogLogger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     lvl,
		AddSource: true,
	}))
}

// WithTrace 将 trace_id 注入 context，后续日志自动携带。
func WithTrace(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

func traceFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

func attrs(ctx context.Context, args []any) []any {
	if tid := traceFromCtx(ctx); tid != "" {
		args = append(args, "trace_id", tid)
	}
	return args
}

// Info 记录信息级别日志（仅异常场景使用，正常流程不调用）。
func Info(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		return
	}
	slogLogger.InfoContext(ctx, msg, attrs(ctx, args)...)
}

// Warn 记录警告级别日志（参数错误、认证失败、慢 SQL 等）。
func Warn(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		return
	}
	slogLogger.WarnContext(ctx, msg, attrs(ctx, args)...)
}

// Error 记录错误级别日志（DB/Redis/Kafka 故障、上传失败等）。
func Error(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		return
	}
	slogLogger.ErrorContext(ctx, msg, attrs(ctx, args)...)
}

// Debug 记录调试级别日志（开发环境使用）。
func Debug(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		return
	}
	slogLogger.DebugContext(ctx, msg, attrs(ctx, args)...)
}

// Fatal 记录致命错误并退出进程（启动阶段使用）。
func Fatal(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		slogLogger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	slogLogger.ErrorContext(ctx, msg, attrs(ctx, args)...)
	os.Exit(1)
}

// gormSlogLogger GORM 日志适配器，将慢 SQL 和错误转为 slog 输出。
type gormSlogLogger struct{}

func (l *gormSlogLogger) Printf(format string, args ...interface{}) {}

func (l *gormSlogLogger) LogMode(level logger.LogLevel) logger.Interface {
	return l
}

func (l *gormSlogLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if slogLogger == nil {
		return
	}
	slogLogger.InfoContext(ctx, msg, data...)
}

func (l *gormSlogLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if slogLogger == nil {
		return
	}
	slogLogger.WarnContext(ctx, msg, data...)
}

func (l *gormSlogLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if slogLogger == nil {
		return
	}
	slogLogger.ErrorContext(ctx, msg, data...)
}

func (l *gormSlogLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if slogLogger == nil {
		return
	}
	elapsed := time.Since(begin)
	sql, rows := fc()
	args := []any{"sql", sql, "rows", rows, "elapsed", elapsed.String()}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			slogLogger.DebugContext(ctx, "SQL 记录未找到", args...)
			return
		}
		slogLogger.ErrorContext(ctx, "SQL 错误", append(args, "error", err)...)
		return
	}
	if elapsed > time.Second {
		slogLogger.WarnContext(ctx, "慢 SQL", args...)
		return
	}
}
