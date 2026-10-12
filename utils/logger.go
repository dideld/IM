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

func Info(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		return
	}
	slogLogger.InfoContext(ctx, msg, attrs(ctx, args)...)
}

func Warn(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		return
	}
	slogLogger.WarnContext(ctx, msg, attrs(ctx, args)...)
}

func Error(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		return
	}
	slogLogger.ErrorContext(ctx, msg, attrs(ctx, args)...)
}

func Debug(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		return
	}
	slogLogger.DebugContext(ctx, msg, attrs(ctx, args)...)
}

func Fatal(ctx context.Context, msg string, args ...any) {
	if slogLogger == nil {
		slogLogger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	slogLogger.ErrorContext(ctx, msg, attrs(ctx, args)...)
	os.Exit(1)
}

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
			slogLogger.DebugContext(ctx, "SQL记录未找到", args...)
			return
		}
		slogLogger.ErrorContext(ctx, "SQL错误", append(args, "error", err)...)
		return
	}
	if elapsed > time.Second {
		slogLogger.WarnContext(ctx, "慢SQL", args...)
		return
	}
}


