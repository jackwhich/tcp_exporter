// Package utils 提供日志工具，封装 zap.Logger，
// 支持全局初始化、动态级别更新和从 context 中自动注入 trace_id。
package utils

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LoggerConfig 定义日志配置接口
type LoggerConfig interface {
	GetLogLevel() string
}

// Logger 封装日志系统的初始化和配置
type Logger struct {
	*zap.Logger
}

var (
	logLevel = zap.NewAtomicLevel()
	Log      *Logger // 全局logger实例
)

// InitLogger 初始化全局logger
func InitLogger(cfg LoggerConfig) {
	// 设置初始日志级别
	updateLogLevel(cfg.GetLogLevel())

	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = logLevel
	zapConfig.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapConfig.DisableStacktrace = true
	log, err := zapConfig.Build(zap.AddCaller())
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化logger失败: %v\n", err)
		os.Exit(1)
	}
	Log = &Logger{log}
}

func updateLogLevel(levelStr string) {
	levelStr = strings.ToLower(levelStr)

	switch levelStr {
	case "trace", "debug":
		logLevel.SetLevel(zap.DebugLevel)
	case "info":
		logLevel.SetLevel(zap.InfoLevel)
	case "warn":
		logLevel.SetLevel(zap.WarnLevel)
	case "error":
		logLevel.SetLevel(zap.ErrorLevel)
	default:
		logLevel.SetLevel(zap.InfoLevel)
	}
}

// UpdateLogLevel 动态更新全局日志级别
func (l *Logger) UpdateLogLevel(levelStr string) {
	updateLogLevel(levelStr)
}

// 定义context key类型
type contextKey string

const (
	TraceIDKey contextKey = "traceID"
)

// GetTraceID 从给定的 context 中获取 trace ID。
// 如果不存在，则返回空字符串。
func GetTraceID(ctx context.Context) string {
	if traceID, ok := ctx.Value(TraceIDKey).(string); ok {
		return traceID
	}
	return ""
}

// Trace 记录TRACE级别日志
func (l *Logger) Trace(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("traceID", GetTraceID(ctx)))
	l.Logger.Debug(msg, fields...)
}

// Debug 记录DEBUG级别日志
func (l *Logger) Debug(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("trace_id", GetTraceID(ctx)))
	l.Logger.Debug(msg, fields...)
}

// Info 记录INFO级别日志
func (l *Logger) Info(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("trace_id", GetTraceID(ctx)))
	l.Logger.Info(msg, fields...)
}

// Warn 记录WARN级别日志
func (l *Logger) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("trace_id", GetTraceID(ctx)))
	l.Logger.Warn(msg, fields...)
}

// Error 记录ERROR级别日志
func (l *Logger) Error(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("trace_id", GetTraceID(ctx)))
	l.Logger.Error(msg, fields...)
}

// Fatal 记录FATAL级别日志
func (l *Logger) Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("trace_id", GetTraceID(ctx)))
	l.Logger.Fatal(msg, fields...)
}

// GetPodName 获取当前Pod的名称标识
func GetPodName() string {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		// 非容器环境回退处理
		hostname, _ := os.Hostname()
		if hostname == "" {
			// 生成基于时间戳和进程ID的唯一标识
			timestamp := time.Now().UnixNano()
			pid := os.Getpid()
			podName = "local-" + strconv.FormatInt(timestamp, 10) + "-" + strconv.Itoa(pid)
		} else {
			podName = hostname
		}
	}
	return podName
}