package main

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)


// Logger 封装日志系统的初始化和配置
type Logger struct {
	*zap.Logger
}

// NewLogger 创建并配置一个新的日志记录器
// cfg: 应用程序配置，包含日志级别等设置
var (
	logLevel = zap.NewAtomicLevel()
)

func NewLogger(cfg *Config) *Logger {
	// 设置初始日志级别
	updateLogLevel(cfg.Server.LogLevel)
	
	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = logLevel
	zapConfig.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapConfig.DisableStacktrace = true
	log, err := zapConfig.Build(zap.AddCaller())
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] 初始化 zap 日志失败: %v\n", CodeInitLoggerFailure, err)
		os.Exit(1)
	}
	return &Logger{log}
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

// Trace 记录TRACE级别日志
func (l *Logger) Trace(msg string, fields ...zap.Field) {
	l.Debug(msg, fields...)
}