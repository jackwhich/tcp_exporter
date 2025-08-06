package main

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"tcp-exporter/utils"
)

func runHTTPServer(cfg *Config) {
	gin.SetMode(cfg.Server.GinMode)
	router := gin.New()

	// 自定义日志中间件（添加trace ID支持）
	router.Use(func(c *gin.Context) {
		start := time.Now()
		url := c.Request.URL.Path

		// 生成trace ID并存入context（使用自定义key类型）
		traceID := uuid.New().String()
		ctx := context.WithValue(c.Request.Context(), utils.TraceIDKey, traceID)
		c.Request = c.Request.WithContext(ctx)

		// 处理请求
		c.Next()

		// 记录请求日志（INFO级别）
		responseTime := time.Since(start)

		// 获取请求协议（优先使用X-Forwarded-Proto头部）
		proto := c.GetHeader("X-Forwarded-Proto")
		if proto == "" {
			proto = c.Request.URL.Scheme
		}
		if proto == "" {
			proto = "http" // 默认协议
		}

		utils.Log.Info(ctx, "HTTP请求",
			zap.String("trace_id", traceID),                 // 添加trace_id
			zap.String("proto", proto),                      // 请求协议（http/https）
			zap.String("client", c.ClientIP()),              // 客户端IP
			zap.String("method", c.Request.Method),          // 请求方法
			zap.String("url", url),                          // 请求路径
			zap.Int("status", c.Writer.Status()),            // HTTP状态码
			zap.String("response_time", responseTime.String()), // 总响应时间
			zap.Int("size", c.Writer.Size()))                // 响应体大小
	})

	router.Use(gin.Recovery())

	// 单独记录/metrics端点的访问日志
	router.GET("/metrics", func(c *gin.Context) {
		utils.Log.Debug(c.Request.Context(), "指标收集请求",
			zap.String("client", c.ClientIP()))
		promhttp.Handler().ServeHTTP(c.Writer, c.Request)
	})

	// 创建启动日志的上下文
	startCtx := context.Background()
	utils.Log.Info(startCtx, "启动HTTP服务器",
		zap.String("address", ":"+cfg.Server.Port),
		zap.String("gin_mode", cfg.Server.GinMode),
		zap.String("log_level", cfg.Server.LogLevel))

	// 启动服务器（阻塞操作）
	err := router.Run(":" + cfg.Server.Port)
	if err != nil {
		utils.Log.Fatal(context.Background(), "HTTP服务启动失败",
			zap.Error(err),
			zap.String("code", CodeHTTPServerStartFailure))
	} else {
		utils.Log.Info(context.Background(), "HTTP服务已停止")
	}
}