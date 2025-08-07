package service

import (
	"context"
	"net/http"
	"time"
	
	"tcp-exporter/utils"
	"go.uber.org/zap"
)

// ShutdownHandler 管理HTTP服务器的优雅关闭
type ShutdownHandler struct {
	srv *http.Server
}

// NewShutdownHandler 创建新的优雅关闭处理器
func NewShutdownHandler(srv *http.Server) *ShutdownHandler {
	return &ShutdownHandler{srv: srv}
}

// Shutdown 执行优雅关闭操作
func (h *ShutdownHandler) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	
	utils.Log.Info(ctx, "开始优雅关闭HTTP服务器",
		zap.String("timeout", timeout.String()))
	
	if err := h.srv.Shutdown(ctx); err != nil {
		utils.Log.Error(ctx, "HTTP服务器关闭失败", zap.Error(err))
		return err
	}
	
	utils.Log.Info(ctx, "HTTP服务器已优雅关闭")
	return nil
}