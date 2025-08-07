package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"
	
	"tcp-exporter/client"
	"tcp-exporter/config"
	"tcp-exporter/service"
	"tcp-exporter/utils"
	"tcp-exporter/snapshot"
	"tcp-exporter/collector"  // 新增collector包导入
	"go.uber.org/zap"
)

func main() {
	// 先加载配置
	cfg, configPath := config.MustLoadConfig()

	// 初始化全局logger
	utils.InitLogger(cfg)

	defer func() {
		if r := recover(); r != nil {
			utils.Log.Error(context.Background(), "程序崩溃",
				zap.Any("reason", r),
				zap.String("stack", string(debug.Stack())))
			os.Exit(1)
		}
	}()

	utils.Log.Info(context.Background(), "启动TCP队列指标导出器")
	utils.Log.Info(context.Background(), "配置加载成功",
		zap.String("path", configPath),
		zap.String("logLevel", cfg.Server.LogLevel),
		zap.String("cacheFilePath", cfg.Kubernetes.CacheFilePath))

	utils.Log.Debug(context.Background(), "初始化Kubernetes客户端")
	clientset, restConfig, factory := client.InitK8sClient(context.Background(), cfg)
	utils.Log.Info(context.Background(), "Kubernetes客户端初始化成功")

	// 使用高效的领导者选举
	client.RunLeaderElection(context.Background(), clientset, cfg.Kubernetes.TargetNamespaces[0],
		func(leaderCtx context.Context) { // 领导者函数
			utils.Log.Debug(leaderCtx, "创建快照管理器")
			snapshotManager := snapshot.NewSnapshotManager(
				factory.Apps().V1().Deployments().Lister(),
				factory.Core().V1().Pods().Lister(),
				cfg,
			)
			utils.Log.Info(leaderCtx, "启动快照管理器监听")
			go snapshotManager.RunWatcher(factory)
		},
	)

	collector := collector.RegisterCollector(clientset, restConfig, cfg, factory)
	utils.Log.Info(context.Background(), "指标收集器注册成功")

	go config.WatchConfig(configPath, func(newCfg *config.Config) {
		utils.Log.Info(context.Background(), "配置热重载完成",
			zap.String("logLevel", newCfg.Server.LogLevel),
			zap.Strings("ignoreContainers", newCfg.Kubernetes.IgnoreContainers))
		collector.SetIgnoreSet(newCfg.Kubernetes.IgnoreContainers)
	})

	utils.Log.Info(context.Background(), "启动HTTP服务器",
		zap.String("address", ":"+cfg.Server.Port))
	srv := service.RunHTTPServer(cfg)
	
	// 初始化优雅关闭处理器
	shutdown := service.NewShutdownHandler(srv)
	
	// 设置信号监听
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)
	
	// 等待终止信号
	<-stopCh
	utils.Log.Info(context.Background(), "接收到终止信号，开始优雅关闭")
	
	// 执行优雅关闭（30秒超时）
	if err := shutdown.Shutdown(30 * time.Second); err != nil {
		utils.Log.Error(context.Background(), "优雅关闭失败", zap.Error(err))
	} else {
		utils.Log.Info(context.Background(), "服务已优雅退出")
	}
}