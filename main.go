package main

import (
	"context"
	"os"
	"runtime/debug"
	
	"tcp-exporter/utils"
	"go.uber.org/zap"
)

func main() {
	// 先加载配置
	cfg, configPath := mustLoadConfig()

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
	clientset, restConfig, factory := initK8sClient(context.Background(), cfg)
	utils.Log.Info(context.Background(), "Kubernetes客户端初始化成功")

	utils.Log.Debug(context.Background(), "创建快照管理器")
	snapshotManager := NewSnapshotManager(
		factory.Apps().V1().Deployments().Lister(),
		factory.Core().V1().Pods().Lister(),
		cfg,
	)
	utils.Log.Info(context.Background(), "启动快照管理器监听")
	go snapshotManager.RunWatcher(factory)

	collector := registerCollector(clientset, restConfig, cfg, factory)
	utils.Log.Info(context.Background(), "指标收集器注册成功")

	go watchConfig(configPath, func(newCfg *Config) {
		utils.Log.Info(context.Background(), "配置热重载完成",
			zap.String("logLevel", newCfg.Server.LogLevel),
			zap.Strings("ignoreContainers", newCfg.Kubernetes.IgnoreContainers))
		collector.ignoreSet = buildIgnoreSet(newCfg.Kubernetes.IgnoreContainers)
	})

	utils.Log.Info(context.Background(), "启动HTTP服务器",
		zap.String("address", ":"+cfg.Server.Port))
	runHTTPServer(cfg)
}