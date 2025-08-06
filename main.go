package main

import (
	"context"
	"os"
	"runtime/debug"
	
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

	utils.Log.Debug(context.Background(), "创建快照管理器")
	snapshotManager := snapshot.NewSnapshotManager(
		factory.Apps().V1().Deployments().Lister(),
		factory.Core().V1().Pods().Lister(),
		cfg,
	)
	utils.Log.Info(context.Background(), "启动快照管理器监听")
	go snapshotManager.RunWatcher(factory)

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
	service.RunHTTPServer(cfg)
}