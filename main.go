package main

import (
        "os"
        "runtime/debug"
        "go.uber.org/zap"
        "tcp-exporter/pkg/utils"
)

var logger *utils.Logger

func main() {
        // 先加载配置
        cfg, configPath := mustLoadConfig()

        // 初始化logger
        logger = utils.NewLogger(cfg.Server.LogLevel)

        defer func() {
                if r := recover(); r != nil {
                        logger.Error("程序崩溃",
                                zap.Any("reason", r),
                                zap.String("stack", string(debug.Stack())))
                        os.Exit(1)
                }
        }()

        logger.Info("启动TCP队列指标导出器")
        logger.Info("配置加载成功",
                zap.String("path", configPath),
                zap.String("logLevel", cfg.Server.LogLevel),
                zap.String("cacheFilePath", cfg.Kubernetes.CacheFilePath))

        logger.Debug("初始化Kubernetes客户端")
        clientset, restConfig, factory := initK8sClient(cfg)
        logger.Info("Kubernetes客户端初始化成功")

        logger.Debug("创建快照管理器")
        snapshotManager := NewSnapshotManager(
                factory.Apps().V1().Deployments().Lister(),
                factory.Core().V1().Pods().Lister(),
                cfg,
        )
        logger.Info("启动快照管理器监听")
        go snapshotManager.RunWatcher(factory)

        collector := registerCollector(clientset, restConfig, cfg, factory)
        logger.Info("指标收集器注册成功")

        go watchConfig(configPath, func(newCfg *Config) {
                logger.Info("配置热重载完成",
                        zap.String("logLevel", newCfg.Server.LogLevel),
                        zap.Strings("ignoreContainers", newCfg.Kubernetes.IgnoreContainers))
                collector.ignoreSet = buildIgnoreSet(newCfg.Kubernetes.IgnoreContainers)
        })

        logger.Info("启动HTTP服务器",
                zap.String("address", ":"+cfg.Server.Port))
        runHTTPServer(cfg)
}