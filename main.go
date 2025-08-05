package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	configPath := flag.String("config", "config/config.yaml", "Path to config file")
	flag.Parse()

	// 初始化日志
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// 加载配置
	cfg, err := LoadConfig(*configPath)
	if err != nil {
		logger.Fatal("加载配置失败", zap.Error(err))
	}

	// 创建Kubernetes客户端
	var clientset *kubernetes.Clientset
	if cfg.Kubernetes.InCluster {
		config, err := rest.InClusterConfig()
		if err != nil {
			logger.Fatal("创建集群内配置失败", zap.Error(err))
		}
		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			logger.Fatal("创建集群内客户端失败", zap.Error(err))
		}
		logger.Info("使用集群内Kubernetes客户端")
	} else {
		kubeconfig := clientcmd.NewDefaultClientConfigLoadingRules().GetDefaultFilename()
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			logger.Fatal("创建集群外配置失败", zap.Error(err))
		}
		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			logger.Fatal("创建集群外客户端失败", zap.Error(err))
		}
		logger.Info("使用集群外Kubernetes客户端", zap.String("kubeconfig", kubeconfig))
	}

	// 启动HTTP服务器
	go func() {
		http.Handle("/metrics", metricsHandler())
		addr := fmt.Sprintf(":%d", cfg.Server.Port)
		logger.Info("启动HTTP服务器", zap.String("addr", addr))
		if err := http.ListenAndServe(addr, nil); err != nil {
			logger.Fatal("HTTP服务器启动失败", zap.Error(err))
		}
	}()

	// 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Info("收到退出信号，关闭程序")
}