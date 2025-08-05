package main

import (
        "fmt"

        "go.uber.org/zap"
        "k8s.io/client-go/informers"
        "k8s.io/client-go/kubernetes"
        "k8s.io/client-go/rest"
        "k8s.io/client-go/tools/cache"
        "k8s.io/client-go/tools/clientcmd"
)

func initK8sClient(cfg *Config, logger *zap.Logger) (*kubernetes.Clientset, *rest.Config, informers.SharedInformerFactory) {
        clientset, restConfig, err := K8sClientFactory(cfg, logger)
        if err != nil {
                logger.Fatal("构建 Kubernetes 客户端失败", zap.Error(err))
        }
        factory := informers.NewSharedInformerFactoryWithOptions(
                clientset,
                0,
        )
        depInformer := factory.Apps().V1().Deployments().Informer()
        podInformer := factory.Core().V1().Pods().Informer()
        stopCh := make(chan struct{})
        factory.Start(stopCh)

        logger.Debug("等待缓存同步")
        if !cache.WaitForCacheSync(stopCh, depInformer.HasSynced, podInformer.HasSynced) {
        	logger.Warn("缓存同步超时或失败")
        } else {
        	logger.Info("缓存同步完成")
        }
        
        logger.Info("Kubernetes 客户端和 informer 初始化完成",
        	zap.Bool("use_in_cluster", cfg.Kubernetes.UseInCluster),
        	zap.String("kubeconfig_path", cfg.Kubernetes.KubeConfigPath),
        	zap.String("api_server", restConfig.Host))
        return clientset, restConfig, factory
}

func K8sClientFactory(cfg *Config, logger *zap.Logger) (*kubernetes.Clientset, *rest.Config, error) {
        logger.Debug("K8sClientFactory: 开始构建 Kubernetes 客户端",
                zap.Bool("use_in_cluster", cfg.Kubernetes.UseInCluster))

        var restConfig *rest.Config
        var err error

        if cfg.Kubernetes.UseInCluster {
                logger.Info("使用 InClusterConfig 构建 Kubernetes 客户端")
                restConfig, err = rest.InClusterConfig()
                if err != nil {
                        logger.Error("InClusterConfig 获取失败", zap.Error(err), zap.String("mode", "in-cluster"))
                        return nil, nil, fmt.Errorf("获取 InCluster 配置失败: %w", err)
                }
        } else {
                logger.Info("使用外部 kubeconfig 构建 Kubernetes 客户端",
                        zap.String("kubeconfig_path", cfg.Kubernetes.KubeConfigPath))
                restConfig, err = clientcmd.BuildConfigFromFlags("", cfg.Kubernetes.KubeConfigPath)
                if err != nil {
                        logger.Error("构建kubeconfig配置失败",
                                zap.Error(err),
                                zap.String("kubeconfig_path", cfg.Kubernetes.KubeConfigPath))
                        return nil, nil, fmt.Errorf("读取 kubeconfig 失败: %w", err)
                }
        }

        clientset, err := kubernetes.NewForConfig(restConfig)
        if err != nil {
                logger.Error("创建Kubernetes客户端失败", zap.Error(err))
                return nil, nil, fmt.Errorf("创建 Kubernetes 客户端失败: %w", err)
        }
        logger.Info("Kubernetes 客户端创建成功",
        	zap.String("api_server", restConfig.Host))
        return clientset, restConfig, nil
}