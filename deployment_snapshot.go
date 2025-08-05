package main

import (
	"encoding/json"
	"os"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// SnapshotManager 管理Kubernetes快照的创建和持久化
type SnapshotManager struct {
	deploymentLister appslisters.DeploymentLister
	podLister        corelisters.PodLister
	cfg              *Config
	logger           *zap.Logger
}

// NewSnapshotManager 创建新的快照管理器
func NewSnapshotManager(deploymentLister appslisters.DeploymentLister, podLister corelisters.PodLister, cfg *Config, logger *zap.Logger) *SnapshotManager {
	return &SnapshotManager{
		deploymentLister: deploymentLister,
		podLister:        podLister,
		cfg:              cfg,
		logger:           logger,
	}
}

// LoadSnapshot 从文件加载快照
func LoadSnapshot(path string, logger *zap.Logger) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		logger.Error("加载快照文件失败",
			zap.String("path", path),
			zap.Error(err))
		return nil, err
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		logger.Error("解析快照文件失败",
			zap.String("path", path),
			zap.Error(err))
		return nil, err
	}

	logger.Info("成功加载快照",
		zap.String("path", path),
		zap.Int("namespaces", len(snap.Namespaces)))

	return &snap, nil
}

// FilterSnapshot 根据允许的部署列表过滤快照
func FilterSnapshot(snap *Snapshot, allowedDeployments []string) *Snapshot {
	if len(allowedDeployments) == 0 {
		return snap
	}

	// 将允许的部署列表转换为map以便快速查找
	allowedMap := make(map[string]bool)
	for _, dep := range allowedDeployments {
		allowedMap[dep] = true
	}

	filtered := Snapshot{
		Namespaces: make(map[string]NamespaceSnapshot),
	}

	for ns, nsSnapshot := range snap.Namespaces {
		filteredDeployments := make(map[string][]string)
		
		for dep, pods := range nsSnapshot.Deployments {
			if allowedMap[dep] {
				filteredDeployments[dep] = pods
			}
		}
		
		if len(filteredDeployments) > 0 {
			filtered.Namespaces[ns] = NamespaceSnapshot{
				Deployments: filteredDeployments,
			}
		}
	}

	return &filtered
}

// ToFile 将当前快照写入文件
func (sm *SnapshotManager) ToFile() error {
	totalDeployments := 0
	totalPods := 0
	snapshot := Snapshot{
		Namespaces: make(map[string]NamespaceSnapshot),
	}

	logger.Debug("开始生成快照")
	for _, ns := range sm.cfg.Kubernetes.TargetNamespaces {
		logger.Debug("处理命名空间",
			zap.String("namespace", ns))
		nsSnapshot := NamespaceSnapshot{
			Deployments: make(map[string][]string),
		}

		deps, err := sm.deploymentLister.Deployments(ns).List(labels.Everything())
		if err != nil {
			logger.Error("列出部署失败", zap.String("namespace", ns), zap.Error(err))
			continue
		}

		logger.Debug("命名空间中的部署数量",
			zap.String("namespace", ns),
			zap.Int("count", len(deps)))
		totalDeployments += len(deps)

		for _, dep := range deps {
			selector := labels.Set(dep.Spec.Selector.MatchLabels).AsSelector()
			pods, listErr := sm.podLister.Pods(ns).List(selector)
			if listErr != nil {
				logger.Warn("列出Pod失败",
					zap.String("deployment", dep.Name),
					zap.String("namespace", ns),
					zap.Error(listErr))
				continue
			}

			var podNames []string
			for _, p := range pods {
				podNames = append(podNames, p.Name)
				totalPods++
			}

			nsSnapshot.Deployments[dep.Name] = podNames
			logger.Debug("为部署添加Pod",
				zap.String("deployment", dep.Name),
				zap.Int("podCount", len(podNames)))
		}

		snapshot.Namespaces[ns] = nsSnapshot
	}

	// 使用缩进格式美化JSON输出
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		logger.Error("序列化快照失败", zap.Error(err))
		return err
	}

	tmp := sm.cfg.Kubernetes.CacheFilePath + ".tmp"
	logger.Debug("写入临时快照文件",
		zap.String("path", tmp),
		zap.Int("size", len(data)))
	if err = os.WriteFile(tmp, data, 0644); err != nil {
		logger.Error("写入临时文件失败",
			zap.String("path", tmp),
			zap.Error(err))
		return err
	}

	logger.Debug("重命名临时文件为正式文件",
		zap.String("tmp", tmp),
		zap.String("target", sm.cfg.Kubernetes.CacheFilePath))
	err = os.Rename(tmp, sm.cfg.Kubernetes.CacheFilePath)
	if err != nil {
		logger.Error("重命名文件失败",
			zap.String("tmp", tmp),
			zap.String("target", sm.cfg.Kubernetes.CacheFilePath),
			zap.Error(err))
	} else {
		logger.Info("快照写入成功",
			zap.String("path", sm.cfg.Kubernetes.CacheFilePath),
			zap.Int("namespaces", len(snapshot.Namespaces)),
			zap.Int("deployments", totalDeployments),
			zap.Int("pods", totalPods))
		
		// 通知分片协调器快照更新
		if shardCoordinator != nil {
			snap, loadErr := LoadSnapshot(sm.cfg.Kubernetes.CacheFilePath, sm.logger)
			if loadErr != nil {
				logger.Error("加载快照失败，无法通知分片协调器",
					zap.String("path", sm.cfg.Kubernetes.CacheFilePath),
					zap.Error(loadErr))
				return err
			}
			
			logger.Info("通知分片协调器快照更新",
				zap.Int("namespaces", len(snap.Namespaces)))
			shardCoordinator.HandleSnapshotUpdate(snap)
		}
	}
	return err
}

// RunWatcher 启动快照文件监视器
func (sm *SnapshotManager) RunWatcher(factory informers.SharedInformerFactory) {
	if err := sm.ToFile(); err != nil {
		logger.Error("首次写入 Pod 快照失败", zap.Error(err))
	}

	podInformer := factory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			if !contains(sm.cfg.Kubernetes.TargetNamespaces, pod.Namespace) {
				return
			}
			logger.Info("Pod 已添加，刷新快照文件", zap.String("pod", pod.Name), zap.String("namespace", pod.Namespace))
			_ = sm.ToFile()
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			if !contains(sm.cfg.Kubernetes.TargetNamespaces, pod.Namespace) {
				return
			}
			logger.Info("Pod 已删除，刷新快照文件", zap.String("pod", pod.Name), zap.String("namespace", pod.Namespace))
			_ = sm.ToFile()
		},
	})
	
	// 添加定时更新快照的循环
	go func() {
		ticker := time.NewTicker(5 * time.Minute) // 每5分钟刷新一次
		defer ticker.Stop()
		
		for range ticker.C {
			logger.Info("定时刷新快照")
			_ = sm.ToFile()
		}
	}()
}

// 全局分片协调器实例
var shardCoordinator *ShardCoordinator

// SetShardCoordinator 设置分片协调器实例
func SetShardCoordinator(coordinator *ShardCoordinator) {
	shardCoordinator = coordinator
}


func contains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}