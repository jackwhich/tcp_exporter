// Package snapshot 提供 Kubernetes 部署和 Pod 快照管理功能，
// 支持快照加载、保存和监视 Pod 变更时自动更新快照
package snapshot

import (
	"context"
	"encoding/json"
	"os"
	
	"github.com/google/uuid"
	"tcp-exporter/config"
	"tcp-exporter/utils"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// Snapshot 表示整个集群的快照
type Snapshot struct {
	Namespaces map[string]NamespaceSnapshot `json:"namespaces"`
}

// NamespaceSnapshot 表示单个命名空间的快照
type NamespaceSnapshot struct {
	Deployments map[string][]string `json:"deployments"`
}

// SnapshotManager 管理Kubernetes快照的创建和持久化
type SnapshotManager struct {
        deploymentLister appslisters.DeploymentLister
        podLister        corelisters.PodLister
        cfg              *config.Config
}

// NewSnapshotManager 创建新的快照管理器
func NewSnapshotManager(deploymentLister appslisters.DeploymentLister, podLister corelisters.PodLister, cfg *config.Config) *SnapshotManager {
        return &SnapshotManager{
                deploymentLister: deploymentLister,
                podLister:        podLister,
                cfg:              cfg,
        }
}

// LoadSnapshot 从文件加载快照
func LoadSnapshot(ctx context.Context, path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		utils.Log.Error(ctx, "加载快照文件失败",
			zap.String("path", path),
			zap.Error(err))
		return nil, err
	}

	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		utils.Log.Error(ctx, "解析快照文件失败",
			zap.String("path", path),
			zap.Error(err))
		return nil, err
	}

	utils.Log.Info(ctx, "成功加载快照",
		zap.String("path", path),
		zap.Int("namespaces", len(snap.Namespaces)))

	return &snap, nil
}

// ToFile 将当前快照写入文件（添加context参数）
func (sm *SnapshotManager) ToFile(ctx context.Context) error {
	totalDeployments := 0
	totalPods := 0
	snapshot := Snapshot{
		Namespaces: make(map[string]NamespaceSnapshot),
	}

	utils.Log.Debug(ctx, "开始生成快照")
	for _, ns := range sm.cfg.Kubernetes.TargetNamespaces {
		utils.Log.Trace(ctx, "处理命名空间",
			zap.String("namespace", ns))
		nsSnapshot := NamespaceSnapshot{
			Deployments: make(map[string][]string),
		}

		deps, err := sm.deploymentLister.Deployments(ns).List(labels.Everything())
		if err != nil {
			utils.Log.Error(ctx, "列出部署失败", zap.String("namespace", ns), zap.Error(err))
			continue
		}

		utils.Log.Trace(ctx, "命名空间中的部署数量",
			zap.String("namespace", ns),
			zap.Int("count", len(deps)))
		totalDeployments += len(deps)

		for _, dep := range deps {
			selector := labels.Set(dep.Spec.Selector.MatchLabels).AsSelector()
			pods, listErr := sm.podLister.Pods(ns).List(selector)
			if listErr != nil {
				utils.Log.Warn(ctx, "列出Pod失败",
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
			utils.Log.Trace(ctx, "为部署添加Pod",
				zap.String("deployment", dep.Name),
				zap.Int("podCount", len(podNames)))
		}

		snapshot.Namespaces[ns] = nsSnapshot
	}

	// 使用缩进格式美化JSON输出
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		utils.Log.Error(ctx, "序列化快照失败", zap.Error(err))
		return err
	}

	tmp := sm.cfg.Kubernetes.CacheFilePath + ".tmp"
	utils.Log.Debug(ctx, "写入临时快照文件",
		zap.String("path", tmp),
		zap.Int("size", len(data)))
	if err = os.WriteFile(tmp, data, 0644); err != nil {
		utils.Log.Error(ctx, "写入临时文件失败",
			zap.String("path", tmp),
			zap.Error(err))
		return err
	}

	utils.Log.Debug(ctx, "重命名临时文件为正式文件",
		zap.String("tmp", tmp),
		zap.String("target", sm.cfg.Kubernetes.CacheFilePath))
	err = os.Rename(tmp, sm.cfg.Kubernetes.CacheFilePath)
	if err != nil {
		utils.Log.Error(ctx, "重命名文件失败",
			zap.String("tmp", tmp),
			zap.String("target", sm.cfg.Kubernetes.CacheFilePath),
			zap.Error(err))
	} else {
		utils.Log.Info(ctx, "快照写入成功",
			zap.String("path", sm.cfg.Kubernetes.CacheFilePath),
			zap.Int("namespaces", len(snapshot.Namespaces)),
			zap.Int("deployments", totalDeployments),
			zap.Int("pods", totalPods))
	}
	return err
}

// RunWatcher 启动快照文件监视器
func (sm *SnapshotManager) RunWatcher(factory informers.SharedInformerFactory) {
	// 创建带trace ID的context（使用自定义key类型）
	taskCtx := context.WithValue(context.Background(), utils.TraceIDKey, "task-"+uuid.NewString())
	
	if err := sm.ToFile(taskCtx); err != nil {
		utils.Log.Error(taskCtx, "首次写入 Pod 快照失败", zap.Error(err))
	}

	podInformer := factory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// 为每个事件创建独立的trace ID（使用自定义key类型）
			eventCtx := context.WithValue(context.Background(), utils.TraceIDKey, "event-"+uuid.NewString())
			pod := obj.(*corev1.Pod)
			if !contains(sm.cfg.Kubernetes.TargetNamespaces, pod.Namespace) {
				return
			}
			utils.Log.Info(eventCtx, "Pod 已添加，刷新快照文件",
				zap.String("pod", pod.Name),
				zap.String("namespace", pod.Namespace))
			_ = sm.ToFile(eventCtx)
		},
		DeleteFunc: func(obj interface{}) {
			// 为每个事件创建独立的trace ID（使用自定义key类型）
			eventCtx := context.WithValue(context.Background(), utils.TraceIDKey, "event-"+uuid.NewString())
			pod := obj.(*corev1.Pod)
			if !contains(sm.cfg.Kubernetes.TargetNamespaces, pod.Namespace) {
				return
			}
			utils.Log.Info(eventCtx, "Pod 已删除，刷新快照文件",
				zap.String("pod", pod.Name),
				zap.String("namespace", pod.Namespace))
			_ = sm.ToFile(eventCtx)
		},
	})
}

func contains(slice []string, s string) bool {
        for _, item := range slice {
                if item == s {
                        return true
                }
        }
        return false
}