package client

import (
	"context"
	"os"
	"sync/atomic"
	"time"
	
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"tcp-exporter/utils"
)

// leaderStatus 用于原子操作跟踪领导者状态
var leaderStatus int32 // 0: follower, 1: leader

// RunLeaderElection 启动高性能领导者选举过程
func RunLeaderElection(
	ctx context.Context,
	clientset *kubernetes.Clientset,
	namespace string,
	leaderFunc func(context.Context),
) {
	lockName := "tcp-exporter-leader"
	podName := os.Getenv("POD_NAME")
	
	// 创建Lease锁
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      lockName,
			Namespace: namespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}
	
	// 优化的选举配置
	lec := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   60 * time.Second, // 延长租约时间
		RenewDeadline:   30 * time.Second, // 延长续约时间
		RetryPeriod:     5 * time.Second,  // 平衡重试频率
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// 原子标记为领导者
				atomic.StoreInt32(&leaderStatus, 1)
				utils.Log.Info(ctx, "成为领导者，负责生成快照")
				leaderFunc(ctx)
			},
			OnStoppedLeading: func() {
				// 原子标记为跟随者
				atomic.StoreInt32(&leaderStatus, 0)
				utils.Log.Debug(context.Background(), "失去领导权") // 改为Debug级别
			},
			OnNewLeader: func(identity string) {
				if identity == podName {
					return
				}
				// 仅当状态变更时记录
				if atomic.LoadInt32(&leaderStatus) == 1 {
					utils.Log.Debug(context.Background(), "放弃领导权",
						zap.String("newLeader", identity))
				} else {
					utils.Log.Debug(context.Background(), "当前领导者",
						zap.String("leader", identity))
				}
			},
		},
	}
	
	// 异步运行领导者选举
	go func() {
		leaderelection.RunOrDie(ctx, lec)
		utils.Log.Debug(context.Background(), "领导者选举协程退出")
	}()
	
	utils.Log.Debug(ctx, "领导者选举协程已启动")
}