package client

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"time"
	
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"tcp-exporter/utils"
	"tcp-exporter/collector"
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
	if podName == "" {
		// 非容器环境回退处理
		hostname, _ := os.Hostname()
		if hostname == "" {
			// 生成基于时间戳和进程ID的唯一标识
			timestamp := time.Now().UnixNano()
			pid := os.Getpid()
			podName = "local-" + strconv.FormatInt(timestamp, 10) + "-" + strconv.Itoa(pid)
			utils.Log.Info(context.Background(), "生成唯一身份标识",
				zap.String("identity", podName))
		} else {
			podName = hostname
			utils.Log.Info(context.Background(), "使用主机名作为身份标识",
				zap.String("identity", podName))
		}
	}
	
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
				// 创建带有trace_id的新上下文
				traceCtx := context.WithValue(ctx, utils.TraceIDKey, "election-event")
				// 记录选举成功指标
				collector.RecordElectionSuccess(traceCtx, podName)
				
				// 设置初始状态（所有实例从非leader开始）
				collector.SetLeaderStatus(podName, 0)
				// 原子标记为领导者
				atomic.StoreInt32(&leaderStatus, 1)
				// 更新指标状态为leader
				collector.SetLeaderStatus(podName, 1)
				// 启动任期更新
				collector.StartTenureUpdate(podName)
				utils.Log.Info(traceCtx, "成为领导者，负责生成快照")
				leaderFunc(traceCtx) // 使用带有trace_id的上下文
			},
			OnStoppedLeading: func() {
				// 记录领导结束指标
				collector.RecordLeadershipEnd(podName)
				
				// 原子标记为跟随者
				atomic.StoreInt32(&leaderStatus, 0)
				// 设置指标状态
				collector.SetLeaderStatus(podName, 0)
				// 停止任期更新
				collector.StopTenureUpdate()
				// 创建带有trace_id的新上下文
				traceCtx := context.WithValue(context.Background(), utils.TraceIDKey, "election-event")
				utils.Log.Debug(traceCtx, "失去领导权")
			},
			OnNewLeader: func(identity string) {
				if identity == podName {
					return
				}
				// 创建带有trace_id的新上下文
				traceCtx := context.WithValue(context.Background(), utils.TraceIDKey, "election-event")
				
				// 仅当状态变更时记录
				if atomic.LoadInt32(&leaderStatus) == 1 {
					utils.Log.Debug(traceCtx, "放弃领导权",
						zap.String("newLeader", identity))
				} else {
					utils.Log.Debug(traceCtx, "当前领导者",
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
	
	// 创建带有trace_id的上下文
	traceCtx := context.WithValue(ctx, utils.TraceIDKey, "leader-election")
	utils.Log.Debug(traceCtx, "领导者选举协程已启动")
}