package collector

import (
	"context"
	"sync"
	"time"
	"github.com/prometheus/client_golang/prometheus"
	"tcp-exporter/utils"
	"go.uber.org/zap"
)

var (
	// leaderElectionSuccess 统计成功当选leader的次数
	leaderElectionSuccess = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leader_election_success_total",
			Help: "成功当选leader的次数",
		},
		[]string{"trace_id", "pod_name"},
	)
	
	// leaderTenureSeconds 记录当前leader的任期时长(秒)
	leaderTenureSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "leader_tenure_seconds",
			Help: "当前leader任期时长(秒)",
		},
		[]string{"pod_name"},
	)
	
	// leaderCurrentTenureSeconds 记录当前leader的实时任期时长(秒)
	leaderCurrentTenureSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "leader_current_tenure_seconds",
			Help: "当前leader的实时任期时长(秒)",
		},
		[]string{"pod_name"},
	)
	
	// leaderStatus 表示当前实例是否为leader(1=是,0=否)
	leaderStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "leader_status",
			Help: "当前实例是否为leader(1=是,0=否)",
		},
		[]string{"pod_name"},
	)
	
	leaderStartTime      time.Time // 记录任期开始时间
	tenureUpdateCancel   context.CancelFunc // 用于取消任期更新goroutine
	tenureUpdateMutex    sync.Mutex         // 保护任期更新goroutine的并发访问
)

// init 函数在包初始化时注册Prometheus指标
func init() {
	prometheus.MustRegister(leaderElectionSuccess)
	prometheus.MustRegister(leaderTenureSeconds)
	prometheus.MustRegister(leaderCurrentTenureSeconds)
	prometheus.MustRegister(leaderStatus)
}

// getTraceID 从上下文中获取 trace ID
func getTraceID(ctx context.Context) string {
	if traceID, ok := ctx.Value(utils.TraceIDKey).(string); ok {
		return traceID
	}
	return ""
}

// SetLeaderStatus 设置当前实例的领导状态
func SetLeaderStatus(podName string, status int) {
	leaderStatus.WithLabelValues(podName).Set(float64(status))
}

// StartTenureUpdate 启动任期时长更新goroutine
func StartTenureUpdate(podName string) {
	tenureUpdateMutex.Lock()
	defer tenureUpdateMutex.Unlock()
	
	// 如果已有goroutine在运行，先停止
	if tenureUpdateCancel != nil {
		tenureUpdateCancel()
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	tenureUpdateCancel = cancel
	
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				tenure := time.Since(leaderStartTime).Seconds()
				leaderCurrentTenureSeconds.WithLabelValues(podName).Set(tenure)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopTenureUpdate 停止任期时长更新
func StopTenureUpdate() {
	tenureUpdateMutex.Lock()
	defer tenureUpdateMutex.Unlock()
	
	if tenureUpdateCancel != nil {
		tenureUpdateCancel()
		tenureUpdateCancel = nil
	}
}

// RecordElectionSuccess 记录成功当选leader事件
// 参数:
//   ctx: 上下文，用于获取trace_id
//   podName: 当前Pod的名称标识
func RecordElectionSuccess(ctx context.Context, podName string) {
	traceID := getTraceID(ctx)
	// 添加选举成功日志
	utils.Log.Info(ctx, "记录选举成功指标",
		zap.String("trace_id", traceID),
		zap.String("pod_name", podName))
	
	leaderElectionSuccess.WithLabelValues(traceID, podName).Inc()
	leaderStartTime = time.Now()
	leaderStatus.WithLabelValues(podName).Set(1)
}

// RecordLeadershipEnd 记录领导结束事件
// 参数:
//   podName: 当前Pod的名称标识
func RecordLeadershipEnd(podName string) {
	tenure := time.Since(leaderStartTime).Seconds()
	// 添加领导结束日志
	utils.Log.Info(context.Background(), "记录领导结束指标",
		zap.String("pod_name", podName),
		zap.Float64("tenure_seconds", tenure))
	
	leaderTenureSeconds.WithLabelValues(podName).Set(tenure)
	leaderStatus.WithLabelValues(podName).Set(0)
}