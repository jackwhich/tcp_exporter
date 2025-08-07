package collector

import (
	"context"
	"time"
	"github.com/prometheus/client_golang/prometheus"
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
	
	// leaderStatus 表示当前实例是否为leader(1=是,0=否)
	leaderStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "leader_status",
			Help: "当前实例是否为leader(1=是,0=否)",
		},
		[]string{"pod_name"},
	)
	
	leaderStartTime time.Time // 记录任期开始时间
)

// init 函数在包初始化时注册Prometheus指标
func init() {
	prometheus.MustRegister(leaderElectionSuccess)
	prometheus.MustRegister(leaderTenureSeconds)
	prometheus.MustRegister(leaderStatus)
}

// getTraceID 从上下文中获取 trace ID
func getTraceID(ctx context.Context) string {
	if traceID, ok := ctx.Value("traceID").(string); ok {
		return traceID
	}
	return ""
}

// RecordElectionSuccess 记录成功当选leader事件
// 参数:
//   ctx: 上下文，用于获取trace_id
//   podName: 当前Pod的名称标识
func RecordElectionSuccess(ctx context.Context, podName string) {
	traceID := getTraceID(ctx)
	leaderElectionSuccess.WithLabelValues(traceID, podName).Inc()
	leaderStartTime = time.Now()
	leaderStatus.WithLabelValues(podName).Set(1)
}

// RecordLeadershipEnd 记录领导结束事件
// 参数:
//   podName: 当前Pod的名称标识
func RecordLeadershipEnd(podName string) {
	tenure := time.Since(leaderStartTime).Seconds()
	leaderTenureSeconds.WithLabelValues(podName).Set(tenure)
	leaderStatus.WithLabelValues(podName).Set(0)
}