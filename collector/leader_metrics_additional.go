package collector

import (
	"context"
	"time"
	"tcp-exporter/utils"
	"go.uber.org/zap"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// 领导者选举总次数指标
	leaderElectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "leader_elections_total",
			Help: "领导者选举事件总数",
		},
		[]string{"outcome"},
	)
	
	// 领导者选举耗时指标
	leaderElectionDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "leader_election_duration_seconds",
			Help:    "领导者选举过程耗时（秒）",
			Buckets: prometheus.DefBuckets,
		},
	)
	
	// 当前领导者状态指标
	leaderStatusGauge prometheus.Gauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "leader_status",
			Help: "当前领导者状态（1=领导者，0=跟随者）",
		},
	)
)

func init() {
	// 注册所有指标
	prometheus.MustRegister(leaderElectionsTotal)
	prometheus.MustRegister(leaderElectionDurationSeconds)
	prometheus.MustRegister(leaderStatusGauge)
}

// RecordElectionStart 记录选举开始时间
// 返回开始时间点用于计算持续时间
func RecordElectionStart(ctx context.Context) time.Time {
	start := time.Now()
	utils.Log.Info(ctx, "开始领导者选举")
	return start
}

// RecordElectionResult 记录选举结果
// ctx: 包含trace_id的上下文
// startTime: 选举开始时间
// success: 选举是否成功
func RecordElectionResult(ctx context.Context, startTime time.Time, success bool) {
	duration := time.Since(startTime).Seconds()
	leaderElectionDurationSeconds.Observe(duration)
	
	outcome := "failure"
	if success {
		outcome = "success"
	}
	leaderElectionsTotal.WithLabelValues(outcome).Inc()
	
	utils.Log.Info(ctx, "记录选举结果", 
		zap.String("outcome", outcome),
		zap.Float64("duration", duration))
}

// UpdateLeaderStatus 更新当前领导者状态
// ctx: 包含trace_id的上下文
// isLeader: 当前节点是否为领导者
func UpdateLeaderStatus(ctx context.Context, isLeader bool) {
	if isLeader {
		leaderStatusGauge.Set(1)
		utils.Log.Info(ctx, "当前角色更新为领导者")
	} else {
		leaderStatusGauge.Set(0)
		utils.Log.Info(ctx, "当前角色更新为跟随者")
	}
}