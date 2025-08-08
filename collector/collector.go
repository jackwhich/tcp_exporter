// Package collector 提供 TCP 队列指标的收集与解析逻辑，
// 实现 Prometheus Collector 接口，通过执行 Pod 内命令和读取 /proc 接口获取指标。
package collector

import (
	"context"
	"hash/fnv"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/kubernetes/scheme"
	"tcp-exporter/config"
	"tcp-exporter/snapshot"
	"tcp-exporter/utils"
	"tcp-exporter/cmd"
)

// getOrdinalFromPodName 从Pod名称提取序号
func getOrdinalFromPodName(podName string) int {
	// 处理空名称
	if podName == "" {
		return 0
	}
	
	// 分割名称获取最后部分
	parts := strings.Split(podName, "-")
	lastPart := parts[len(parts)-1]
	
	// 尝试转换为整数
	ordinal, err := strconv.Atoi(lastPart)
	if err != nil {
		// 如果转换失败，使用哈希值作为回退
		h := fnv.New32a()
		h.Write([]byte(podName))
		return int(h.Sum32() % 1000)
	}
	return ordinal
}



type podTask struct {
	deploymentName string
	namespace     string
	podName       string
	containerName string
	podIP         string
}

func buildIgnoreSet(ignoreContainers []string) map[string]struct{} {
        ignores := make(map[string]struct{}, len(ignoreContainers))
        for _, n := range ignoreContainers {
                ignores[n] = struct{}{}
        }
        return ignores
}

type TCPQueueCollector struct {
        clientset            *kubernetes.Clientset
        restConfig           *rest.Config
        cacheFilePath        string
        maxConcurrent        int
        maxPodContainer      int
        ignoreSet            map[string]struct{}
        deploymentLister     appslisters.DeploymentLister
        podLister            corelisters.PodLister
        descSyncSent         *prometheus.Desc
        descListenOverflows  *prometheus.Desc
        descListenDrops      *prometheus.Desc
        descMaxSomaxconn     *prometheus.Desc
        descCurrentEstablished *prometheus.Desc
        
        // 轻量级缓存
        lastCheck    time.Time      // 最后检查时间
        lastSnapshot *snapshot.Snapshot // 上次快照
        
        // 添加配置引用
        cfg *config.Config
}

func TCPQueueCollectorFactory(
        clientset *kubernetes.Clientset,
        restConfig *rest.Config,
        cfg *config.Config, // 添加配置参数
        cacheFilePath string,
        maxConcurrent int,
        maxPodContainer int,
        ignoreSet map[string]struct{},
        deploymentLister appslisters.DeploymentLister,
        podLister corelisters.PodLister,
) *TCPQueueCollector {
        labels := []string{"namespace", "pod", "pod_ip", "container"}
        return &TCPQueueCollector{
                clientset:        clientset,
                restConfig:       restConfig,
                cfg:              cfg, // 初始化配置
                cacheFilePath:    cacheFilePath,
                maxConcurrent:    maxConcurrent,
                maxPodContainer:  maxPodContainer,
                ignoreSet:        ignoreSet,
                deploymentLister: deploymentLister,
                podLister:        podLister,
                descListenDrops: prometheus.NewDesc(
                        "tcp_listen_drops_total",
                        "Listen drops：握手完成后未 accept 被丢弃的连接数 (/proc/net/netstat)",
                        labels, nil,
                ),
                descCurrentEstablished: prometheus.NewDesc(
                        "tcp_current_established",
                        "当前已建立的TCP连接数（/proc/net/snmp: CurrEstab）",
                        labels, nil,
                ),
                descSyncSent: prometheus.NewDesc(
                        "tcp_syncookies_sent_total",
                        "SYN cookies sent：三次握手未完成时 backlog 满触发的 syncookie 次数 (/proc/net/netstat)",
                        labels, nil,
                ),
                descListenOverflows: prometheus.NewDesc(
                        "tcp_listen_overflows_total",
                        "Listen overflows：accept 队列溢出次数 (/proc/net/netstat)",
                        labels, nil,
                ),
                descMaxSomaxconn: prometheus.NewDesc(
                        "tcp_max_somaxconn",
                        "最大 Accept 队列长度 (/proc/sys/net/core/somaxconn)",
                        labels, nil,
                ),
        }
}

func (collector *TCPQueueCollector) SetIgnoreSet(ignoreContainers []string) {
        collector.ignoreSet = buildIgnoreSet(ignoreContainers)
}

func (collector *TCPQueueCollector) Describe(descChan chan<- *prometheus.Desc) {
        descChan <- collector.descSyncSent
        descChan <- collector.descListenOverflows
        descChan <- collector.descListenDrops
        descChan <- collector.descMaxSomaxconn
        descChan <- collector.descCurrentEstablished
}

var taskPool = sync.Pool{
	New: func() interface{} {
		return &podTask{}
	},
}

func putTask(task *podTask) {
	// 重置字段
	*task = podTask{}
	taskPool.Put(task)
}

func (collector *TCPQueueCollector) buildTasks(ctx context.Context, snap *snapshot.Snapshot) <-chan *podTask {
	taskChan := make(chan *podTask)
	go func() {
		defer close(taskChan)
		for nsName, nsData := range snap.Namespaces {
			for depName, podInfos := range nsData.Deployments {
				utils.Log.Debug(ctx, "处理 Deployment",
					zap.String("namespace", nsName),
					zap.String("deployment", depName),
					zap.Int("podCount", len(podInfos)))
				
				for _, podInfo := range podInfos {
					for _, container := range podInfo.Containers {
						if container.State == "running" {
							task := taskPool.Get().(*podTask)
							task.deploymentName = depName
							task.namespace = nsName
							task.podName = podInfo.PodName
							task.containerName = container.Name
							task.podIP = podInfo.IP
							taskChan <- task
						}
					}
				}
			}
		}
	}()
	return taskChan
}

func (collector *TCPQueueCollector) collectSingleTask(ctx context.Context, t *podTask, metricChan chan<- prometheus.Metric, cm *ConcurrencyManager) {
	defer putTask(t)
	utils.Log.Debug(ctx, "开始处理任务",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.podName),
		zap.String("container", t.containerName))

	// 并发控制日志
	utils.Log.Trace(ctx, "获取全局并发槽位")
	cm.AcquireGlobal(ctx)
	defer func() {
		utils.Log.Trace(ctx, "释放全局并发槽位")
		cm.ReleaseGlobal(ctx)
	}()

	utils.Log.Trace(ctx, "获取部署级并发槽位",
		zap.String("deployment", t.deploymentName),
		zap.Int("maxPodContainer", collector.maxPodContainer))
	cm.AcquireDep(ctx, t.namespace, t.deploymentName, collector.maxPodContainer)
	defer func() {
		utils.Log.Trace(ctx, "释放部署级并发槽位",
			zap.String("deployment", t.deploymentName))
		cm.ReleaseDep(ctx, t.namespace, t.deploymentName)
	}()

	utils.Log.Debug(ctx, "采集指标",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.podName),
		zap.String("container", t.containerName),
		zap.String("pod_ip", t.podIP))

	execCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	req := collector.clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(t.podName).
		Namespace(t.namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: t.containerName,
			Command:   cmd.GetTCPMetricsCommand(),
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	req.Timeout(1 * time.Second)
	executor, err := remotecommand.NewSPDYExecutor(collector.restConfig, "POST", req.URL())
	if err != nil {
		utils.Log.Error(ctx, "创建远程执行器失败",
			zap.String("namespace", t.namespace),
			zap.String("pod", t.podName),
			zap.Error(err))
		return
	}
	var stderr strings.Builder
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		for i := 0; i < 2; i++ {
			utils.Log.Debug(ctx, "执行命令尝试",
				zap.String("pod", t.podName),
				zap.String("namespace", t.namespace),
				zap.String("container", t.containerName),
				zap.Int("attempt", i+1))

			err := executor.StreamWithContext(execCtx, remotecommand.StreamOptions{
				Stdout: pipeWriter,
				Stderr: &stderr,
			})
			if err == nil {
				utils.Log.Debug(ctx, "命令执行成功",
					zap.String("pod", t.podName),
					zap.String("namespace", t.namespace),
					zap.String("container", t.containerName))
				return
			}
			if strings.Contains(err.Error(), "dial tcp") {
				utils.Log.Warn(ctx, "执行命令网络超时，准备重试",
					zap.String("pod", t.podName),
					zap.String("namespace", t.namespace),
					zap.String("container", t.containerName),
					zap.Error(err),
					zap.Int("attempt", i+1))
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			utils.Log.Error(ctx, "执行命令失败",
				zap.String("pod", t.podName),
				zap.String("namespace", t.namespace),
				zap.String("container", t.containerName),
				zap.Error(err),
				zap.String("stderr", stderr.String()))
			return
		}
		utils.Log.Error(ctx, "命令执行重试次数用尽",
			zap.String("pod", t.podName),
			zap.String("namespace", t.namespace),
			zap.String("container", t.containerName))
	}()
	streamParseAndReport(pipeReader, collector, metricChan, t.namespace, t.podName, t.podIP, t.containerName)
	utils.Log.Debug(ctx, "任务处理完成",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.podName),
		zap.String("container", t.containerName))
}


func (collector *TCPQueueCollector) Collect(metricChan chan<- prometheus.Metric) {
	// 为整个收集任务创建trace ID（使用自定义key类型）
	taskCtx := context.WithValue(context.Background(), utils.TraceIDKey, "collector-"+uuid.NewString())
	
	utils.Log.Info(taskCtx, "开始收集 TCP 队列指标")
	
	// 轻量级缓存检查
	var snap *snapshot.Snapshot
	fi, err := os.Stat(collector.cacheFilePath)
	if err != nil {
		utils.Log.Error(taskCtx, "无法访问快照文件",
			zap.String("cacheFilePath", collector.cacheFilePath),
			zap.Error(err))
		return
	}
	
	// 检查文件是否修改
	if fi.ModTime().After(collector.lastCheck) {
		utils.Log.Debug(taskCtx, "快照文件已更新，重新加载")
		snap, err = snapshot.LoadSnapshot(taskCtx, collector.cacheFilePath)
		if err != nil {
			utils.Log.Error(taskCtx, "加载快照失败，跳过本次采集",
				zap.String("cacheFilePath", collector.cacheFilePath),
				zap.Error(err))
			return
		}
		// 更新缓存
		collector.lastCheck = fi.ModTime()
		collector.lastSnapshot = snap
	} else {
		utils.Log.Debug(taskCtx, "复用缓存快照")
		snap = collector.lastSnapshot
	}
	namespaces := make([]string, 0, len(snap.Namespaces))
	for ns := range snap.Namespaces {
		namespaces = append(namespaces, ns)
	}
	utils.Log.Info(taskCtx, "成功加载快照",
		zap.String("cacheFilePath", collector.cacheFilePath),
		zap.String("namespaces", strings.Join(namespaces, ", ")))
	
	totalPods := 0
	for _, nsData := range snap.Namespaces {
		for _, pods := range nsData.Deployments {
			totalPods += len(pods)
		}
	}
	
	// 创建并发管理器（无需任务总数）
	cm := NewConcurrencyManager(collector.maxConcurrent, collector.maxPodContainer)
	
	// 获取任务通道
	taskChan := collector.buildTasks(taskCtx, snap)
	
	// 创建Worker Pool (完全动态调整)
	workerCount := collector.maxConcurrent
	if workerCount < 1 {
		// 计算总Pod数
		totalPods := 0
		for _, nsData := range snap.Namespaces {
			for _, pods := range nsData.Deployments {
				totalPods += len(pods)
			}
		}
		
		// 动态计算worker数量 (每20个Pod分配1个worker，上限200)
		workerCount = totalPods / 20
		if workerCount < 10 {
			workerCount = 10 // 最小10个worker
		} else if workerCount > 200 {
			workerCount = 200 // 最大200个worker
		}
	}
	workerPool := make(chan struct{}, workerCount)
	utils.Log.Info(taskCtx, "初始化动态Worker Pool",
		zap.Int("worker_count", workerCount),
		zap.Int("total_pods", totalPods),
		zap.Int("ratio", totalPods/workerCount))
	
	// 使用Worker Pool处理任务
	var wg sync.WaitGroup
	for task := range taskChan {
		// Kubernetes模式分片检查
		if collector.cfg.Baremetal.Mode != "standalone" {
			replicas, _ := strconv.Atoi(os.Getenv("REPLICA_COUNT"))
			podName := os.Getenv("POD_NAME")
			ordinal := getOrdinalFromPodName(podName)
			
			// 使用部署名称哈希分片
			hash := fnv.New32a()
			hash.Write([]byte(task.deploymentName))
			if int(hash.Sum32())%replicas != ordinal {
				continue
			}
		}
		
		// 获取worker
		workerPool <- struct{}{}
		wg.Add(1)
		
		go func(t *podTask) {
			defer func() {
				<-workerPool
				wg.Done()
			}()
			collector.collectSingleTask(taskCtx, t, metricChan, cm)
		}(task)
	}
	
	wg.Wait()
	close(workerPool)
}

func RegisterCollector(clientset *kubernetes.Clientset, restConfig *rest.Config, cfg *config.Config, factory informers.SharedInformerFactory) *TCPQueueCollector {
        deploymentLister := factory.Apps().V1().Deployments().Lister()
        podLister := factory.Core().V1().Pods().Lister()
        collector := TCPQueueCollectorFactory(
                clientset,
                restConfig,
                cfg, // 添加配置引用
                cfg.Kubernetes.CacheFilePath,
                cfg.Kubernetes.MaxConcurrent,
                cfg.Kubernetes.MaxPodContainer,
                buildIgnoreSet(cfg.Kubernetes.IgnoreContainers),
                deploymentLister,
                podLister,
        )
        prometheus.MustRegister(collector)
        return collector
}