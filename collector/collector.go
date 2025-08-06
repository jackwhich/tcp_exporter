// Package collector 提供 TCP 队列指标的收集与解析逻辑，
// 实现 Prometheus Collector 接口，通过执行 Pod 内命令和读取 /proc 接口获取指标。
package collector

import (
	"context"
	"io"
	"os"
	"sort"
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



type podTask struct {
	deploymentName string
	namespace     string
	pod           *corev1.Pod
	containerName string
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
        tasks                []podTask
        descSyncSent         *prometheus.Desc
        descListenOverflows  *prometheus.Desc
        descListenDrops      *prometheus.Desc
        descMaxSomaxconn     *prometheus.Desc
        descCurrentEstablished *prometheus.Desc
}

func TCPQueueCollectorFactory(
        clientset *kubernetes.Clientset,
        restConfig *rest.Config,
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

func (collector *TCPQueueCollector) buildTasks(ctx context.Context, snap *snapshot.Snapshot) []podTask {
	total := 0
	for _, nsData := range snap.Namespaces {
		for _, pods := range nsData.Deployments {
			total += len(pods)
		}
	}

	tasks := make([]podTask, 0, total)

	for nsName, nsData := range snap.Namespaces {
		for depName, podNames := range nsData.Deployments {
			utils.Log.Debug(ctx, "处理 Deployment",
				zap.String("namespace", nsName),
				zap.String("deployment", depName),
				zap.Int("podCount", len(podNames)))
			utils.Log.Trace(ctx, "Deployment 详情",
				zap.Strings("pods", podNames))

			for _, podName := range podNames {
				pObj, err := collector.podLister.Pods(nsName).Get(podName)
				if err != nil {
					utils.Log.Warn(ctx, "无法获取 Pod 对象，跳过",
						zap.String("namespace", nsName),
						zap.String("pod", podName),
						zap.Error(err))
					continue
				}

				var containerName string
				containerCount := 0
				ignoredContainers := 0
				for _, cs := range pObj.Status.ContainerStatuses {
					if _, skip := collector.ignoreSet[cs.Name]; skip {
						utils.Log.Trace(ctx, "跳过忽略容器",
							zap.String("container", cs.Name))
						ignoredContainers++
						continue
					}
					if cs.State.Running != nil {
						containerName = cs.Name
						containerCount++
						utils.Log.Trace(ctx, "找到运行中容器",
							zap.String("container", cs.Name))
						break
					}
					utils.Log.Trace(ctx, "容器非运行状态",
						zap.String("container", cs.Name),
						zap.Any("state", cs.State))
				}

				if containerName == "" {
					utils.Log.Info(ctx, "无可用容器，跳过",
						zap.String("namespace", nsName),
						zap.String("pod", podName),
						zap.Int("totalContainers", len(pObj.Status.ContainerStatuses)),
						zap.Int("ignoredContainers", ignoredContainers),
						zap.Int("runningContainers", containerCount))
					continue
				}

				utils.Log.Debug(ctx, "为 Pod 选择容器",
					zap.String("namespace", nsName),
					zap.String("pod", podName),
					zap.String("container", containerName))

				tasks = append(tasks, podTask{
					deploymentName: depName,
					namespace:     nsName,
					pod:           pObj,
					containerName: containerName,
				})
			}
		}
	}

	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].namespace != tasks[j].namespace {
			return tasks[i].namespace < tasks[j].namespace
		}
		return tasks[i].pod.Name < tasks[j].pod.Name
	})

	return tasks
}

func (collector *TCPQueueCollector) collectSingleTask(ctx context.Context, t podTask, metricChan chan<- prometheus.Metric, cm *ConcurrencyManager) {
	utils.Log.Debug(ctx, "开始处理任务",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.pod.Name),
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

	ip := t.pod.Status.PodIP
	utils.Log.Debug(ctx, "采集指标",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.pod.Name),
		zap.String("container", t.containerName),
		zap.String("pod_ip", ip))

	execCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	req := collector.clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(t.pod.Name).
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
			zap.String("pod", t.pod.Name),
			zap.Error(err))
		return
	}
	var stderr strings.Builder
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		for i := 0; i < 2; i++ {
			utils.Log.Debug(ctx, "执行命令尝试",
				zap.String("pod", t.pod.Name),
				zap.String("namespace", t.namespace),
				zap.String("container", t.containerName),
				zap.Int("attempt", i+1))

			err := executor.StreamWithContext(execCtx, remotecommand.StreamOptions{
				Stdout: pipeWriter,
				Stderr: &stderr,
			})
			if err == nil {
				utils.Log.Debug(ctx, "命令执行成功",
					zap.String("pod", t.pod.Name),
					zap.String("namespace", t.namespace),
					zap.String("container", t.containerName))
				return
			}
			if strings.Contains(err.Error(), "dial tcp") {
				utils.Log.Warn(ctx, "执行命令网络超时，准备重试",
					zap.String("pod", t.pod.Name),
					zap.String("namespace", t.namespace),
					zap.String("container", t.containerName),
					zap.Error(err),
					zap.Int("attempt", i+1))
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			utils.Log.Error(ctx, "执行命令失败",
				zap.String("pod", t.pod.Name),
				zap.String("namespace", t.namespace),
				zap.String("container", t.containerName),
				zap.Error(err),
				zap.String("stderr", stderr.String()))
			return
		}
		utils.Log.Error(ctx, "命令执行重试次数用尽",
			zap.String("pod", t.pod.Name),
			zap.String("namespace", t.namespace),
			zap.String("container", t.containerName))
	}()
	streamParseAndReport(pipeReader, collector, metricChan, t.namespace, t.pod.Name, ip, t.containerName)
	utils.Log.Debug(ctx, "任务处理完成",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.pod.Name),
		zap.String("container", t.containerName))
}

func (collector *TCPQueueCollector) collectTasks(ctx context.Context, tasks []podTask, metricChan chan<- prometheus.Metric, cm *ConcurrencyManager) {
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(t podTask) {
			defer wg.Done()
			collector.collectSingleTask(ctx, t, metricChan, cm)
		}(t)
	}
	wg.Wait()
}

func (collector *TCPQueueCollector) Collect(metricChan chan<- prometheus.Metric) {
	// 为整个收集任务创建trace ID（使用自定义key类型）
	taskCtx := context.WithValue(context.Background(), utils.TraceIDKey, "collector-"+uuid.NewString())
	
	utils.Log.Info(taskCtx, "开始收集 TCP 队列指标")
	snap, err := snapshot.LoadSnapshot(taskCtx, collector.cacheFilePath)
	if err != nil {
		utils.Log.Error(taskCtx, "加载快照失败，跳过本次采集",
			zap.String("cacheFilePath", collector.cacheFilePath),
			zap.Error(err))
		return
	}
	namespaces := make([]string, 0, len(snap.Namespaces))
	for ns := range snap.Namespaces {
		namespaces = append(namespaces, ns)
	}
	utils.Log.Info(taskCtx, "成功加载快照",
		zap.String("cacheFilePath", collector.cacheFilePath),
		zap.String("namespaces", strings.Join(namespaces, ", ")))
	replicas, _ := strconv.Atoi(os.Getenv("REPLICA_COUNT"))
	podName := os.Getenv("POD_NAME")
	parts := strings.Split(podName, "-")
	ordinal := 0
	if idx, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
		ordinal = idx
	}
	totalPods := 0
	for _, nsData := range snap.Namespaces {
		for _, pods := range nsData.Deployments {
			totalPods += len(pods)
		}
	}
	cm := NewConcurrencyManager(totalPods, collector.maxConcurrent, collector.maxPodContainer)
	sm := &ShardingManager{Mode: ShardByDeployment}

	tasks := collector.buildTasks(taskCtx, snap)
	tasks = sm.ShardTasks(taskCtx, tasks, replicas, ordinal)
	collector.collectTasks(taskCtx, tasks, metricChan, cm)
}

func RegisterCollector(clientset *kubernetes.Clientset, restConfig *rest.Config, cfg *config.Config, factory informers.SharedInformerFactory) *TCPQueueCollector {
        deploymentLister := factory.Apps().V1().Deployments().Lister()
        podLister := factory.Core().V1().Pods().Lister()
        collector := TCPQueueCollectorFactory(
                clientset,
                restConfig,
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