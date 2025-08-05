package main

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

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
)

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
	logger               *zap.Logger
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

func (collector *TCPQueueCollector) Describe(descChan chan<- *prometheus.Desc) {
	descChan <- collector.descSyncSent
	descChan <- collector.descListenOverflows
	descChan <- collector.descListenDrops
	descChan <- collector.descMaxSomaxconn
	descChan <- collector.descCurrentEstablished
}

func (collector *TCPQueueCollector) buildTasks(snap *Snapshot) []podTask {
	total := 0
	for _, nsData := range snap.Namespaces {
		for _, pods := range nsData.Deployments {
			total += len(pods)
		}
	}

	tasks := make([]podTask, 0, total)

	for nsName, nsData := range snap.Namespaces {
		for depName, podNames := range nsData.Deployments {
			collector.logger.Debug("处理 Deployment",
				zap.String("namespace", nsName),
				zap.String("deployment", depName),
				zap.Int("podCount", len(podNames)))
			collector.logger.Debug("Deployment 详情",
				zap.Strings("pods", podNames))

			for _, podName := range podNames {
				pObj, err := collector.podLister.Pods(nsName).Get(podName)
				if err != nil {
					collector.logger.Warn("无法获取 Pod 对象，跳过",
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
						collector.logger.Debug("跳过忽略容器",
							zap.String("container", cs.Name))
						ignoredContainers++
						continue
					}
					if cs.State.Running != nil {
						containerName = cs.Name
						containerCount++
						collector.logger.Debug("找到运行中容器",
							zap.String("container", cs.Name))
						break
					}
					collector.logger.Debug("容器非运行状态",
						zap.String("container", cs.Name),
						zap.Any("state", cs.State))
				}

				if containerName == "" {
					collector.logger.Info("无可用容器，跳过",
						zap.String("namespace", nsName),
						zap.String("pod", podName),
						zap.Int("totalContainers", len(pObj.Status.ContainerStatuses)),
						zap.Int("ignoredContainers", ignoredContainers),
						zap.Int("runningContainers", containerCount))
					continue
				}

				collector.logger.Debug("为 Pod 选择容器",
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

func (collector *TCPQueueCollector) collectSingleTask(t podTask, metricChan chan<- prometheus.Metric, cm *ConcurrencyManager) {
	collector.logger.Debug("开始处理任务",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.pod.Name),
		zap.String("container", t.containerName))

	// 并发控制日志
	collector.logger.Debug("获取全局并发槽位")
	cm.AcquireGlobal()
	defer func() {
		collector.logger.Debug("释放全局并发槽位")
		cm.ReleaseGlobal()
	}()

	collector.logger.Debug("获取部署级并发槽位",
		zap.String("deployment", t.deploymentName),
		zap.Int("maxPodContainer", collector.maxPodContainer))
	cm.AcquireDep(t.namespace, t.deploymentName, collector.maxPodContainer)
	defer func() {
		collector.logger.Debug("释放部署级并发槽位",
			zap.String("deployment", t.deploymentName))
		cm.ReleaseDep(t.namespace, t.deploymentName)
	}()

	ip := t.pod.Status.PodIP
	collector.logger.Debug("采集指标",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.pod.Name),
		zap.String("container", t.containerName),
		zap.String("pod_ip", ip))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	req := collector.clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(t.pod.Name).
		Namespace(t.namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: t.containerName,
			Command:   getTCPMetricsCommand(),
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	req.Timeout(1 * time.Second)
	executor, err := remotecommand.NewSPDYExecutor(collector.restConfig, "POST", req.URL())
	if err != nil {
		collector.logger.Error("创建远程执行器失败",
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
			collector.logger.Debug("执行命令尝试",
				zap.String("pod", t.pod.Name),
				zap.String("namespace", t.namespace),
				zap.String("container", t.containerName),
				zap.Int("attempt", i+1))
				
			err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
				Stdout: pipeWriter,
				Stderr: &stderr,
			})
			if err == nil {
				collector.logger.Debug("命令执行成功",
					zap.String("pod", t.pod.Name),
					zap.String("namespace", t.namespace),
					zap.String("container", t.containerName))
				return
			}
			if strings.Contains(err.Error(), "dial tcp") {
				collector.logger.Warn("执行命令网络超时，准备重试",
					zap.String("pod", t.pod.Name),
					zap.String("namespace", t.namespace),
					zap.String("container", t.containerName),
					zap.Error(err),
					zap.Int("attempt", i+1))
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			collector.logger.Error("执行命令失败",
				zap.String("pod", t.pod.Name),
				zap.String("namespace", t.namespace),
				zap.String("container", t.containerName),
				zap.Error(err),
				zap.String("stderr", stderr.String()))
			return
		}
		collector.logger.Error("命令执行重试次数用尽",
			zap.String("pod", t.pod.Name),
			zap.String("namespace", t.namespace),
			zap.String("container", t.containerName))
	}()
	streamParseAndReport(pipeReader, collector, metricChan, t.namespace, t.pod.Name, ip, t.containerName)
	collector.logger.Debug("任务处理完成",
		zap.String("namespace", t.namespace),
		zap.String("pod", t.pod.Name),
		zap.String("container", t.containerName))
}

func (collector *TCPQueueCollector) collectTasks(tasks []podTask, metricChan chan<- prometheus.Metric, cm *ConcurrencyManager) {
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(t podTask) {
			defer wg.Done()
			collector.collectSingleTask(t, metricChan, cm)
		}(t)
	}
	wg.Wait()
}

func (collector *TCPQueueCollector) Collect(metricChan chan<- prometheus.Metric) {
	collector.logger.Info("开始收集 TCP 队列指标")
	snap, err := LoadSnapshot(collector.cacheFilePath, collector.logger)
	if err != nil {
		collector.logger.Error("加载快照失败，跳过本次采集",
			zap.String("cacheFilePath", collector.cacheFilePath),
			zap.Error(err))
		return
	}

	namespaces := make([]string, 0, len(snap.Namespaces))
	for ns := range snap.Namespaces {
		namespaces = append(namespaces, ns)
	}
	collector.logger.Info("成功加载快照",
		zap.String("cacheFilePath", collector.cacheFilePath),
		zap.String("namespaces", strings.Join(namespaces, ", ")))
	
	// 构建所有任务
	tasks := collector.buildTasks(snap)
	
	collector.logger.Info("任务分配",
		zap.Int("totalTasks", len(tasks)))
		
	totalPods := len(tasks)
	cm := NewConcurrencyManager(totalPods, collector.maxConcurrent, collector.maxPodContainer)
	collector.collectTasks(tasks, metricChan, cm)
}

func registerCollector(clientset *kubernetes.Clientset, restConfig *rest.Config, cfg *Config, factory informers.SharedInformerFactory, logger *zap.Logger) *TCPQueueCollector {
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
	collector.logger = logger
	prometheus.MustRegister(collector)
	return collector
}