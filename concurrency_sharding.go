package main

import (
	"hash/fnv"
	"sync"

	"go.uber.org/zap"
)

type ConcurrencyManager struct {
	globalSem      chan struct{}
	deploymentSems *sync.Map // 使用sync.Map实现无锁读取
}

func NewConcurrencyManager(totalTasks, maxGlobal, maxPerDep int) *ConcurrencyManager {
	return &ConcurrencyManager{
		globalSem:      make(chan struct{}, maxGlobal),
		deploymentSems: &sync.Map{},
	}
}

type concurrencySlot struct {
	sem       chan struct{}
	slotType  string
	namespace string
	deployment string
	capacity  int
}

func (cm *ConcurrencyManager) acquire(slot concurrencySlot) {
	slot.sem <- struct{}{}
	if slot.slotType == "deployment" {
		logger.Debug("获取并发槽",
			zap.String("类型", slot.slotType),
			zap.String("命名空间", slot.namespace),
			zap.String("部署名", slot.deployment),
			zap.Int("已占用", len(slot.sem)),
			zap.Int("总容量", slot.capacity),
		)
	} else {
		logger.Debug("获取并发槽",
			zap.String("类型", slot.slotType),
			zap.String("名称", slot.slotType),
			zap.Int("已占用", len(slot.sem)),
			zap.Int("总容量", slot.capacity),
		)
	}
}

func (cm *ConcurrencyManager) release(slot concurrencySlot) {
	<-slot.sem
	if slot.slotType == "deployment" {
		logger.Debug("释放并发槽",
			zap.String("类型", slot.slotType),
			zap.String("命名空间", slot.namespace),
			zap.String("部署名", slot.deployment),
			zap.Int("已占用", len(slot.sem)),
			zap.Int("总容量", slot.capacity),
		)
	} else {
		logger.Debug("释放并发槽",
			zap.String("类型", slot.slotType),
			zap.String("名称", slot.slotType),
			zap.Int("已占用", len(slot.sem)),
			zap.Int("总容量", slot.capacity),
		)
	}
}

func (cm *ConcurrencyManager) AcquireGlobal() {
	cm.acquire(concurrencySlot{
		sem:      cm.globalSem,
		slotType: "global",
		capacity: cap(cm.globalSem),
	})
}

func (cm *ConcurrencyManager) ReleaseGlobal() {
	used := len(cm.globalSem)
	capacity := cap(cm.globalSem)
	
	cm.release(concurrencySlot{
		sem:      cm.globalSem,
		slotType: "global",
		capacity: capacity,
	})
	
	logger.Info("已释放全局信号量",
		zap.Int("used", used),
		zap.Int("capacity", capacity))
}

func (cm *ConcurrencyManager) AcquireDep(namespace, deployment string, limit int) {
	key := namespace + "/" + deployment
	
	// 快速路径：尝试直接获取现有信号量
	if sem, ok := cm.deploymentSems.Load(key); ok {
		cm.acquire(concurrencySlot{
			sem:       sem.(chan struct{}),
			slotType:  "deployment",
			namespace: namespace,
			deployment: deployment,
			capacity:  cap(sem.(chan struct{})),
		})
		return
	}
	
	// 慢速路径：创建新信号量
	logger.Info("为部署创建新信号量",
		zap.String("命名空间", namespace),
		zap.String("部署名", deployment),
		zap.Int("limit", limit))
	newSem := make(chan struct{}, limit)
	sem, loaded := cm.deploymentSems.LoadOrStore(key, newSem)
	
	if loaded {
		// 其他goroutine已经创建，使用现有的
		logger.Debug("其他goroutine已创建信号量，关闭新创建信号量",
			zap.String("key", key))
		close(newSem) // 避免内存泄漏
	} else {
		logger.Info("成功创建新信号量",
			zap.String("命名空间", namespace),
			zap.String("部署名", deployment),
			zap.Int("capacity", cap(newSem)))
	}
	
	cm.acquire(concurrencySlot{
		sem:       sem.(chan struct{}),
		slotType:  "deployment",
		namespace: namespace,
		deployment: deployment,
		capacity:  cap(sem.(chan struct{})),
	})
}

func (cm *ConcurrencyManager) ReleaseDep(namespace, deployment string) {
	key := namespace + "/" + deployment
	
	if sem, ok := cm.deploymentSems.Load(key); ok {
		cm.release(concurrencySlot{
			sem:       sem.(chan struct{}),
			slotType:  "deployment",
			namespace: namespace,
			deployment: deployment,
			capacity:  cap(sem.(chan struct{})),
		})
		semChan := sem.(chan struct{})
		used := len(semChan)
		capacity := cap(semChan)
		
		logger.Info("已释放部署信号量",
			zap.String("命名空间", namespace),
			zap.String("部署名", deployment),
			zap.Int("used", used),
			zap.Int("capacity", capacity))
	}
}

type ShardingMode int

const (
	ShardByIndex ShardingMode = iota // 默认：按任务索引分片
	ShardByDeployment                // 按Deployment分片
)

type ShardingManager struct {
	Mode ShardingMode
}

func (sm *ShardingManager) ShardTasks(tasks []podTask, replicas, ordinal int) []podTask {
	if replicas <= 0 {
		return tasks
	}

	switch sm.Mode {
	case ShardByDeployment:
		return sm.shardByDeployment(tasks, replicas, ordinal)
	default:
		return sm.shardByIndex(tasks, replicas, ordinal)
	}
}

// 按任务索引分片（原有逻辑）
func (sm *ShardingManager) shardByIndex(tasks []podTask, replicas, ordinal int) []podTask {
	capHint := len(tasks)/replicas + 1
	result := make([]podTask, 0, capHint)
	for i, t := range tasks {
		if i%replicas == ordinal {
			result = append(result, t)
		}
	}

	if len(result) > 0 {
		logger.Info("索引分片完成",
			zap.Int("总任务数", len(tasks)),
			zap.Int("副本数", replicas),
			zap.Int("当前副本序号", ordinal),
			zap.Int("分配到的任务数", len(result)),
			zap.String("命名空间示例", result[0].namespace),
			zap.String("Pod示例", result[0].pod.Name),
		)
	} else {
		logger.Info("索引分片完成，但未分配到任何任务",
			zap.Int("总任务数", len(tasks)),
			zap.Int("副本数", replicas),
			zap.Int("当前副本序号", ordinal),
		)
	}
	return result
}

// 按Pod名称哈希分片（确保每个Pod只被一个exporter实例采集）
func (sm *ShardingManager) shardByDeployment(tasks []podTask, replicas, ordinal int) []podTask {
	// 缓存Pod哈希值避免重复计算
	hashCache := make(map[string]int)
	result := make([]podTask, 0, len(tasks)/replicas)
	
	for _, task := range tasks {
		key := task.namespace + "/" + task.pod.Name
		
		// 获取或计算哈希值
		hashValue, exists := hashCache[key]
		if !exists {
			hash := fnv.New32a()
			hash.Write([]byte(key))
			hashValue = int(hash.Sum32())
			hashCache[key] = hashValue
			logger.Debug("计算Pod哈希",
				zap.String("key", key),
				zap.Int("hash", hashValue))
		}
		
		// 确定当前副本是否负责此任务
		assigned := hashValue%replicas == ordinal
		if assigned {
			result = append(result, task)
		}
		
		logger.Debug("任务分配决策",
			zap.String("namespace", task.namespace),
			zap.String("pod", task.pod.Name),
			zap.Int("hash", hashValue),
			zap.Int("replicas", replicas),
			zap.Int("ordinal", ordinal),
			zap.Bool("assigned", assigned))
	}

	logger.Info("Pod分片完成",
		zap.Int("唯一Pod数", len(hashCache)),
		zap.Int("总任务数", len(tasks)),
		zap.Int("分配到的任务数", len(result)),
		zap.Int("副本数", replicas),
		zap.Int("当前副本序号", ordinal),
	)

	return result
}
