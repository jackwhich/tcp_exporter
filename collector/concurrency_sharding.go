// Package collector 提供并发信号量管理功能，
// 支持全局与部署级的并发限流和分片调度
package collector

import (
	"context"
	"hash/fnv"
	"sync"

	"tcp-exporter/utils"
	"go.uber.org/zap"
)

const bucketCount = 16

type ConcurrencyManager struct {
        globalSem    chan struct{}
        buckets      [bucketCount]chan struct{}
        bucketLocks  [bucketCount]sync.Mutex
        bucketCounts [bucketCount]int
}

func NewConcurrencyManager(maxGlobal, maxPerDep int) *ConcurrencyManager {
        return &ConcurrencyManager{
                globalSem: make(chan struct{}, maxGlobal),
        }
}

type concurrencySlot struct {
        sem       chan struct{}
        slotType  string
        namespace string
        deployment string
        capacity  int
}

func (cm *ConcurrencyManager) acquire(ctx context.Context, slot concurrencySlot) {
	slot.sem <- struct{}{}
	if slot.slotType == "deployment" {
		utils.Log.Debug(ctx, "获取并发槽",
			zap.String("类型", slot.slotType),
			zap.String("命名空间", slot.namespace),
			zap.String("部署名", slot.deployment),
			zap.Int("已占用", len(slot.sem)),
			zap.Int("总容量", slot.capacity),
		)
	} else {
		utils.Log.Debug(ctx, "获取并发槽",
			zap.String("类型", slot.slotType),
			zap.String("名称", slot.slotType),
			zap.Int("已占用", len(slot.sem)),
			zap.Int("总容量", slot.capacity),
		)
	}
}

func (cm *ConcurrencyManager) release(ctx context.Context, slot concurrencySlot) {
	<-slot.sem
	if slot.slotType == "deployment" {
		utils.Log.Debug(ctx, "释放并发槽",
			zap.String("类型", slot.slotType),
			zap.String("命名空间", slot.namespace),
			zap.String("部署名", slot.deployment),
			zap.Int("已占用", len(slot.sem)),
			zap.Int("总容量", slot.capacity),
		)
	} else {
		utils.Log.Debug(ctx, "释放并发槽",
			zap.String("类型", slot.slotType),
			zap.String("名称", slot.slotType),
			zap.Int("已占用", len(slot.sem)),
			zap.Int("总容量", slot.capacity),
		)
	}
}

func (cm *ConcurrencyManager) AcquireGlobal(ctx context.Context) {
	cm.acquire(ctx, concurrencySlot{
		sem:      cm.globalSem,
		slotType: "global",
		capacity: cap(cm.globalSem),
	})
}

func (cm *ConcurrencyManager) ReleaseGlobal(ctx context.Context) {
	used := len(cm.globalSem)
	capacity := cap(cm.globalSem)

	cm.release(ctx, concurrencySlot{
		sem:      cm.globalSem,
		slotType: "global",
		capacity: capacity,
	})

	utils.Log.Debug(ctx, "已释放全局信号量",
		zap.Int("used", used),
		zap.Int("capacity", capacity))
}

func (cm *ConcurrencyManager) AcquireDep(ctx context.Context, namespace, deployment string, limit int) {
	// 计算部署的桶索引
	h := fnv.New32a()
	h.Write([]byte(namespace + "/" + deployment))
	bucketIdx := h.Sum32() % bucketCount

	// 获取桶锁
	cm.bucketLocks[bucketIdx].Lock()
	defer cm.bucketLocks[bucketIdx].Unlock()

	// 如果桶未初始化则创建
	if cm.buckets[bucketIdx] == nil {
		cm.buckets[bucketIdx] = make(chan struct{}, limit)
		cm.bucketCounts[bucketIdx] = 0
	}

	// 桶计数器增加
	cm.bucketCounts[bucketIdx]++

	// 获取信号量
	cm.buckets[bucketIdx] <- struct{}{}
	
	utils.Log.Debug(ctx, "获取部署信号量",
		zap.String("namespace", namespace),
		zap.String("deployment", deployment),
		zap.Uint32("bucket", bucketIdx),
		zap.Int("usage", len(cm.buckets[bucketIdx])),
		zap.Int("capacity", cap(cm.buckets[bucketIdx])))
}

func (cm *ConcurrencyManager) ReleaseDep(ctx context.Context, namespace, deployment string) {
	// 计算部署的桶索引
	h := fnv.New32a()
	h.Write([]byte(namespace + "/" + deployment))
	bucketIdx := h.Sum32() % bucketCount

	// 获取桶锁
	cm.bucketLocks[bucketIdx].Lock()
	defer cm.bucketLocks[bucketIdx].Unlock()

	if cm.buckets[bucketIdx] == nil {
		return
	}

	// 释放信号量
	<-cm.buckets[bucketIdx]
	
	// 桶计数器减少
	cm.bucketCounts[bucketIdx]--
	
	// 如果桶为空则重置
	if cm.bucketCounts[bucketIdx] == 0 {
		close(cm.buckets[bucketIdx])
		cm.buckets[bucketIdx] = nil
	}

	utils.Log.Debug(ctx, "释放部署信号量",
		zap.String("namespace", namespace),
		zap.String("deployment", deployment),
		zap.Uint32("bucket", bucketIdx),
		zap.Int("usage", len(cm.buckets[bucketIdx])),
		zap.Int("capacity", cap(cm.buckets[bucketIdx])))
}

type ShardingMode int

const (
        ShardByIndex ShardingMode = iota // 默认：按任务索引分片
        ShardByDeployment                // 按Deployment分片
)

type ShardingManager struct {
        Mode ShardingMode
}

// GetShardRange 计算当前副本的分片范围
// 参数:
//   totalItems: 总任务数
//   replicas: 副本总数
//   ordinal: 当前Pod序号 (0-indexed)
// 返回值:
//   start: 分片起始索引 (包含)
//   end: 分片结束索引 (不包含)
func (sm *ShardingManager) GetShardRange(totalItems, replicas, ordinal int) (start, end int) {
	// 单个副本或没有任务时处理所有任务
	if replicas <= 1 || totalItems == 0 {
		return 0, totalItems
	}
	
	// 基本分片大小
	baseSize := totalItems / replicas
	// 余数任务（需要分配到前面的分片）
	remainder := totalItems % replicas
	
	// 计算起始位置
	start = ordinal * baseSize
	if ordinal < remainder {
		start += ordinal
	} else {
		start += remainder
	}
	
	// 计算结束位置
	end = start + baseSize
	if ordinal < remainder {
		end += 1
	}
	
	return
}

func (sm *ShardingManager) ShardTasks(ctx context.Context, tasks []podTask, replicas, ordinal int) []podTask {
	if replicas <= 0 {
		return tasks
	}

	switch sm.Mode {
	case ShardByDeployment:
		return sm.shardByDeployment(ctx, tasks, replicas, ordinal)
	default:
		return sm.shardByIndex(ctx, tasks, replicas, ordinal)
	}
}

// 按任务索引分片（原有逻辑）
func (sm *ShardingManager) shardByIndex(ctx context.Context, tasks []podTask, replicas, ordinal int) []podTask {
	capHint := len(tasks)/replicas + 1
	result := make([]podTask, 0, capHint)
	for i, t := range tasks {
		if i%replicas == ordinal {
			result = append(result, t)
		}
	}

	if len(result) > 0 {
		utils.Log.Info(ctx, "索引分片完成",
			zap.Int("总任务数", len(tasks)),
			zap.Int("副本数", replicas),
			zap.Int("当前副本序号", ordinal),
			zap.Int("分配到的任务数", len(result)),
			zap.String("命名空间示例", result[0].namespace),
			zap.String("Pod示例", result[0].podName),
		)
	} else {
		utils.Log.Info(ctx, "索引分片完成，但未分配到任何任务",
			zap.Int("总任务数", len(tasks)),
			zap.Int("副本数", replicas),
			zap.Int("当前副本序号", ordinal),
		)
	}
	return result
}

// 按Deployment哈希分片（优化版）
func (sm *ShardingManager) shardByDeployment(ctx context.Context, tasks []podTask, replicas, ordinal int) []podTask {
	// 缓存Deployment哈希值避免重复计算
	hashCache := make(map[string]int)
	result := make([]podTask, 0, len(tasks)/replicas)

	for _, task := range tasks {
		key := task.namespace + "/" + task.deploymentName

		// 获取或计算哈希值
		hashValue, exists := hashCache[key]
		if !exists {
			hash := fnv.New32a()
			hash.Write([]byte(key))
			hashValue = int(hash.Sum32())
			hashCache[key] = hashValue
			utils.Log.Debug(ctx, "计算Deployment哈希",
				zap.String("key", key),
				zap.Int("hash", hashValue))
		}

		// 确定当前副本是否负责此任务
		assigned := hashValue%replicas == ordinal
		if assigned {
			result = append(result, task)
		}

		utils.Log.Info(ctx, "任务分配决策",
			zap.String("namespace", task.namespace),
			zap.String("deployment", task.deploymentName),
			zap.String("pod", task.podName),
			zap.Int("hash", hashValue),
			zap.Int("replicas", replicas),
			zap.Int("ordinal", ordinal),
			zap.Bool("assigned", assigned))
	}

	utils.Log.Info(ctx, "Deployment分片完成",
		zap.Int("唯一Deployment数", len(hashCache)),
		zap.Int("总任务数", len(tasks)),
		zap.Int("分配到的任务数", len(result)),
		zap.Int("副本数", replicas),
		zap.Int("当前副本序号", ordinal),
	)

	return result
}
