package collector

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// ConsistentHashSharder 实现一致性哈希分片
type ConsistentHashSharder struct {
	replicas int
	circle   map[uint32]int
	keys     []uint32
	sync.RWMutex
}

// NewConsistentHashSharder 创建新的分片器
func NewConsistentHashSharder(replicas int, nodes []int) *ConsistentHashSharder {
	ch := &ConsistentHashSharder{
		replicas: replicas,
		circle:   make(map[uint32]int),
	}
	
	// 确保至少有一个节点
	if len(nodes) == 0 {
		nodes = []int{0} // 默认节点
	}
	
	ch.AddNodes(nodes)
	return ch
}

// AddNodes 添加节点到哈希环
func (ch *ConsistentHashSharder) AddNodes(nodes []int) {
	ch.Lock()
	defer ch.Unlock()
	
	for _, node := range nodes {
		for i := 0; i < ch.replicas; i++ {
			virtualKey := strconv.Itoa(node) + "-" + strconv.Itoa(i)
			hash := ch.hashKey(virtualKey)
			ch.circle[hash] = node
			ch.keys = append(ch.keys, hash)
		}
	}
	sort.Slice(ch.keys, func(i, j int) bool { return ch.keys[i] < ch.keys[j] })
}

// RemoveNodes 从哈希环移除节点
func (ch *ConsistentHashSharder) RemoveNodes(nodes []int) {
	ch.Lock()
	defer ch.Unlock()
	
	for _, node := range nodes {
		for i := 0; i < ch.replicas; i++ {
			virtualKey := strconv.Itoa(node) + "-" + strconv.Itoa(i)
			hash := ch.hashKey(virtualKey)
			delete(ch.circle, hash)
			
			// 从keys中移除
			for idx, key := range ch.keys {
				if key == hash {
					ch.keys = append(ch.keys[:idx], ch.keys[idx+1:]...)
					break
				}
			}
		}
	}
}

// GetShard 获取键对应的分片
func (ch *ConsistentHashSharder) GetShard(key string) int {
	ch.RLock()
	defer ch.RUnlock()
	
	if len(ch.keys) == 0 {
		return -1
	}
	
	hash := ch.hashKey(key)
	idx := sort.Search(len(ch.keys), func(i int) bool { return ch.keys[i] >= hash })
	
	if idx >= len(ch.keys) {
		idx = 0
	}
	return ch.circle[ch.keys[idx]]
}

func (ch *ConsistentHashSharder) hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}