// Copyright 2025 肖其顿
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package limit 提供了一个高性能、并发安全的动态速率限制器。
// 它使用分片锁来减少高并发下的锁竞争，并能自动清理长期未使用的限制器。
package limit

import (
	"hash"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// defaultShardCount 是默认的分片数量，设为2的幂可以优化哈希计算。
const defaultShardCount = 32

// Config 定义了 Limiter 的可配置项。
type Config struct {
	// ShardCount 指定分片数量，必须是2的幂。如果为0或无效值，则使用默认值32。
	ShardCount int
	// GCInterval 指定GC周期，即检查并清理过期限制器的间隔。如果为0，则使用默认值10分钟。
	GCInterval time.Duration
	// Expiration 指定过期时间，即限制器在最后一次使用后能存活多久。如果为0，则使用默认值30分钟。
	Expiration time.Duration
}

// Limiter 是一个高性能、分片实现的动态速率限制器。
// 它的实例在并发使用时是安全的。
type Limiter struct {
	// 存储所有分片
	shards []*shard
	// 配置信息
	config Config
	// 标记限制器是否已停止
	stopped atomic.Bool
	// 确保Stop方法只执行一次
	stopOnce sync.Once
}

// New 使用默认配置创建一个新的 Limiter 实例。
func New() *Limiter {
	return NewWithConfig(Config{})
}

// NewWithConfig 根据提供的配置创建一个新的 Limiter 实例。
func NewWithConfig(config Config) *Limiter {
	// 如果未设置，则使用默认值
	if config.ShardCount == 0 {
		config.ShardCount = defaultShardCount
	}
	if config.GCInterval == 0 {
		config.GCInterval = 10 * time.Minute
	}
	if config.Expiration == 0 {
		config.Expiration = 30 * time.Minute
	}

	// 确保分片数量是2的幂，以便进行高效的位运算
	if config.ShardCount <= 0 || (config.ShardCount&(config.ShardCount-1)) != 0 {
		config.ShardCount = defaultShardCount
	}

	l := &Limiter{
		shards: make([]*shard, config.ShardCount),
		config: config,
	}

	// 初始化所有分片
	for i := 0; i < config.ShardCount; i++ {
		l.shards[i] = newShard(config.GCInterval, config.Expiration)
	}
	return l
}

// Get 获取或创建一个与指定键关联的速率限制器。
// 如果限制器已存在，它会根据传入的 r (速率) 和 b (并发数) 更新其配置。
// 如果 Limiter 实例已被 Stop 方法关闭，此方法将返回 nil。
func (l *Limiter) Get(k string, r rate.Limit, b int) *rate.Limiter {
	// 快速路径检查，避免在已停止时进行哈希和查找
	if l.stopped.Load() {
		return nil
	}
	// 定位到具体分片进行操作
	return l.getShard(k).get(k, r, b)
}

// Del 手动移除一个与指定键关联的速率限制器。
// 如果 Limiter 实例已被 Stop 方法关闭，此方法不执行任何操作。
func (l *Limiter) Del(k string) {
	// 快速路径检查
	if l.stopped.Load() {
		return
	}
	// 定位到具体分片进行操作
	l.getShard(k).del(k)
}

// Stop 停止 Limiter 的所有后台清理任务，并释放相关资源。
// 此方法对于并发调用是安全的，并且可以被多次调用。
func (l *Limiter) Stop() {
	l.stopOnce.Do(func() {
		l.stopped.Store(true)
		for _, s := range l.shards {
			s.stop()
		}
	})
}

// getShard 根据key的哈希值获取对应的分片。
func (l *Limiter) getShard(key string) *shard {
	hasher := fnvHasherPool.Get().(hash.Hash32)
	defer func() {
		hasher.Reset()
		fnvHasherPool.Put(hasher)
	}()
	_, _ = hasher.Write([]byte(key)) // FNV-1a never returns an error.
	// 使用位运算代替取模，提高效率
	return l.shards[hasher.Sum32()&(uint32(l.config.ShardCount)-1)]
}

// shard 代表 Limiter 的一个分片，它包含独立的锁和数据，以减少全局锁竞争。
type shard struct {
	mutex     sync.Mutex
	stopCh    chan struct{}
	limiter   map[string]*session
	stopOnce  sync.Once
	waitGroup sync.WaitGroup
}

// newShard 创建一个新的分片实例，并启动其gc任务。
func newShard(gcInterval, expiration time.Duration) *shard {
	s := &shard{
		// mutex 会被自动初始化为其零值（未锁定状态）
		stopCh:  make(chan struct{}),
		limiter: make(map[string]*session),
	}
	s.waitGroup.Add(1)
	go s.gc(gcInterval, expiration)
	return s
}

// gc 定期清理分片中过期的限制器。
func (s *shard) gc(interval, expiration time.Duration) {
	defer s.waitGroup.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		// 优先检查停止信号，确保能快速响应
		select {
		case <-s.stopCh:
			return
		default:
		}

		select {
		case <-ticker.C:
			s.mutex.Lock()
			// 再次检查分片是否已停止，防止在等待锁期间被停止
			if s.limiter == nil {
				s.mutex.Unlock()
				return
			}
			for k, v := range s.limiter {
				// 清理过期的限制器
				if time.Since(v.lastGet) > expiration {
					// 将 session 对象放回池中前，重置其状态
					v.limiter = nil
					v.lastGet = time.Time{}
					sessionPool.Put(v)
					delete(s.limiter, k)
				}
			}
			s.mutex.Unlock()
		case <-s.stopCh:
			// 收到停止信号，退出goroutine
			return
		}
	}
}

// get 获取或创建一个新的速率限制器，如果已存在则更新其配置。
func (s *shard) get(k string, r rate.Limit, b int) *rate.Limiter {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// 检查分片是否已停止
	if s.limiter == nil {
		return nil
	}
	sess, ok := s.limiter[k]
	if !ok {
		// 从池中获取 session 对象
		sess = sessionPool.Get().(*session)
		sess.limiter = rate.NewLimiter(r, b)
		s.limiter[k] = sess
	} else {
		// 如果已存在，则更新其速率和并发数
		sess.limiter.SetLimit(r)
		sess.limiter.SetBurst(b)
	}
	sess.lastGet = time.Now()
	return sess.limiter
}

// del 从分片中移除一个键的速率限制器。
func (s *shard) del(k string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// 检查分片是否已停止
	if s.limiter == nil {
		return
	}
	if sess, ok := s.limiter[k]; ok {
		// 将 session 对象放回池中前，重置其状态
		sess.limiter = nil
		sess.lastGet = time.Time{}
		sessionPool.Put(sess)
		delete(s.limiter, k)
	}
}

// stop 停止分片的gc任务，并同步等待其完成后再清理资源。
func (s *shard) stop() {
	// 使用 sync.Once 确保 channel 只被关闭一次，彻底避免并发风险
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	// 等待 gc goroutine 完全退出
	s.waitGroup.Wait()

	// 锁定并进行最终的资源清理
	// 因为 gc 已经退出，所以此时只有 Get/Del 会竞争锁
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 检查是否已被清理，防止重复操作
	if s.limiter == nil {
		return
	}

	// 将所有 session 对象放回对象池
	for _, sess := range s.limiter {
		sess.limiter = nil
		sess.lastGet = time.Time{}
		sessionPool.Put(sess)
	}
	// 清理map，释放内存，并作为停止标记
	s.limiter = nil
}

// session 存储每个键的速率限制器实例和最后访问时间。
type session struct {
	// 最后一次访问时间
	lastGet time.Time
	// 速率限制器
	limiter *rate.Limiter
}

// sessionPool 使用 sync.Pool 来复用 session 对象，以减少 GC 压力。
var sessionPool = sync.Pool{
	New: func() interface{} {
		return new(session)
	},
}

// fnvHasherPool 使用 sync.Pool 来复用 FNV-1a 哈希对象，以减少高并发下的内存分配。
var fnvHasherPool = sync.Pool{
	New: func() interface{} {
		return fnv.New32a()
	},
}
