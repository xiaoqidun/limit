// Copyright 2025 肖其顿 (XIAO QI DUN)
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

package limit_test

import (
	"fmt"
	"time"

	"github.com/xiaoqidun/limit"
	"golang.org/x/time/rate"
)

// ExampleLimiter 演示了 limit 包的基本用法。
func ExampleLimiter() {
	// 创建一个使用默认配置的 Limiter 实例
	limiter := limit.New()
	// 程序退出前，优雅地停止后台任务，这非常重要
	defer limiter.Stop()
	// 为一个特定的测试键获取一个速率限制器
	// 限制为每秒2个请求，最多允许3个并发（桶容量）
	testKey := "testKey"
	rateLimiter := limiter.Get(testKey, rate.Limit(2), 3)
	// 模拟连续的请求
	for i := 0; i < 5; i++ {
		if rateLimiter.Allow() {
			fmt.Printf("请求 %d: 已允许\n", i+1)
		} else {
			fmt.Printf("请求 %d: 已拒绝\n", i+1)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// 手动移除一个不再需要的限制器
	limiter.Del(testKey)
	// Output:
	// 请求 1: 已允许
	// 请求 2: 已允许
	// 请求 3: 已允许
	// 请求 4: 已拒绝
	// 请求 5: 已拒绝
}

// ExampleNewWithConfig 展示了如何使用自定义配置。
func ExampleNewWithConfig() {
	// 自定义配置
	config := limit.Config{
		ShardCount: 64,               // 分片数量，必须是2的幂
		GCInterval: 5 * time.Minute,  // GC 检查周期
		Expiration: 15 * time.Minute, // 限制器过期时间
	}
	// 使用自定义配置创建一个 Limiter 实例
	customLimiter := limit.NewWithConfig(config)
	defer customLimiter.Stop()
	fmt.Println("使用自定义配置的限制器已成功创建")
	// Output:
	// 使用自定义配置的限制器已成功创建
}
