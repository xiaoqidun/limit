// Copyright 2025-2026 肖其顿 (XIAO QI DUN)
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

package limit

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// TestLimiter 覆盖了 Limiter 的主要功能。
func TestLimiter(t *testing.T) {
	// 子测试：验证基本的允许/拒绝逻辑
	t.Run("基本功能测试", func(t *testing.T) {
		limiter := New()
		defer limiter.Stop()
		key := "测试键"
		// 创建一个每秒2个令牌，桶容量为1的限制器
		rl := limiter.Get(key, rate.Limit(2), 1)
		if rl == nil {
			t.Fatal("limiter.Get() 意外返回 nil，测试无法继续")
		}
		if !rl.Allow() {
			t.Error("rl.Allow(): 首次调用应返回 true, 实际为 false")
		}
		if rl.Allow() {
			t.Error("rl.Allow(): 超出突发容量的调用应返回 false, 实际为 true")
		}
		time.Sleep(500 * time.Millisecond)
		if !rl.Allow() {
			t.Error("rl.Allow(): 令牌补充后的调用应返回 true, 实际为 false")
		}
	})

	// 子测试：验证 Del 方法的功能
	t.Run("删除功能测试", func(t *testing.T) {
		limiter := New()
		defer limiter.Stop()
		key := "测试键"
		rl1 := limiter.Get(key, rate.Limit(2), 1)
		if !rl1.Allow() {
			t.Fatal("获取限制器后的首次 Allow() 调用失败")
		}
		limiter.Del(key)
		rl2 := limiter.Get(key, rate.Limit(2), 1)
		if !rl2.Allow() {
			t.Error("Del() 后重新获取的限制器未能允许请求")
		}
	})

	// 子测试：验证 Stop 方法的功能
	t.Run("停止功能测试", func(t *testing.T) {
		limiter := New()
		limiter.Stop()
		if rl := limiter.Get("任意键", 1, 1); rl != nil {
			t.Error("Stop() 后 Get() 应返回 nil, 实际返回了有效实例")
		}
		// 多次调用 Stop 不应引发 panic
		limiter.Stop()
	})

	// 子测试：验证并发安全性
	t.Run("并发安全测试", func(t *testing.T) {
		limiter := New()
		defer limiter.Stop()
		var wg sync.WaitGroup
		numGoroutines := 100
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("并发测试键-%d", i)
				if limiter.Get(key, rate.Limit(10), 5) == nil {
					t.Errorf("并发获取键 '%s' 时, Get() 意外返回 nil", key)
				}
			}(i)
		}
		wg.Wait()
	})
}
