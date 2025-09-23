# limit [![PkgGoDev](https://pkg.go.dev/badge/github.com/xiaoqidun/limit)](https://pkg.go.dev/github.com/xiaoqidun/limit)
一个高性能、并发安全的 Go 语言动态速率限制器

# 安装指南
```shell
go get -u github.com/xiaoqidun/limit
```

# 快速开始
```go
package main

import (
	"fmt"

	"github.com/xiaoqidun/limit"
	"golang.org/x/time/rate"
)

func main() {
	// 1. 创建一个新的 Limiter 实例
	limiter := limit.New()
	// 2. 确保在程序退出前优雅地停止后台任务，这非常重要
	defer limiter.Stop()
	// 3. 为任意键 "some-key" 获取一个速率限制器
	//    - rate.Limit(2): 表示速率为 "每秒2个请求"
	//    - 2: 表示桶的容量 (Burst)，允许瞬时处理2个请求
	rateLimiter := limiter.Get("some-key", rate.Limit(2), 2)
	// 4. 模拟3次连续的突发请求
	//    由于速率和容量都为2，只有前两次请求能立即成功
	for i := 0; i < 3; i++ {
		if rateLimiter.Allow() {
			fmt.Printf("请求 %d: 已允许\n", i+1)
		} else {
			fmt.Printf("请求 %d: 已拒绝\n", i+1)
		}
	}
}
```

# 授权协议
本项目使用 [Apache License 2.0](https://github.com/xiaoqidun/limit/blob/main/LICENSE) 授权协议