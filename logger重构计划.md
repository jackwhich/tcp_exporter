# Logger重构与Trace ID实现计划

## 目标
1. 创建全局logger实例，减少引用传递
2. 为所有日志添加trace ID支持
3. 统一HTTP请求和后台任务的日志追踪

## 目录结构调整
- 移动 `logger.go` → `utils/logger.go`

## 代码修改

### utils/logger.go
```go
package utils

import (
    "context"
    "fmt"
    "os"
    "strings"

    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

var (
    Log *Logger // 全局logger实例
    logLevel = zap.NewAtomicLevel()
)

// InitLogger 初始化全局logger
func InitLogger(cfg *Config) {
    updateLogLevel(cfg.Server.LogLevel)
    
    zapConfig := zap.NewProductionConfig()
    zapConfig.Level = logLevel
    // ...保持原有配置
    
    log, err := zapConfig.Build(zap.AddCaller())
    if err != nil {
        fmt.Fprintf(os.Stderr, "初始化logger失败: %v\n", err)
        os.Exit(1)
    }
    Log = &Logger{log}
}

// 添加trace ID支持
func getTraceID(ctx context.Context) string {
    if traceID, ok := ctx.Value("traceID").(string); ok {
        return traceID
    }
    return ""
}

// 修改所有日志方法
func (l *Logger) Info(ctx context.Context, msg string, fields ...zap.Field) {
    fields = append(fields, zap.String("trace_id", getTraceID(ctx)))
    l.Logger.Info(msg, fields...)
}

// 其他日志方法(Trace, Debug, Error等)同样修改...
```

### main.go
```go
package main

import (
    "utils" // 导入utils包
    // ...其他导入
)

func main() {
    cfg, configPath := mustLoadConfig()
    
    // 初始化全局logger
    utils.InitLogger(cfg)
    
    defer func() {
        if r := recover(); r != nil {
            utils.Log.Error(context.Background(), "程序崩溃",
                zap.Any("reason", r),
                zap.String("stack", string(debug.Stack())))
            os.Exit(1)
        }
    }()
    
    // 更新日志调用
    utils.Log.Info(context.Background(), "启动TCP队列指标导出器")
    // ...其他代码
}
```

### http_server.go
```go
// 添加中间件
func traceIDMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        traceID := uuid.New().String() // 需要导入"github.com/google/uuid"
        ctx := context.WithValue(r.Context(), "traceID", traceID)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}

// 应用中间件
func runHTTPServer(cfg *Config) {
    http.Handle("/metrics", traceIDMiddleware(promhttp.Handler()))
    // ...其他路由
}
```

### 后台任务(如deployment_snapshot.go)
```go
func (m *SnapshotManager) RunWatcher(factory informers.SharedInformerFactory) {
    // 创建带trace ID的context
    taskCtx := context.WithValue(context.Background(), "traceID", "task-"+uuid.NewString())
    
    // 使用带trace ID的context记录日志
    utils.Log.Info(taskCtx, "启动快照管理器监听")
    // ...其他代码
}
```

## 实施步骤
1. 创建utils目录（如果不存在）
2. 移动logger.go到utils目录
3. 修改utils/logger.go实现全局logger和trace ID
4. 更新main.go使用全局logger
5. 在http_server.go添加中间件
6. 更新所有日志调用点添加context参数
7. 为后台任务添加trace ID

## 验证测试
1. 启动应用，检查全局logger初始化
2. 访问/metrics端点，检查日志中的trace ID
3. 验证后台任务的trace ID格式
4. 测试日志级别动态更新功能