package main

const (
    CodeInitLoggerFailure      = "INIT_LOGGER_FAILURE"       // 初始化日志失败错误代码
    CodeLoadConfigFailure      = "LOAD_CONFIG_FAILURE"       // 加载配置失败错误代码
    CodeBuildClientFailure     = "BUILD_CLIENTSET_FAILURE"   // 构建 Kubernetes 客户端失败错误代码
    CodeHTTPServerStartFailure = "HTTP_SERVER_START_FAILURE" // HTTP 服务启动失败错误代码
)