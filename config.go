package main

import (
        "flag"
        "fmt"
        "os"

        "github.com/fsnotify/fsnotify"
        "go.uber.org/zap"
        "gopkg.in/yaml.v2"
)


// K8sConfig 包含Kubernetes相关的配置项
type K8sConfig struct {
        UseInCluster     bool     `yaml:"use_in_cluster"`      // 是否使用集群内配置
        KubeConfigPath   string   `yaml:"kube_config_path"`    // kubeconfig文件路径
        TargetNamespaces []string `yaml:"target_namespaces"`   // 目标命名空间列表
        IgnoreContainers []string `yaml:"ignore_containers"`   // 忽略的容器列表
        MaxConcurrent    int      `yaml:"max_concurrent"`      // 最大全局并发数
        MaxPodContainer  int      `yaml:"max_pod_container"`   // 每个Pod的最大容器并发数
        CacheFilePath    string   `yaml:"cache_file_path"`     // 缓存文件路径
}

// Config 表示应用程序的整体配置
type Config struct {
        Kubernetes K8sConfig `yaml:"kubernetes"` // Kubernetes相关配置
        Server     struct {
                Port     string `yaml:"port"`      // 服务监听端口
                LogLevel string `yaml:"log_level"` // 日志级别
                GinMode  string `yaml:"gin_mode"`  // Gin运行模式
        } `yaml:"server"` // 服务相关配置
}

func loadConfig(path string) (*Config, error) {
        data, err := os.ReadFile(path)
        if err != nil {
                return nil, fmt.Errorf("读取配置文件失败: %w", err)
        }

        var cfg Config
        if err := yaml.Unmarshal(data, &cfg); err != nil {
                return nil, fmt.Errorf("解析 YAML 失败: %w", err)
        }

        return &cfg, nil
}

func mustLoadConfig() (*Config, string) {
        configPath := flag.String("config", "config.yaml", "配置文件路径")
        flag.Parse()
        fmt.Fprintf(os.Stdout, "加载配置文件: %s\n", *configPath)
        cfg, err := loadConfig(*configPath)
        if err != nil {
                fmt.Fprintf(os.Stderr, "读取配置文件失败 [%s]: %v\n", CodeLoadConfigFailure, err)
                os.Exit(1)
        }
        fmt.Fprintf(os.Stdout, "配置文件加载成功: %s\n", *configPath)
        return cfg, *configPath
}

func watchConfig(path string, onChange func(*Config)) {
        logger.Info("启动配置文件监控", zap.String("path", path))
        watcher, err := fsnotify.NewWatcher()
        if err != nil {
                logger.Error("创建配置文件监控失败", zap.Error(err))
                return
        }
        defer watcher.Close()
        err = watcher.Add(path)
        if err != nil {
                logger.Error("添加配置文件监控失败", zap.String("path", path), zap.Error(err))
                return
        }
        logger.Info("配置文件监控已启动", zap.String("path", path))

        for {
                select {
                case ev, ok := <-watcher.Events:
                        if !ok {
                                logger.Info("配置文件监控通道已关闭")
                                return
                        }
                        logger.Trace("配置文件事件",
                                zap.String("name", ev.Name),
                                zap.String("op", ev.Op.String()))

                        if ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
                                logger.Info("配置文件已修改，重新加载",
                                        zap.String("path", path),
                                        zap.String("event", ev.Op.String()))
                                cfg, err := loadConfig(path)
                                if err != nil {
                                        logger.Error("重新加载配置失败",
                                                zap.String("path", path),
                                                zap.Error(err))
                                        continue
                                }
                                onChange(cfg)
                                logger.Info("配置重载成功", zap.String("path", path))
                        }
                case err, ok := <-watcher.Errors:
                        if !ok {
                                logger.Info("配置文件错误通道已关闭")
                                return
                        }
                        logger.Error("文件监控错误",
                                zap.String("path", path),
                                zap.Error(err))
                }
        }
}