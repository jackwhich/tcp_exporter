// Package config 提供应用配置的加载、解析及热重载功能
// 包含 LoadConfig、MustLoadConfig 和 WatchConfig 方法
package config

import (
        "context"
        "flag"
        "os"

        "github.com/fsnotify/fsnotify"
        "go.uber.org/zap"
        "gopkg.in/yaml.v2"
        "tcp-exporter/utils"
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

// GetLogLevel 实现utils.LoggerConfig接口
func (c *Config) GetLogLevel() string {
	return c.Server.LogLevel
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		zap.L().Fatal("读取配置文件失败", zap.Error(err))
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		zap.L().Fatal("解析 YAML 失败", zap.Error(err))
	}

        return &cfg, nil
}

func MustLoadConfig() (*Config, string) {
        configPath := flag.String("config", "config/config.yaml", "配置文件路径")
        flag.Parse()
        zap.L().Info("加载配置文件", zap.String("path", *configPath))
        cfg, err := LoadConfig(*configPath)
        if err != nil {
                zap.L().Fatal("读取配置文件失败", zap.Error(err))
        }
        zap.L().Info("配置文件加载成功", zap.String("path", *configPath))
        return cfg, *configPath
}


func WatchConfig(path string, onChange func(*Config)) {
	ctx := context.Background()
	utils.Log.Info(ctx, "启动配置文件监控", zap.String("path", path))
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		utils.Log.Error(ctx, "创建配置文件监控失败", zap.Error(err))
		return
	}
	defer watcher.Close()
	err = watcher.Add(path)
	if err != nil {
		utils.Log.Error(ctx, "添加配置文件监控失败", zap.String("path", path), zap.Error(err))
		return
	}
	utils.Log.Info(ctx, "配置文件监控已启动", zap.String("path", path))

	for {
		select {
		case ev, ok := <-watcher.Events:
			if !ok {
				utils.Log.Info(ctx, "配置文件监控通道已关闭")
				return
			}
			utils.Log.Trace(ctx, "配置文件事件",
				zap.String("name", ev.Name),
				zap.String("op", ev.Op.String()))

			if ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				utils.Log.Info(ctx, "配置文件已修改，重新加载",
					zap.String("path", path),
					zap.String("event", ev.Op.String()))
				cfg, err := LoadConfig(path)
				if err != nil {
					utils.Log.Error(ctx, "重新加载配置失败",
						zap.String("path", path),
						zap.Error(err))
					continue
				}
				onChange(cfg)
				utils.Log.Info(ctx, "配置重载成功", zap.String("path", path))
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				utils.Log.Info(ctx, "配置文件错误通道已关闭")
				return
			}
			utils.Log.Error(ctx, "文件监控错误",
				zap.String("path", path),
				zap.Error(err))
		}
	}
}