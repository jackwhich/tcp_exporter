package main

import corev1 "k8s.io/api/core/v1"

// NamespaceSnapshot 表示单个命名空间的快照
type NamespaceSnapshot struct {
	Deployments map[string][]string `json:"deployments"`
}

// Snapshot 表示整个集群的快照
type Snapshot struct {
	Namespaces map[string]NamespaceSnapshot `json:"namespaces"`
}

type podTask struct {
	deploymentName string
	namespace     string
	pod           *corev1.Pod
	containerName string
}


