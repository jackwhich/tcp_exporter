// Package cmd 提供TCP指标导出器的命令行接口，
// 包含构造执行 /proc/net/netstat、/proc/sys/net/core/somaxconn、/proc/net/snmp 查询的 shell 命令
package cmd

func GetTCPMetricsCommand() []string {
	return []string{"sh", "-c", `
echo "---TCP_EXT---"
grep '^TcpExt:' /proc/net/netstat
echo "---SYSCTL---"
echo -n "somaxconn: "; cat /proc/sys/net/core/somaxconn
echo "---SNMP---"
grep '^Tcp:' /proc/net/snmp
`}
}
