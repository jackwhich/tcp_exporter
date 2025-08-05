package main

func getTCPMetricsCommand() []string {
	return []string{"sh", "-c", `
echo "---TCP_EXT---"
grep '^TcpExt:' /proc/net/netstat
echo "---SYSCTL---"
echo -n "somaxconn: "; cat /proc/sys/net/core/somaxconn
echo "---SNMP---"
grep '^Tcp:' /proc/net/snmp
`}
}
