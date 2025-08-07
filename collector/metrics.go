// Package collector 提供 TCP 扩展与基础指标的解析与上报功能
// 包含处理 TcpExt、somaxconn 和 SNMP Tcp 行的逻辑，以及 streamParseAndReport
package collector

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strconv"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"tcp-exporter/utils"
	"go.uber.org/zap"
)

// 定义指标键常量
const (
        KeySyncookiesSent    = "SyncookiesSent"    // SYN cookie发送数量
        KeyListenOverflows   = "ListenOverflows"   // 监听队列溢出次数
        KeyListenDrops       = "ListenDrops"       // 监听队列丢弃次数
        KeySomaxconn         = "somaxconn"         // 最大监听队列长度
        KeyCurrEstab         = "CurrEstab"         // 当前已建立连接数
        KeyTCPExt            = "TcpExt:"           // TCP扩展指标前缀
        KeyTCPExtData        = "TcpExt"            // TCP扩展数据行前缀
        KeyTCP               = "Tcp:"              // TCP基础指标前缀
)

// 解析状态封装
type parseState struct {
        tcpExtKeys        []string   // TCP扩展指标键列表
        snmpHeaders       []string   // SNMP表头
        somaxconnValue    float64    // somaxconn值
        syncookiesSent    float64    // SYN cookie发送数量值
        listenOverflows   float64    // 监听队列溢出值
        listenDrops       float64    // 监听队列丢弃值
        currentEstablished float64    // 当前已建立连接数值
        ctx               context.Context // 上下文
}

// 创建解析状态
func newParseState(ctx context.Context) *parseState {
        return &parseState{
                ctx: ctx,
        }
}

// 解析指标值
// valueStr: 待解析的字符串
// ctx: 上下文
// 返回值: 解析后的浮点数值，解析失败返回0并记录日志
func parseMetricValue(ctx context.Context, valueStr string) float64 {
        value, err := strconv.ParseFloat(valueStr, 64)
        if err != nil {
                utils.Log.Debug(ctx, "解析指标值失败",
                        zap.String("value", valueStr),
                        zap.Error(err))
                return 0
        }
        return value
}


// 行处理器类型
type lineHandler func([]byte, *parseState)

// 行处理器列表
var lineHandlers = []struct {
        prefix  string
        handler lineHandler
}{
        {prefix: KeyTCPExt,       handler: handleTCPExtLine},
        {prefix: "somaxconn:",    handler: handleSomaxconnLine},
        {prefix: KeyTCP,          handler: handleTCPLine},
}

// 处理TCP扩展行（自动区分表头与数据）
func handleTCPExtLine(lineBytes []byte, state *parseState) {
        fields := bytes.Fields(lineBytes)
        if len(fields) < 2 {
                return
        }
        // 如果第二个字段是非数字，则为表头行
        if !isNumericField(fields[1]) {
                keys := make([]string, len(fields)-1)
                for i, f := range fields[1:] {
                        keys[i] = string(f)
                }
                state.tcpExtKeys = keys
                utils.Log.Debug(state.ctx, "解析TCP扩展指标头",
                        zap.Strings("keys", keys),
                        zap.ByteString("line", lineBytes))
                return
        }
        // 否则为数据行
        if state.tcpExtKeys == nil {
                utils.Log.Debug(state.ctx, "跳过TCP扩展数据行，缺少表头")
                return
        }
        if len(fields)-1 < len(state.tcpExtKeys) {
                utils.Log.Debug(state.ctx, "TCP扩展数据行格式错误",
                        zap.ByteString("line", lineBytes),
                        zap.Int("fields", len(fields)),
                        zap.Int("keys", len(state.tcpExtKeys)))
                return
        }
        for i, key := range state.tcpExtKeys {
                value := parseMetricValue(state.ctx, string(fields[i+1]))
                switch key {
                case KeySyncookiesSent:
                        state.syncookiesSent = value
                case KeyListenOverflows:
                        state.listenOverflows = value
                case KeyListenDrops:
                        state.listenDrops = value
                }
        }
        utils.Log.Debug(state.ctx, "处理TCP扩展数据行",
                zap.Float64("SyncookiesSent", state.syncookiesSent),
                zap.Float64("ListenOverflows", state.listenOverflows),
                zap.Float64("ListenDrops", state.listenDrops))
}

// 处理somaxconn行
func handleSomaxconnLine(lineBytes []byte, state *parseState) {
        parts := bytes.SplitN(lineBytes, []byte(":"), 2)
        if len(parts) < 2 {
                return
        }

        valueBytes := bytes.TrimSpace(parts[1])
        valueStr := string(valueBytes)
        parsedValue := parseMetricValue(state.ctx, valueStr)
        state.somaxconnValue = parsedValue

        utils.Log.Debug(state.ctx, "解析somaxconn值", zap.Float64("value", parsedValue))
}

// 简化TCP行处理
func handleTCPLine(lineBytes []byte, state *parseState) {
        fields := bytes.Fields(lineBytes)
        if len(fields) < 2 {
                return
        }

        // 表头行处理（第一个字段是"Tcp:"，后续字段都是字符串）
        if !isNumericField(fields[1]) {
                handleTCPHeaderLine(fields, state)
                return
        }

        // 数据行处理（第一个字段是"Tcp:"，后续字段包含数字）
        handleTCPDataLine(fields, state)
}

// 判断字段是否为数字
func isNumericField(field []byte) bool {
        _, err := strconv.Atoi(string(field))
        return err == nil
}

// 处理TCP表头行
func handleTCPHeaderLine(fields [][]byte, state *parseState) {
        headers := make([]string, len(fields)-1)
        for i, f := range fields[1:] {
                headers[i] = string(f)
        }
        state.snmpHeaders = headers

        utils.Log.Debug(state.ctx, "设置TCP SNMP表头",
                zap.Strings("headers", headers))
}

// 默认TCP SNMP表头（来自/proc/net/snmp）
var defaultTCPHeaders = []string{
        "RtoAlgorithm",
        "RtoMin",
        "RtoMax",
        "MaxConn",
        "ActiveOpens",
        "PassiveOpens",
        "AttemptFails",
        "EstabResets",
        "CurrEstab",
        "InSegs",
        "OutSegs",
        "RetransSegs",
        "InErrs",
        "OutRsts",
        "InCsumErrors",
}

// 处理TCP数据行
func handleTCPDataLine(fields [][]byte, state *parseState) {
        headers := state.snmpHeaders
        if headers == nil {
                // 使用默认表头
                headers = defaultTCPHeaders
                utils.Log.Debug(state.ctx, "使用默认TCP表头",
                        zap.Strings("headers", headers))
        }

        // 检查数据行字段数是否与表头匹配
        if len(fields)-1 < len(headers) {
                utils.Log.Debug(state.ctx, "数据行字段数少于表头字段数",
                        zap.Int("fields", len(fields)-1),
                        zap.Int("headers", len(headers)))
                return
        }

        for headerIndex, headerName := range headers {
                if headerName != KeyCurrEstab {
                        continue
                }

                // 数据行中，字段索引为headerIndex+1（因为fields[0]是"Tcp:"）
                valueIndex := headerIndex + 1
                if valueIndex < len(fields) {
                        rawValue := string(fields[valueIndex])
                        parsedValue := parseMetricValue(state.ctx, rawValue)
                        state.currentEstablished = parsedValue
                        utils.Log.Debug(state.ctx, "解析CurrEstab值",
                                zap.String("raw", rawValue),
                                zap.Float64("parsed", parsedValue))
                }
                break
        }
}

// 流式解析TCP指标数据并上报（性能优化版）
func streamParseAndReport(r io.Reader, collector *TCPQueueCollector,
        metricChan chan<- prometheus.Metric, namespace, pod, ip, container string) {

        // 创建带trace ID的上下文
        ctx := context.WithValue(context.Background(), utils.TraceIDKey, "metrics-"+uuid.NewString())
        reader := bufio.NewReader(r)
        labels := []string{namespace, pod, ip, container}
        utils.Log.Debug(ctx, "开始解析TCP指标",
                zap.String("namespace", namespace),
                zap.String("pod", pod),
                zap.String("ip", ip),
                zap.String("container", container))

        state := newParseState(ctx)
        lineCount := 0
        handledCount := 0

        for {
                lineBytes, err := reader.ReadBytes('\n')
                if err != nil && len(lineBytes) == 0 {
                        break
                }

                lineCount++
                lineBytes = bytes.TrimSpace(lineBytes)
                if len(lineBytes) == 0 {
                        utils.Log.Trace(ctx, "跳过空行")
                        continue
                }

                utils.Log.Trace(ctx, "处理行",
                        zap.Int("lineNumber", lineCount),
                        zap.ByteString("content", lineBytes))

               for _, lh := range lineHandlers {
                       if bytes.HasPrefix(lineBytes, []byte(lh.prefix)) {
                               utils.Log.Trace(ctx, "匹配到处理器",
                                       zap.String("prefix", lh.prefix))
                               lh.handler(lineBytes, state)
                               handledCount++
                               break
                       }
               }
        }

        utils.Log.Debug(ctx, "解析完成",
                zap.Int("totalLines", lineCount),
                zap.Int("handledLines", handledCount),
                zap.Float64("SyncookiesSent", state.syncookiesSent),
                zap.Float64("ListenOverflows", state.listenOverflows),
                zap.Float64("ListenDrops", state.listenDrops))

        // 上报指标
        reportMetric := func(desc *prometheus.Desc, value float64, name string) {
                utils.Log.Debug(ctx, "上报指标",
                        zap.String("name", name),
                        zap.Float64("value", value),
                        zap.String("namespace", namespace),
                        zap.String("pod", pod),
                        zap.String("ip", ip),
                        zap.String("container", container))
                metricChan <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
        }

        reportMetric(collector.descMaxSomaxconn, state.somaxconnValue, KeySomaxconn)
        reportMetric(collector.descSyncSent, state.syncookiesSent, KeySyncookiesSent)
        reportMetric(collector.descListenOverflows, state.listenOverflows, KeyListenOverflows)
        reportMetric(collector.descListenDrops, state.listenDrops, KeyListenDrops)
        reportMetric(collector.descCurrentEstablished, state.currentEstablished, KeyCurrEstab)

        utils.Log.Debug(ctx, "指标上报完成",
                zap.String("namespace", namespace),
                zap.String("pod", pod),
                zap.String("ip", ip),
                zap.String("container", container))
}