package main

import (
        "bufio"
        "bytes"
        "io"
        "strconv"
        "sync"

        "github.com/prometheus/client_golang/prometheus"
        "go.uber.org/zap"
)

var bufPool = sync.Pool{
        New: func() any { return new(bytes.Buffer) },
}

// 定义指标键常量
const (
        KeySyncookiesSent    = "SyncookiesSent"    // SYN cookie发送数量
        KeyListenOverflows   = "ListenOverflows"   // 监听队列溢出次数
        KeyListenDrops       = "ListenDrops"       // 监听队列丢弃次数
        KeySomaxconn         = "somaxconn"         // 最大监听队列长度
        KeyCurrEstab         = "CurrEstab"         // 当前已建立连接数
        KeyTCPExt            = "TcpExt:"           // TCP扩展指标前缀
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
        logger            *Logger    // 日志记录器
        debugEnabled      bool       // 是否启用调试日志
}

// 创建解析状态
func newParseState(logger *Logger) *parseState {
        return &parseState{
                logger: logger,
                debugEnabled: logger.Core().Enabled(zap.DebugLevel),
        }
}

// 解析指标值
// valueStr: 待解析的字符串
// logger: 日志记录器
// 返回值: 解析后的浮点数值，解析失败返回0并记录日志
func parseMetricValue(valueStr string, logger *Logger) float64 {
        value, err := strconv.ParseFloat(valueStr, 64)
        if err != nil {
                logger.Debug("解析指标值失败", 
                        zap.String("value", valueStr), 
                        zap.Error(err))
                return 0
        }
        return value
}


// 行处理器类型
type lineHandler func([]byte, *parseState)

// 行处理器映射表
var lineHandlers = map[string]lineHandler{
        KeyTCPExt:    handleTCPExtLine,    // 处理TCP扩展指标行
        "somaxconn:": handleSomaxconnLine, // 处理somaxconn行
        KeyTCP:       handleTCPLine,       // 处理TCP基础指标行
}

// 简化日志和代码结构
func handleTCPExtLine(lineBytes []byte, state *parseState) {
        fields := bytes.Fields(lineBytes)
        if len(fields) > 1 {
                keys := make([]string, len(fields)-1)
                for i, f := range fields[1:] {
                        keys[i] = string(f)
                }
                state.tcpExtKeys = keys
                if state.debugEnabled {
                        state.logger.Debug("解析TCP扩展指标头", zap.ByteString("line", lineBytes))
                }
        }
}

// 简化日志和代码结构
func handleSomaxconnLine(lineBytes []byte, state *parseState) {
        parts := bytes.SplitN(lineBytes, []byte(":"), 2)
        if len(parts) < 2 {
                return
        }

        valueBytes := bytes.TrimSpace(parts[1])
        valueStr := string(valueBytes)
        parsedValue := parseMetricValue(valueStr, state.logger)
        state.somaxconnValue = parsedValue

        if state.debugEnabled {
                state.logger.Debug("解析somaxconn值", zap.Float64("value", parsedValue))
        }
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

        if state.debugEnabled {
                state.logger.Debug("设置TCP SNMP表头", 
                        zap.Strings("headers", headers))
        }
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
                if state.debugEnabled {
                        state.logger.Debug("使用默认TCP表头", 
                                zap.Strings("headers", headers))
                }
        }

        // 检查数据行字段数是否与表头匹配
        if len(fields)-1 < len(headers) {
                if state.debugEnabled {
                        state.logger.Debug("数据行字段数少于表头字段数",
                                zap.Int("fields", len(fields)-1),
                                zap.Int("headers", len(headers)))
                }
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
                        parsedValue := parseMetricValue(rawValue, state.logger)
                        state.currentEstablished = parsedValue

                        if state.debugEnabled {
                                state.logger.Debug("解析CurrEstab值", 
                                        zap.String("raw", rawValue),
                                        zap.Float64("parsed", parsedValue))
                        }
                }
                break
        }
}

// 流式解析TCP指标数据并上报（性能优化版）
func streamParseAndReport(r io.Reader, collector *TCPQueueCollector,
        metricChan chan<- prometheus.Metric, namespace, pod, ip, container string) {

        reader := bufio.NewReader(r)
        labels := []string{namespace, pod, ip, container}
        logger.Debug("开始解析TCP指标",
                zap.String("namespace", namespace),
                zap.String("pod", pod),
                zap.String("ip", ip),
                zap.String("container", container))

        state := newParseState(logger)
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
                        logger.Trace("跳过空行")
                        continue
                }

                logger.Trace("处理行",
                        zap.Int("lineNumber", lineCount),
                        zap.ByteString("content", lineBytes))

                handled := false
                for prefix, handler := range lineHandlers {
                        if bytes.HasPrefix(lineBytes, []byte(prefix)) {
                                logger.Trace("匹配到处理器",
                                        zap.String("prefix", prefix))
                                handler(lineBytes, state)
                                handled = true
                                handledCount++
                                break
                        }
                }

                if !handled && state.debugEnabled {
                        logger.Debug("未处理的行",
                                zap.ByteString("line", lineBytes))
                }
        }

        logger.Debug("解析完成",
                zap.Int("totalLines", lineCount),
                zap.Int("handledLines", handledCount))

        // 上报指标
        reportMetric := func(desc *prometheus.Desc, value float64, name string) {
                logger.Debug("上报指标",
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

        logger.Debug("指标上报完成",
                zap.String("namespace", namespace),
                zap.String("pod", pod),
                zap.String("ip", ip),
                zap.String("container", container))
}