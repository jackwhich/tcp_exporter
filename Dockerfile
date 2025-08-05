### Dockerfile for Building and Running tcp-exporter

# Stage 1: Build Go binary
FROM golang:1.24-alpine AS builder

# 配置代理（根据你的环境调整）
ARG HTTP_PROXY=http://172.31.178.150:13080
ARG HTTPS_PROXY=http://172.31.178.150:13080
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}
ENV NO_PROXY=localhost,127.0.0.1

WORKDIR /app

# Copy module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build a fully compatible binary without stripping symbols
# Ensures symbol table integrity and matching Go version
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /tcp-exporter

# Stage 2: Minimal runtime image
FROM busybox:1.35.0-glibc
WORKDIR /app

# Copy compiled binary
COPY --from=builder /tcp-exporter /app/tcp-exporter

# 暴露指标端口
EXPOSE 9325

# 卷用于挂载配置
VOLUME ["/app/config"]

# 启动导出器
# 启动导出器
ENTRYPOINT ["/app/tcp-exporter", "--config=/app/config/config.yaml"]