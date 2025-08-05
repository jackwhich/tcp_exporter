#!/bin/bash
# 方法1: 直接访问API服务器（需要token）
TOKEN=$(kubectl -n ebpay-mid get secret $(kubectl -n ebpay-mid get serviceaccount tcp-exporter -o jsonpath='{.secrets[0].name}') -o jsonpath='{.data.token}' | base64 --decode)
POD_NAME=$(kubectl -n ebpay-mid get pods -l app=tcp-exporter -o jsonpath='{.items[0].metadata.name}')

curl -k -H "Authorization: Bearer $TOKEN" \
https://10.8.64.11:6443/api/v1/namespaces/ebpay-mid/pods/http:$POD_NAME:9325/proxy/metrics

# 方法2: 通过kubectl proxy（更简单）
# kubectl proxy --port=8001 &
# curl http://localhost:8001/api/v1/namespaces/ebpay-mid/pods/http:$POD_NAME:9325/proxy/metrics
# kill %1
