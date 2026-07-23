#!/bin/bash
# Modistreams Go 版本 - 构建和部署脚本
# 
# 使用方法:
#   1. 把 modistreams.go 和 go.mod 放到同一目录
#   2. 运行此脚本: bash build.sh
#   3. 启动比如: nohup ./modistreams > modistreams.log 2>&1 &
#
# 前置要求:
#   - Go 1.21+ (apt install golang-go 或从 go.dev 下载)
#   - Chrome/Chromium (apt install chromium)

set -e

echo "=========================================="
echo "Modistreams Go 版本构建"
echo "=========================================="

# 检查 Go
if ! command -v go &> /dev/null; then
    echo "❌ 未安装 Go，请先安装:"
    echo "   apt install golang-go"
    echo "   或从 https://go.dev/dl/go1.22.0.linux-amd64.tar.gz 下载"
    exit 1
fi
echo "✓ Go $(go version | awk '{print $3}')"

# 检查 Chrome
if command -v chromium-browser &> /dev/null; then
    echo "✓ Chromium 已安装"
elif command -v google-chrome &> /dev/null; then
    echo "✓ Chrome 已安装"
elif command -v chromium &> /dev/null; then
    echo "✓ Chromium 已安装"
else
    echo "⚠ 未检测到 Chrome/Chromium"
    echo "  安装: apt install chromium"
fi

# 下载依赖
echo ""
echo "下载依赖..."
go mod tidy

# 编译
echo ""
echo "编译中..."
CGO_ENABLED=0 go build -o modistreams modistreams.go

echo ""
echo "=========================================="
echo "✓ 构建成功: ./modistreams"
echo ""
echo "运行:"
echo "  ./modistreams                           # 前台运行"
echo "  nohup ./modistreams > modistreams.log 2>&1 &  # 后台运行"
echo "  PORT=8080 ./modistreams                 # 自定义端口"
echo ""
echo "接口:"
echo "  /stream?uri=xxx   → ts直连CDN"
echo "  /stream2?uri=xxx  → ts经Nginx透传"
echo "  /status           → 服务状态"
echo "  /clear-cache      → 清除缓存"
echo "  /restart          → 重启浏览器"
echo "=========================================="
