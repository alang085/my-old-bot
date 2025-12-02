#!/bin/bash
# 生产环境运行收入明细查询脚本

# 设置日期（默认 2025-12-02）
DATE=${1:-2025-12-02}

echo "=========================================="
echo "生产环境收入明细查询"
echo "=========================================="
echo "日期: $DATE"
echo ""

# 进入项目目录
cd /app || {
    echo "❌ 错误: 无法进入 /app 目录"
    exit 1
}

# 设置数据库路径
export DATA_DIR=${DATA_DIR:-/data}

# 检查数据库文件
DB_PATH="$DATA_DIR/loan_bot.db"
if [ ! -f "$DB_PATH" ]; then
    echo "⚠️  警告: 数据库文件不存在: $DB_PATH"
    echo "   尝试使用默认路径..."
    DB_PATH="./loan_bot.db"
    if [ ! -f "$DB_PATH" ]; then
        echo "❌ 错误: 找不到数据库文件"
        exit 1
    fi
fi

echo "数据库路径: $DB_PATH"
echo "DATA_DIR: $DATA_DIR"
echo ""

# 检查脚本文件
if [ ! -f "list_all_income_records.py" ]; then
    echo "❌ 错误: 找不到脚本 list_all_income_records.py"
    echo "   请确保在项目根目录运行此脚本"
    echo "   或运行: git pull origin main"
    exit 1
fi

echo "=========================================="
echo "开始查询..."
echo "=========================================="
echo ""

# 运行脚本
python list_all_income_records.py "$DATE"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✅ 查询完成"
    echo "=========================================="
else
    echo ""
    echo "=========================================="
    echo "❌ 查询失败，退出码: $EXIT_CODE"
    echo "=========================================="
    exit $EXIT_CODE
fi

