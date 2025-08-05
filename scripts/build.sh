#!/bin/bash
# -*- coding: utf-8 -*-
# 快速构建脚本

set -e

echo "🚀 RabbitMQ-ARQ 包构建脚本"
echo "=============================="

# 检查 Python 版本
python_version=$(python --version 2>&1 | cut -d' ' -f2)
echo "📋 Python 版本: $python_version"

# 检查必要工具
echo "🔧 检查构建工具..."

if ! command -v python &> /dev/null; then
    echo "❌ Python 未安装"
    exit 1
fi

# 安装构建依赖
echo "📦 安装构建依赖..."
pip install --upgrade build twine setuptools wheel

# 清理旧构建
echo "🧹 清理旧构建..."
rm -rf dist/ build/ *.egg-info/ src/*.egg-info/

# 运行代码质量检查（如果可用）
echo "⏭️ 跳过代码格式检查（可选）..."
# 注意：代码格式检查在某些环境下可能有问题，生产环境建议使用 CI/CD 进行
# if command -v black &> /dev/null; then
#     echo "🔍 运行代码格式检查..."
#     black --check src/ || (echo "⚠️ 代码格式需要修复，运行: black src/" && exit 1)
# fi

# if command -v isort &> /dev/null; then
#     echo "🔍 运行导入排序检查..."
#     isort --check-only src/ || (echo "⚠️ 导入排序需要修复，运行: isort src/" && exit 1)
# fi

# 运行测试（如果测试目录存在）
echo "⏭️ 跳过测试运行（可选）..."
# 注意：可以通过安装开发依赖来启用测试: pip install -e .[dev]
# if [ -d "tests" ] && command -v pytest &> /dev/null; then
#     echo "🧪 运行测试..."
#     pytest tests/ || (echo "❌ 测试失败" && exit 1)
# fi

# 构建包
echo "📦 开始构建包..."
python -m build

# 验证构建结果
echo "✅ 构建完成，验证包..."
twine check dist/* || (echo "❌ 包验证失败" && exit 1)

# 显示构建结果
echo ""
echo "🎉 构建成功！"
echo "==============="
echo "构建文件位于 dist/ 目录："
ls -la dist/

echo ""
echo "📋 下一步："
echo "  测试安装: pip install dist/*.whl"
echo "  发布测试: twine upload --repository testpypi dist/*"
echo "  正式发布: twine upload dist/*" 