# Python 包打包完整指南

本文档介绍如何将 RabbitMQ-ARQ 项目打包并发布到 PyPI，遵循现代 Python 打包标准（PEP 518、PEP 621、PEP 660）。

## 📁 项目结构

```
rabbitmq-arq/
├── src/
│   └── rabbitmq_arq/            # 主包目录
│       ├── __init__.py          # 包初始化文件
│       ├── client.py            # RabbitMQ 客户端
│       ├── worker.py            # Worker 实现
│       ├── models.py            # 数据模型
│       ├── connections.py       # 连接配置
│       ├── exceptions.py        # 异常定义
│       ├── protocols.py         # 协议定义
│       ├── cli.py              # 命令行工具
│       └── README.md           # 包说明文档
├── tests/                       # 测试代码
├── examples/                    # 使用示例
├── docs/                        # 文档
├── pyproject.toml              # 项目配置（核心）
├── README.md                   # 项目说明
├── LICENSE                     # 许可证
├── .gitignore                  # Git 忽略文件
└── requirements*.txt           # 依赖文件（可选）
```

## ⚙️ 配置文件详解

### pyproject.toml

这是现代 Python 包的核心配置文件，包含所有项目元数据：

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "rabbitmq-arq"
version = "0.1.0"
description = "一个基于RabbitMQ的任务队列库，提供类似arq的简洁API"
readme = "README.md"
license = {text = "MIT"}
authors = [
    {name = "RabbitMQ-ARQ Team", email = "rabbitmq-arq@example.com"}
]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Topic :: System :: Distributed Computing",
]
requires-python = ">=3.8"
dependencies = [
    "aio-pika>=9.0.0",
    "pydantic>=2.0.0",
    "click>=8.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "pytest-cov>=4.0.0",
    "black>=23.0.0",
    "isort>=5.12.0",
    "flake8>=6.0.0",
    "mypy>=1.5.0",
    "pre-commit>=3.0.0",
]
redis = [
    "redis>=4.5.0",
]
mongodb = [
    "pymongo>=4.3.0",
    "motor>=3.2.0",
]

[project.urls]
Homepage = "https://github.com/your-username/rabbitmq-arq"
Repository = "https://github.com/your-username/rabbitmq-arq"
Documentation = "https://rabbitmq-arq.readthedocs.io"
"Bug Tracker" = "https://github.com/your-username/rabbitmq-arq/issues"

[project.scripts]
rabbitmq-arq = "rabbitmq_arq.cli:main"

[tool.setuptools.packages.find]
where = ["src"]
```

### 关键配置说明

| 配置项 | 说明 | 重要性 |
|--------|------|--------|
| `name` | 包名称，必须在 PyPI 上唯一 | ⭐⭐⭐⭐⭐ |
| `version` | 版本号，遵循语义化版本 | ⭐⭐⭐⭐⭐ |
| `dependencies` | 核心依赖，只包含必需的包 | ⭐⭐⭐⭐⭐ |
| `optional-dependencies` | 可选依赖，按功能分组 | ⭐⭐⭐⭐ |
| `classifiers` | PyPI 分类标签 | ⭐⭐⭐ |
| `scripts` | 命令行入口点 | ⭐⭐⭐ |

## 🔧 构建工具

### 1. 安装构建工具

```bash
# 安装现代构建工具
pip install build twine

# 或者使用 pipx（推荐）
pipx install build
pipx install twine
```

### 2. 构建包

```bash
# 清理之前的构建
rm -rf dist/ build/ *.egg-info/

# 构建源码分发包和轮子
python -m build

# 构建结果
ls dist/
# rabbitmq_arq-0.1.0-py3-none-any.whl
# rabbitmq-arq-0.1.0.tar.gz
```

### 3. 验证构建

```bash
# 检查包内容
python -m zipfile -l dist/rabbitmq_arq-0.1.0-py3-none-any.whl

# 验证包元数据
twine check dist/*

# 本地安装测试
pip install dist/rabbitmq_arq-0.1.0-py3-none-any.whl
```

## 📦 发布流程

### 1. 准备发布

```bash
# 1. 更新版本号
# 编辑 pyproject.toml 中的 version 字段

# 2. 更新 CHANGELOG（如果有）
# 记录本次版本的变更

# 3. 提交代码
git add .
git commit -m "Release v0.1.0"
git tag v0.1.0
git push origin main --tags
```

### 2. 测试发布（TestPyPI）

```bash
# 注册 TestPyPI 账号
# https://test.pypi.org/account/register/

# 配置 API Token
# 在 ~/.pypirc 中添加：
[distutils]
index-servers =
    pypi
    testpypi

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-xxx  # 你的 TestPyPI token

# 上传到 TestPyPI
twine upload --repository testpypi dist/*

# 测试安装
pip install --index-url https://test.pypi.org/simple/ rabbitmq-arq
```

### 3. 正式发布（PyPI）

```bash
# 注册 PyPI 账号
# https://pypi.org/account/register/

# 配置 API Token
# 在 ~/.pypirc 中添加：
[pypi]
repository = https://upload.pypi.org/legacy/
username = __token__
password = pypi-xxx  # 你的 PyPI token

# 上传到 PyPI
twine upload dist/*

# 验证安装
pip install rabbitmq-arq
```

## 🤖 CI/CD 自动化

### GitHub Actions 工作流

创建 `.github/workflows/publish.yml`：

```yaml
name: Publish Python Package

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install build twine
    
    - name: Build package
      run: python -m build
    
    - name: Check package
      run: twine check dist/*
    
    - name: Publish to PyPI
      env:
        TWINE_USERNAME: __token__
        TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
      run: twine upload dist/*
```

### 预发布检查

创建 `.github/workflows/test.yml`：

```yaml
name: Test Package

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ['3.8', '3.9', '3.10', '3.11', '3.12']
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]
    
    - name: Run tests
      run: |
        pytest
    
    - name: Build package
      run: |
        pip install build
        python -m build
    
    - name: Check package
      run: |
        pip install twine
        twine check dist/*
```

## 📊 版本管理策略

### 语义化版本控制

遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)：

```
版本格式：MAJOR.MINOR.PATCH
示例：1.2.3

MAJOR：不兼容的 API 修改
MINOR：向下兼容的功能性新增
PATCH：向下兼容的问题修正
```

### 版本号示例

```
0.1.0  - 初始版本
0.1.1  - Bug 修复
0.2.0  - 新功能
1.0.0  - 首个稳定版本
1.0.1  - 补丁版本
1.1.0  - 向下兼容的新功能
2.0.0  - 重大变更，不向下兼容
```

### 自动版本管理

使用 `bump2version` 工具：

```bash
# 安装
pip install bump2version

# 配置 .bumpversion.cfg
[bumpversion]
current_version = 0.1.0
commit = True
tag = True

[bumpversion:file:pyproject.toml]
search = version = "{current_version}"
replace = version = "{new_version}"

# 使用
bump2version patch  # 0.1.0 -> 0.1.1
bump2version minor  # 0.1.1 -> 0.2.0
bump2version major  # 0.2.0 -> 1.0.0
```

## 🔍 质量检查

### 预提交钩子

创建 `.pre-commit-config.yaml`：

```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.7.0
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort

  - repo: https://github.com/pycqa/flake8
    rev: 6.0.0
    hooks:
      - id: flake8

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.5.1
    hooks:
      - id: mypy
        additional_dependencies: [types-all]
```

### 代码质量工具配置

在 `pyproject.toml` 中配置：

```toml
[tool.black]
line-length = 88
target-version = ['py38']

[tool.isort]
profile = "black"
line_length = 88

[tool.mypy]
python_version = "3.8"
strict = true

[tool.pytest.ini_options]
minversion = "6.0"
addopts = [
    "--strict-markers",
    "--cov=rabbitmq_arq",
    "--cov-report=term-missing",
]
```

## 📋 发布清单

### 发布前检查

- [ ] 代码质量检查通过
- [ ] 所有测试通过
- [ ] 文档更新完整
- [ ] 版本号正确
- [ ] CHANGELOG 更新
- [ ] 依赖项最新
- [ ] 许可证正确

### 发布步骤

1. **准备阶段**
   - [ ] 更新版本号
   - [ ] 运行完整测试
   - [ ] 检查依赖安全性

2. **构建阶段**
   - [ ] 清理旧构建
   - [ ] 构建新包
   - [ ] 验证包内容

3. **测试阶段**
   - [ ] 上传到 TestPyPI
   - [ ] 测试安装
   - [ ] 功能验证

4. **发布阶段**
   - [ ] 上传到 PyPI
   - [ ] 创建 Git 标签
   - [ ] 发布 GitHub Release

5. **后续工作**
   - [ ] 更新文档
   - [ ] 通知用户
   - [ ] 监控反馈

## 🚀 快速发布命令

创建 `scripts/release.sh`：

```bash
#!/bin/bash
set -e

# 检查参数
if [ $# -eq 0 ]; then
    echo "用法: $0 <version>"
    echo "示例: $0 0.1.1"
    exit 1
fi

VERSION=$1

echo "🚀 准备发布版本 $VERSION"

# 1. 清理
echo "📁 清理旧构建..."
rm -rf dist/ build/ *.egg-info/

# 2. 更新版本
echo "📝 更新版本号..."
sed -i "s/version = \".*\"/version = \"$VERSION\"/" pyproject.toml

# 3. 运行测试
echo "🧪 运行测试..."
python -m pytest

# 4. 构建
echo "📦 构建包..."
python -m build

# 5. 检查
echo "🔍 检查包..."
twine check dist/*

# 6. 提交代码
echo "💾 提交代码..."
git add .
git commit -m "Release v$VERSION"
git tag "v$VERSION"

# 7. 发布到 TestPyPI
echo "🚀 发布到 TestPyPI..."
twine upload --repository testpypi dist/*

echo "✅ 发布完成！"
echo "📋 下一步："
echo "  1. 测试 TestPyPI 安装"
echo "  2. 运行 'twine upload dist/*' 发布到 PyPI"
echo "  3. 运行 'git push origin main --tags'"
```

## 📚 相关资源

### 官方文档
- [Python Packaging User Guide](https://packaging.python.org/)
- [PEP 518 - pyproject.toml](https://peps.python.org/pep-0518/)
- [PEP 621 - 项目元数据](https://peps.python.org/pep-0621/)

### 工具链
- [build](https://pypa-build.readthedocs.io/) - 现代构建工具
- [twine](https://twine.readthedocs.io/) - 安全上传工具
- [setuptools](https://setuptools.pypa.io/) - 构建后端

### 最佳实践
- 使用 `src/` 布局
- 遵循语义化版本
- 编写完整测试
- 自动化 CI/CD
- 保持依赖最小化

---

**🎉 现在您可以将 RabbitMQ-ARQ 发布到 PyPI，让全世界的开发者都能使用！** 