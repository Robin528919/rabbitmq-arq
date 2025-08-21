# GitHub Release 自动化指南

本文档详细介绍 RabbitMQ-ARQ 项目的**基于标签推送的自动 Release**方案，这是我们推荐的主要发布方式。

## 概述

基于标签推送的自动 Release 是最简单、最可控的发布方式。它通过 Git 标签触发自动化流程，让开发者完全控制发布时机，同时享受自动化构建、测试和发布的便利。

### 为什么选择这个方案？
- ✅ **控制力强**：完全由开发者决定发布时机
- ✅ **操作简单**：只需推送一个标签即可
- ✅ **易于理解**：符合传统的版本发布流程
- ✅ **回滚方便**：可以轻松删除标签和 release
- ✅ **兼容性好**：与各种开发工作流兼容

## 核心流程：基于标签推送的自动 Release

### 适用场景
- 开发者手动控制发布时机
- 适合传统的版本发布流程
- 简单直接，易于理解

### 使用步骤

1. **更新版本号**
   编辑 `pyproject.toml` 文件中的版本号：
   ```toml
   [project]
   version = "0.2.1"  # 更新为新版本
   ```

2. **提交版本变更**
   ```bash
   git add pyproject.toml
   git commit -m "bump version to 0.2.1"
   git push origin main
   ```

3. **创建并推送标签**
   ```bash
   # 创建带注释的标签
   git tag -a v0.2.1 -m "Release v0.2.1"
   
   # 推送标签到远程仓库
   git push origin v0.2.1
   ```

4. **自动化流程**
   - GitHub Actions 自动检测到标签推送
   - 自动构建 Python 包
   - 生成基于 git commits 的 changelog
   - 创建 GitHub release
   - 上传构建产物到 release

## 配置要求

在开始使用基于标签的自动发布之前，需要进行一些基本配置：

### GitHub Secrets 配置

在 GitHub 仓库设置中配置以下 Secrets：

1. **GITHUB_TOKEN**（自动提供）
   - GitHub 自动为 Actions 提供此令牌
   - 用于创建 release、上传文件等操作
   - 无需手动配置

2. **PYPI_API_TOKEN**（可选，用于自动发布到 PyPI）
   ```bash
   # 在 PyPI 创建 API Token
   # 1. 访问 https://pypi.org/manage/account/token/
   # 2. 创建新的 API token，范围设置为整个账户或特定项目
   # 3. 复制生成的 token（格式：pypi-...）
   # 4. 在 GitHub 仓库 Settings → Secrets and variables → Actions 中添加
   #    Name: PYPI_API_TOKEN
   #    Value: pypi-AgEIcHlwaS5vcmcC...
   ```

### GitHub Actions Workflow 配置

确保 `.github/workflows/release-on-tag.yml` 文件配置正确：

```yaml
name: Release on Tag

on:
  push:
    tags:
      - 'v*.*.*'  # 匹配 v1.0.0, v0.2.1 等格式的标签

permissions:
  contents: write  # 创建 release 需要写权限
  id-token: write  # PyPI trusted publishing 需要

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      # ... 详细步骤见实际 workflow 文件
```

## 发布流程

### 发布流程概览

```
1. 开发完成 → 2. 更新版本号 → 3. 提交代码 → 4. 创建标签 → 5. 推送标签
                                                      ↓
6. 自动构建 ← 7. 自动测试 ← 8. 触发 GitHub Actions ←
   ↓
9. 生成 Changelog → 10. 创建 Release → 11. 发布到 PyPI（可选）
```

### 详细操作步骤

#### 第一步：准备发布

1. **确保代码就绪**
   ```bash
   # 检查当前状态
   git status
   git log --oneline -10  # 查看最近提交
   
   # 运行测试确保一切正常
   conda activate rabbitmq_arq
   pytest
   black src tests examples && isort src tests examples
   mypy src
   ```

2. **更新版本号**
   编辑 `pyproject.toml` 中的版本号：
   ```toml
   [project]
   name = "rabbitmq-arq"
   version = "0.3.0"  # 从 0.2.x 升级到 0.3.0
   description = "RabbitMQ-based task queue with ARQ-compatible API"
   ```

3. **更新相关文档**（可选但推荐）
   - 更新 `CHANGELOG.md`（如果存在）
   - 确保 `README.md` 中的示例代码是最新的
   - 检查 `docs/` 目录中的文档是否需要更新

#### 第二步：提交版本变更

```bash
# 添加版本变更
git add pyproject.toml

# 如果还有其他文档更新
git add CHANGELOG.md README.md docs/

# 提交版本升级
git commit -m "bump version to v0.3.0

- 添加了 Redis 结果存储支持
- 优化了任务重试逻辑  
- 修复了连接超时问题"

# 推送到主分支
git push origin main
```

#### 第三步：创建和推送标签

```bash
# 创建带注释的标签（推荐）
git tag -a v0.3.0 -m "Release v0.3.0

主要更新：
- feat: 新增 Redis 任务结果存储支持
- feat: 支持自定义任务超时配置  
- fix: 修复连接池资源泄漏问题
- perf: 优化消息处理性能，提升 20%

破坏性变更：
- JobResult 数据结构调整，需要更新客户端代码

详细变更请查看：https://github.com/your-repo/compare/v0.2.x...v0.3.0"

# 验证标签创建成功
git tag -l "v0.3.0" -n9

# 推送标签到远程仓库（这将触发自动化流程）
git push origin v0.3.0
```

#### 第四步：监控自动化流程

推送标签后，GitHub Actions 会自动执行以下步骤：

1. **代码检出**: 获取代码和标签信息
2. **环境准备**: 设置 Python 环境和依赖
3. **代码质量检查**: 运行 black, isort, flake8, mypy
4. **测试执行**: 运行完整测试套件
5. **构建包**: 使用 `python -m build` 构建 wheel 和 sdist
6. **生成 Changelog**: 基于 git commits 自动生成变更日志
7. **创建 Release**: 在 GitHub 创建正式 release
8. **上传构建产物**: 将 .whl 和 .tar.gz 文件上传到 release
9. **发布到 PyPI**: （如果配置了 `PYPI_API_TOKEN`）自动发布到 PyPI

### 高级用法和技巧

#### 本地预测试发布流程

```bash
# 本地构建测试
conda activate rabbitmq_arq
python -m build
ls dist/  # 检查生成的文件

# 验证包安装
pip install dist/rabbitmq_arq-0.3.0-py3-none-any.whl

# 测试包功能
python -c "import rabbitmq_arq; print('Package imported successfully')"

# 使用 twine 检查包质量
twine check dist/*
```

#### 标签管理最佳实践

```bash
# 查看所有标签
git tag -l

# 查看标签详情
git show v0.3.0

# 删除错误的标签（本地和远程）
git tag -d v0.3.0          # 删除本地标签
git push origin :v0.3.0    # 删除远程标签

# 重新创建正确的标签
git tag -a v0.3.0 -m "Corrected release v0.3.0"
git push origin v0.3.0
```


### 版本管理策略

#### 语义化版本规范 (SemVer)

我们严格遵循 [语义化版本](https://semver.org/) 规范：

- **MAJOR**（主版本号): 不兼容的 API 变更
  - 例如: `1.0.0` → `2.0.0`
  - 移除公共 API、更改方法签名、重大架构调整

- **MINOR**（次版本号): 向后兼容的新功能
  - 例如: `1.0.0` → `1.1.0`  
  - 新增 API、新功能特性、性能优化

- **PATCH**（修订号): 向后兼容的问题修正
  - 例如: `1.0.0` → `1.0.1`
  - Bug 修复、安全补丁、文档更新

#### 版本号决策指南

```bash
# PATCH 版本 (1.0.0 → 1.0.1)
# 适用情况：
- 修复了任务执行中的内存泄漏
- 更正了文档中的错误示例
- 修复了边缘情况下的异常处理
- 改进了错误信息的清晰度

# MINOR 版本 (1.0.0 → 1.1.0)  
# 适用情况：
- 添加了新的结果存储后端支持
- 新增了任务优先级功能
- 增加了监控指标收集
- 支持新的配置选项

# MAJOR 版本 (1.0.0 → 2.0.0)
# 适用情况：
- 重构了 Job API，移除了废弃方法
- 更改了配置文件格式
- 修改了核心数据结构
- 更新最低 Python 版本要求
```

## 其他发布方案简介

虽然我们推荐基于标签推送的方案，但项目也支持其他自动化方案：

### 方案二：PR 自动版本管理
通过 PR 标题控制版本：`[patch] fix: 修复bug` → 自动发布 patch 版本

### 方案三：手动触发 Workflow  
在 GitHub Actions 页面手动运行 release workflow，适合临时发布需求

### 方案四：Conventional Commits
基于标准化的 commit 消息自动确定版本类型：`feat:` → minor, `fix:` → patch

> 详细说明请参考其他发布方案文档或联系维护者

## 发布质量保证

### 发布前检查清单

每次发布前，请完成以下检查：

#### 代码质量检查

```bash
# 激活开发环境
conda activate rabbitmq_arq

# 1. 代码格式化检查
black --check src tests examples
isort --check-only src tests examples

# 2. 代码质量检查
flake8 src tests

# 3. 类型检查
mypy src

# 4. 运行完整测试套件
pytest --cov=rabbitmq_arq --cov-report=term-missing

# 5. 性能测试（如果有）
pytest -m performance
```

#### 功能完整性检查

```bash
# 1. 验证示例代码可以正常运行
cd examples
python example.py
python burst_example.py

# 2. 测试命令行工具
rabbitmq-arq --help
rabbitmq-arq worker --help

# 3. 验证包依赖
pip check
```

#### 文档和版本检查

- [ ] `pyproject.toml` 版本号已正确更新
- [ ] `CHANGELOG.md` 包含本版本的更新内容
- [ ] `README.md` 中的示例代码是最新的
- [ ] API 文档反映了最新变更
- [ ] 破坏性变更在文档中有明确说明

### Release Notes 最佳实践

#### 高质量标签注释示例

```bash
git tag -a v0.3.0 -m "Release v0.3.0 - Redis 存储和性能优化

🚀 新功能：
- feat: 新增 Redis 任务结果存储支持 (#45)
- feat: 支持自定义任务超时配置 (#47)
- feat: 添加任务执行监控指标 (#48)

🐛 问题修复：
- fix: 修复连接池资源泄漏问题 (#43)
- fix: 解决高并发下的消息重复处理 (#44)
- fix: 修正 JobContext 中的时间戳精度问题 (#46)

⚡ 性能优化：
- perf: 优化消息处理性能，提升 20% 吞吐量
- perf: 减少内存占用，降低 15% 内存使用

💥 破坏性变更：
- JobResult.timestamp 字段类型从 float 改为 datetime
- 移除已废弃的 legacy_mode 配置选项
- 最低 Python 版本要求提升至 3.12

📖 文档更新：
- docs: 新增 Redis 存储配置指南
- docs: 更新性能调优建议
- docs: 添加监控指标说明

🔧 其他改进：
- chore: 更新依赖包到最新稳定版本
- test: 增加 Redis 存储的集成测试
- ci: 优化 GitHub Actions 构建速度

详细变更日志：https://github.com/your-org/rabbitmq-arq/compare/v0.2.5...v0.3.0
迁移指南：https://docs.example.com/migration/v0.3.0

Special thanks to @contributor1, @contributor2 for their contributions!"
```

#### Changelog 编写原则

1. **用户导向**: 从用户角度描述变更的价值和影响
2. **分类清晰**: 使用固定的分类（新功能、修复、性能、文档等）
3. **影响程度**: 明确标识破坏性变更和迁移要求
4. **可操作性**: 提供具体的升级和配置指导
5. **感谢贡献**: 认可社区贡献者的工作

### 发布时机规划

#### 定期发布策略

- **主版本**: 每 6-12 个月，包含重大功能或架构变更
- **次版本**: 每 4-8 周，包含新功能和显著改进
- **修订版本**: 根据需要，主要用于重要 bug 修复

#### 紧急发布策略

对于安全漏洞或严重 bug，遵循快速发布流程：

```bash
# 紧急修复发布示例
git checkout main
git pull origin main

# 创建紧急修复分支
git checkout -b hotfix/security-fix-v0.2.6

# 实现修复...
# 测试修复...

# 合并回主分支
git checkout main
git merge hotfix/security-fix-v0.2.6

# 立即发布
sed -i 's/version = "0.2.5"/version = "0.2.6"/' pyproject.toml
git add pyproject.toml
git commit -m "hotfix: 修复安全漏洞 CVE-2024-xxxx

- fix: 修复 JobContext 中的代码注入漏洞
- security: 增强输入验证和过滤

影响版本: 0.2.0 - 0.2.5
建议措施: 立即升级到 0.2.6"

git tag -a v0.2.6 -m "Security hotfix v0.2.6"
git push origin main v0.2.6
```

## 故障排除和调试

### 常见问题及解决方案

#### 1. 推送标签后没有自动创建 release

**可能原因及解决方案：**

```bash
# 检查标签格式
git tag -l  # 列出所有标签
# ✅ 正确格式: v0.3.0, v1.0.0, v2.1.3
# ❌ 错误格式: 0.3.0, release-0.3.0, ver-0.3.0

# 检查 GitHub Actions 权限
# 确保仓库设置 → Actions → General → Workflow permissions 
# 设置为 "Read and write permissions"

# 检查工作流文件是否存在和正确
ls -la .github/workflows/
cat .github/workflows/release-on-tag.yml

# 手动触发检查（如果有手动 workflow）
# GitHub 页面 → Actions → 选择对应 workflow → Run workflow
```

#### 2. Release 创建成功但 PyPI 发布失败

**诊断步骤：**

```bash
# 1. 检查包版本是否已存在于 PyPI
curl -s https://pypi.org/pypi/rabbitmq-arq/json | jq '.releases | keys[]'

# 2. 验证本地构建包
python -m build
twine check dist/*

# 3. 测试 PyPI 上传（使用测试 PyPI）
twine upload --repository testpypi dist/*

# 4. 检查 PYPI_API_TOKEN 配置
# GitHub 仓库 → Settings → Secrets and variables → Actions
# 确保 PYPI_API_TOKEN 存在且格式正确（pypi-...）
```

#### 3. Actions 执行失败或超时

**调试方法：**

```bash
# 查看详细的 Actions 日志
# GitHub → Actions → 点击失败的 workflow → 查看具体步骤日志

# 本地复现构建过程
conda activate rabbitmq_arq

# 模拟 Actions 环境
python -m pip install --upgrade pip build twine
python -m build
pytest --cov=rabbitmq_arq
black --check src tests examples
isort --check-only src tests examples
mypy src
flake8 src tests

# 检查网络和依赖问题
pip install --dry-run -e .
```

#### 4. 版本号更新失败

**解决步骤：**

```bash
# 检查 pyproject.toml 语法
python -c "import tomli; tomli.load(open('pyproject.toml', 'rb'))"

# 验证版本号格式
python -c "import re; assert re.match(r'^\d+\.\d+\.\d+$', '0.3.0')"

# 检查文件权限
ls -la pyproject.toml
git status

# 确保在正确分支上
git branch
git log --oneline -5
```

### 高级调试技巧

#### 1. 本地模拟完整发布流程

```bash
#!/bin/bash
# scripts/test-release-flow.sh

set -e  # 遇到错误立即退出

echo "🔍 开始本地发布流程测试..."

# 环境检查
echo "📦 检查 conda 环境..."
conda activate rabbitmq_arq
python --version

# 代码质量检查
echo "🧹 代码质量检查..."
black --check src tests examples || (echo "❌ Black 格式检查失败"; exit 1)
isort --check-only src tests examples || (echo "❌ isort 检查失败"; exit 1)
flake8 src tests || (echo "❌ flake8 检查失败"; exit 1)
mypy src || (echo "❌ mypy 类型检查失败"; exit 1)

# 运行测试
echo "🧪 运行测试套件..."
pytest --cov=rabbitmq_arq --cov-report=term-missing || (echo "❌ 测试失败"; exit 1)

# 构建包
echo "📦 构建 Python 包..."
python -m build || (echo "❌ 构建失败"; exit 1)

# 验证包
echo "✅ 验证包质量..."
twine check dist/* || (echo "❌ 包验证失败"; exit 1)

echo "🎉 本地发布流程测试通过！"
```

#### 2. 监控和日志分析

```bash
# 实时监控 GitHub Actions
watch -n 10 "gh run list --limit 5"

# 下载 Actions 日志进行本地分析
gh run download <run-id>

# 分析失败模式
grep -r "ERROR\|FAILED\|Exception" downloaded-logs/
```

### 紧急恢复程序

#### 回滚错误的发布

```bash
# 1. 删除错误的标签和 release
git tag -d v0.3.0                    # 删除本地标签
git push origin :refs/tags/v0.3.0    # 删除远程标签

# 2. 从 GitHub 删除 release（如果已创建）
gh release delete v0.3.0 --yes

# 3. 从 PyPI 撤回包（如果已发布且有问题）
# 注意：PyPI 不允许删除已发布的版本，只能发布新版本修复

# 4. 修复问题后重新发布
# 更正版本号（例如 v0.3.1）
sed -i 's/version = "0.3.0"/version = "0.3.1"/' pyproject.toml
git add pyproject.toml
git commit -m "fix: 修复 v0.3.0 发布中的问题"
git tag -a v0.3.1 -m "Release v0.3.1 - 修复 v0.3.0 问题"
git push origin main v0.3.1
```

### 预防性措施

1. **发布前检查清单**: 使用上面提供的检查脚本
2. **分阶段发布**: 先发布到测试环境，验证无误后再发布到生产
3. **自动化测试**: 增加集成测试覆盖关键功能路径
4. **监控设置**: 配置 GitHub Actions 失败通知
5. **回滚准备**: 提前准备回滚脚本和程序

## 总结和建议

**基于标签推送的自动 Release** 是我们强烈推荐的发布方案，它具有以下优势：

### 🎯 核心价值
- **完全掌控**: 开发者决定何时发布，避免意外发布
- **流程简单**: 只需三步：更新版本 → 推送代码 → 推送标签  
- **质量保证**: 自动化测试、构建、发布全流程
- **易于维护**: 出现问题时容易定位和修复

### 📈 最佳实践建议

1. **建立发布节奏**: 定期发布（如每月一次 minor 版本）
2. **完善测试覆盖**: 确保自动化测试覆盖核心功能
3. **文档同步更新**: 每次发布都更新相关文档
4. **社区沟通**: 重大版本发布前征求用户反馈
5. **监控发布质量**: 跟踪发布后的问题反馈和性能指标

### 🚀 持续改进

随着项目的发展，我们会持续优化发布流程：
- 添加更多自动化测试场景
- 完善监控和告警机制
- 优化构建和发布速度
- 增强文档和示例的质量

有任何问题或建议，欢迎在项目仓库创建 Issue 或与维护团队联系！