# CLAUDE.md

此文件为 Claude Code (claude.ai/code) 在此代码库中工作时提供指导。

## 项目概述

RabbitMQ-ARQ 是一个基于 RabbitMQ 的异步任务队列库，提供类似 ARQ 的简洁 API。项目使用 Python 3.12+ 和现代类型注解，Pydantic V2 进行数据验证，完全基于 asyncio。

## 核心架构

### 主要组件
- **Worker** (`src/rabbitmq_arq/worker.py`): 核心 Worker 实现，包含智能错误处理和重试机制
- **Client** (`src/rabbitmq_arq/client.py`): RabbitMQ 客户端，用于任务提交和延迟机制检测
- **Connections** (`src/rabbitmq_arq/connections.py`): 连接管理和配置
- **Models** (`src/rabbitmq_arq/models.py`): 使用 Pydantic V2 的任务和工作器数据模型
- **Exceptions** (`src/rabbitmq_arq/exceptions.py`): 自定义异常层次结构

### 核心特性
- 高性能任务处理（≥5000 消息/秒）
- 类似 ARQ 的装饰器风格任务定义
- 智能错误分类和重试策略
- 延迟队列支持和自动机制检测
- 中文友好的日志和错误信息
- 内置监控和健康检查

## 开发命令

### 环境设置
项目使用名为 `rabbitmq_arq` 的 conda 虚拟环境。运行任何 Python 脚本前都要激活此环境：
```bash
conda activate rabbitmq_arq
```

### 测试
```bash
# 运行所有测试
pytest

# 运行覆盖率测试
pytest --cov=rabbitmq_arq --cov-report=html --cov-report=term-missing

# 运行特定标记的测试
pytest -m error_handling    # 错误处理测试
pytest -m integration       # 集成测试（需要外部服务）
pytest -m slow             # 长时间运行的测试

# 运行单个测试文件
pytest tests/test_error_handling.py
```

### 代码质量
```bash
# 格式化代码
black src tests examples
isort src tests examples

# 类型检查
mypy src

# 代码检查
flake8 src tests
```

### 构建和分发
```bash
# 使用提供的构建脚本
./scripts/build.sh

# 手动构建
python -m build

# 验证包
twine check dist/*
```

### 开发安装
```bash
# 以开发模式安装，包含所有依赖
pip install -e ".[dev]"
```

## 代码规范

### 来自 .cursor/rules/object-rules.mdc 的重要规则
- 使用 Python 3.12 特性和清晰的类型注解
- 使用最新的 Pydantic V2 进行数据验证
- 完全基于 asyncio 的实现
- 代码注释使用中文
- 始终在 `rabbitmq_arq` conda 虚拟环境中工作

### RabbitMQ 消息消费规则
- **禁止使用 GET API**: 严禁使用 RabbitMQ 的 GET API（如 `basic_get`）获取消息，这种方式是轮询模式，性能极差
- **必须使用 CONSUME 模式**: 只能使用 `basic_consume` 消费者模式，通过回调函数异步处理消息
- **推荐模式**: 使用 aio-pika 的 `queue.consume()` 或 `queue.iterator()` 方法进行消息消费
- **性能考虑**: GET 方式会导致频繁的网络往返，严重影响性能，而 CONSUME 方式是推送模式，性能更优

### 架构模式
- **错误分类**: `worker.py` 中的 `ErrorClassification` 类定义可重试与不可重试错误，用于智能重试策略
- **配置管理**: 使用 Pydantic 配置类（`WorkerSettings`、`RabbitMQSettings`）进行配置管理
- **任务生命周期**: 任务状态流程：pending → running → completed/failed，失败任务具有重试逻辑
- **连接管理**: 强健的连接处理，支持自动重连和连接池

### 任务定义模式
任务使用类似 ARQ 的装饰器模式定义：
```python
@task
async def my_task(data: dict) -> dict:
    # 任务实现
    return result
```

## 外部依赖

### 必需服务
- **RabbitMQ**: 核心消息代理（通常在 Docker 中运行：`docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management`）

### 关键 Python 依赖
- `aio-pika>=9.0.0`: RabbitMQ 客户端
- `pydantic>=2.0.0`: 数据验证
- `click>=8.0.0`: CLI 接口

## 测试策略

测试按功能组织：
- `test_error_handling.py`: 错误分类和重试机制测试
- 集成测试需要 RabbitMQ 服务运行
- 性能测试使用 `@pytest.mark.performance` 标记
- 异步测试通过 `asyncio_mode = "auto"` 自动处理

## 常见开发模式

### Worker 配置
Worker 使用 `WorkerSettings` 配置，具有高性能处理的智能默认值（prefetch_count=5000，突发处理，健康检查）。

### 客户端使用
`RabbitMQClient` 自动检测每个队列的延迟机制支持并缓存此信息以优化性能。

### 错误处理
项目实现了复杂的错误分类系统，区分可重试错误（网络问题、临时故障）和不可重试错误（代码错误、配置错误）。