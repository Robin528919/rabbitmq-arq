# CLAUDE.md

此文件为 Claude Code (claude.ai/code) 在此代码库中工作时提供指导。

## 项目概述

RabbitMQ-ARQ 是一个基于 RabbitMQ 的异步任务队列库，提供类似 ARQ 的简洁 API。项目使用 Python 3.12+ 和现代类型注解，Pydantic V2 进行数据验证，完全基于 asyncio。

## 核心架构

### 主要组件
- **Worker** (`src/rabbitmq_arq/worker.py`): 核心 Worker 实现，包含智能错误处理、重试机制和 Burst 模式
- **Client** (`src/rabbitmq_arq/client.py`): RabbitMQ 客户端，用于任务提交和延迟机制检测
- **Connections** (`src/rabbitmq_arq/connections.py`): 连接管理和配置
- **Models** (`src/rabbitmq_arq/models.py`): 使用 Pydantic V2 的任务和工作器数据模型
- **Exceptions** (`src/rabbitmq_arq/exceptions.py`): 自定义异常层次结构
- **CLI** (`src/rabbitmq_arq/cli.py`): 完整的命令行工具集
- **Protocols** (`src/rabbitmq_arq/protocols.py`): 类型协议定义
- **Constants** (`src/rabbitmq_arq/constants.py`): 系统常量定义

### 核心特性
- **高性能任务处理**：≥5000 消息/秒的处理能力
- **JobContext 支持**：任务函数接收上下文参数，包含任务元信息
- **智能错误分类**：自动区分可重试和不可重试错误
- **延迟任务支持**：支持 `_defer_by` 和 `defer_until` 参数
- **Burst 模式**：处理完队列后自动退出的模式
- **生命周期钩子**：startup, shutdown, job_start, job_end 钩子函数
- **死信队列 (DLQ)**：失败任务的处理机制
- **中文友好**：日志和错误信息支持中文
- **命令行工具**：完整的 CLI 工具集
- **监控支持**：内置监控指标收集

## 开发命令

### 环境设置
项目使用名为 `rabbitmq_arq` 的 conda 虚拟环境。运行任何 Python 脚本前都要激活此环境：
```bash
conda activate rabbitmq_arq

# 验证环境
python --version  # 应显示 Python 3.12+
which python      # 应指向 conda 环境中的 Python
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
任务函数必须接收 `JobContext` 作为第一个参数：
```python
from rabbitmq_arq import JobContext, Retry

async def my_task(ctx: JobContext, data: dict) -> dict:
    """标准任务函数定义"""
    print(f"任务ID: {ctx.job_id}, 尝试次数: {ctx.job_try}")
    
    # 任务实现逻辑
    if some_condition and ctx.job_try <= 2:
        raise Retry(defer=5)  # 5秒后重试
    
    return {"processed": True, "data": data}
```

### Worker 配置模式
使用 `WorkerSettings` 进行完整配置：
```python
from rabbitmq_arq import Worker, WorkerSettings
from rabbitmq_arq.connections import RabbitMQSettings

rabbitmq_settings = RabbitMQSettings(
    rabbitmq_url="amqp://localhost:5672",
    prefetch_count=100,
    connection_timeout=30
)

worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[my_task],  # 任务函数列表
    worker_name="my_worker",
    queue_name="default",
    dlq_name="default_dlq",
    max_retries=3,
    job_timeout=300,
    max_concurrent_jobs=10,
    # 生命周期钩子
    on_startup=startup_hook,
    on_shutdown=shutdown_hook
)

worker = Worker(worker_settings)
```

## 外部依赖

### 必需服务
- **RabbitMQ**: 核心消息代理（通常在 Docker 中运行：`docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management`）

### 关键 Python 依赖
- `aio-pika>=9.0.0`: 高性能异步 RabbitMQ 客户端
- `pydantic>=2.0.0`: 数据验证和配置管理
- `click>=8.0.0`: 命令行接口框架
- `asyncio`: Python 原生异步 I/O 支持
- `typing`: 现代类型注解支持

### 开发依赖
- `pytest>=7.0.0`: 测试框架
- `pytest-asyncio`: 异步测试支持
- `pytest-cov`: 测试覆盖率
- `black`: 代码格式化
- `isort`: 导入排序
- `mypy`: 静态类型检查
- `flake8`: 代码质量检查

## 测试策略

### 测试结构
- `tests/test_error_handling.py`: 错误分类和重试机制测试
- `tests/conftest.py`: 测试配置和共享 fixtures
- `tests/pytest.ini`: pytest 配置文件

### 测试标记
- `@pytest.mark.error_handling`: 错误处理相关测试
- `@pytest.mark.integration`: 集成测试（需要 RabbitMQ 服务）
- `@pytest.mark.slow`: 长时间运行的测试
- `@pytest.mark.performance`: 性能测试

### 测试环境
- 异步测试通过 `asyncio_mode = "auto"` 自动处理
- 集成测试需要 RabbitMQ 服务运行
- 使用 Docker 启动测试依赖：`docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management`

### 示例代码测试
- `examples/example.py`: 完整的使用示例
- `examples/test_example.py`: 测试用例示例
- `examples/burst_example.py`: Burst 模式示例

## 常见开发模式

### Worker 配置
- **常规模式**：持续运行，处理队列中的消息
- **Burst 模式**：处理完队列中所有消息后自动退出
- **并发控制**：通过 `max_concurrent_jobs` 控制并发任务数
- **预取配置**：通过 `prefetch_count` 优化消息预取
- **超时控制**：任务执行超时和连接超时配置

### 客户端使用
- **连接管理**：自动连接管理和重连机制
- **延迟任务**：支持 `_defer_by`（秒数）和 `defer_until`（具体时间）
- **队列指定**：可为不同任务指定不同队列
- **批量提交**：支持并发批量任务提交

### 错误处理和重试
- **智能分类**：自动区分可重试和不可重试错误
- **可重试错误**：网络错误、超时、临时故障、显式 Retry 异常
- **不可重试错误**：语法错误、类型错误、参数错误、权限错误
- **重试策略**：指数退避、最大重试次数限制
- **死信队列**：最终失败的任务进入 DLQ

### 命令行工具使用
```bash
# 启动常规模式 Worker
rabbitmq-arq worker -m myapp.workers:worker_settings

# 启动 Burst 模式 Worker
rabbitmq-arq worker -m myapp.workers:worker_settings --burst

# 查看队列信息
rabbitmq-arq queue-info --queue default

# 验证配置
rabbitmq-arq validate-config -m myapp.workers:worker_settings
```