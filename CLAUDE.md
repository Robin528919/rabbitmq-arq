# CLAUDE.md

此文件为 Claude Code (claude.ai/code) 在此代码库中工作时提供指导。

## 项目概述

RabbitMQ-ARQ 是一个基于 RabbitMQ 的异步任务队列库，提供类似 ARQ 的简洁 API。项目使用 Python 3.12+ 和现代类型注解，Pydantic V2 进行数据验证，完全基于 asyncio。

## 🎯 核心设计理念

**模仿 ARQ 库的优雅设计**是本项目的核心指导原则。所有 API 设计、数据结构、异常处理和用户体验都应该参考 [Python ARQ 库](https://github.com/python-arq/arq) 的最佳实践。

### ARQ 兼容性目标
- **API 风格一致性**：尽可能保持与 ARQ 相同的方法命名和调用方式
- **Job 对象中心化**：采用 Job 对象统一管理任务操作，而非分散的客户端方法
- **异常设计兼容**：使用类似 `ResultNotFound`、`JobNotFound` 等 ARQ 风格的异常
- **配置参数一致**：延迟任务参数如 `_defer_by`、`_defer_until` 保持 ARQ 命名
- **用户体验统一**：从 ARQ 迁移到本库应该是平滑和直观的

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
- **ARQ 风格 API**：完全模仿 ARQ 库的优雅设计和用户体验
- **Job 对象中心化**：统一的任务操作接口（`job.status()`, `job.result()`, `job.abort()`）
- **高性能任务处理**：≥5000 消息/秒的处理能力
- **JobContext 支持**：任务函数接收上下文参数，包含任务元信息
- **智能错误分类**：自动区分可重试和不可重试错误
- **延迟任务支持**：支持 `_defer_by` 和 `defer_until` 参数（ARQ 兼容）
- **Burst 模式**：处理完队列后自动退出的模式
- **生命周期钩子**：startup, shutdown, job_start, job_end 钩子函数
- **死信队列 (DLQ)**：失败任务的处理机制
- **结果存储系统**：多后端任务结果持久化（Redis、数据库、云存储）
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

### ARQ 库模仿规则 ⭐ 最高优先级
- **API 设计必须参考 ARQ**：在实现任何新功能前，先研究 ARQ 库的相应实现
- **保持方法命名一致**：使用 ARQ 相同的方法名（如 `enqueue_job`、`result()`、`status()`、`abort()`）
- **参数命名兼容**：延迟参数使用 `_defer_by`、`_defer_until`，作业ID使用 `_job_id`
- **异常类型统一**：使用 `ResultNotFound`、`JobNotFound` 等与 ARQ 一致的异常
- **Job 对象优先**：新功能应围绕 Job 对象设计，而非客户端分散方法
- **向 ARQ 用户友好**：确保从 ARQ 迁移的用户能快速上手

### 软件设计原则 ⭐ 高优先级

#### 职责分离原则 (Separation of Concerns)
开发过程中必须严格遵循职责分离原则，确保每个类、模块和函数都有单一、明确的职责：

- **单一职责原则 (SRP)**：每个类只负责一个功能领域
  ```python
  # ✅ 正确：职责清晰分离
  class JobResult:          # 只负责任务结果数据模型
  class Job:               # 只负责任务操作接口  
  class RabbitMQClient:    # 只负责 RabbitMQ 通信
  class ResultStore:       # 只负责结果存储
  class Worker:            # 只负责任务执行
  
  # ❌ 错误：职责混合
  class JobManager:        # 既负责数据模型又负责存储还负责网络通信
  ```

- **模块职责分离**：按功能领域组织代码
  ```python
  src/rabbitmq_arq/
  ├── client.py           # 客户端通信职责
  ├── worker.py           # 任务执行职责  
  ├── job.py             # 任务操作接口职责
  ├── models.py          # 数据模型职责
  ├── exceptions.py      # 异常定义职责
  ├── connections.py     # 连接配置职责
  └── result_storage/    # 结果存储职责
      ├── base.py        # 存储接口抽象
      ├── redis.py       # Redis存储实现
      ├── models.py      # 存储数据模型
      └── factory.py     # 存储工厂创建
  ```

- **接口分离原则 (ISP)**：接口设计要职责单一
  ```python
  # ✅ 正确：接口职责分离
  class ResultStore(ABC):
      async def store_result(self, result: JobResult) -> None: ...
      async def get_result(self, job_id: str) -> JobResult: ...
      
  class ConnectionManager(ABC):
      async def connect(self) -> None: ...
      async def close(self) -> None: ...
  
  # ❌ 错误：接口职责混合
  class JobManager(ABC):
      async def store_result(self, result: JobResult) -> None: ...
      async def send_message(self, message: str) -> None: ...
      async def format_log(self, msg: str) -> str: ...
  ```

- **依赖注入和控制反转**：高层模块不依赖低层模块细节
  ```python
  # ✅ 正确：依赖抽象而非具体实现
  class Worker:
      def __init__(self, result_store: ResultStore):  # 依赖抽象
          self.result_store = result_store
  
  # ❌ 错误：直接依赖具体实现
  class Worker:
      def __init__(self):
          self.result_store = RedisResultStore()  # 硬编码依赖
  ```

### Python 开发规范
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
- **结果存储**: 支持多种存储后端（内存、Redis），用于持久化任务结果和状态查询

### Redis 存储规则
- **依赖管理**: 使用 `redis>=4.5.0` 包，该包已集成异步 Redis 客户端，无需额外安装 `aioredis`
- **连接模式**: 使用 `redis.asyncio` 模块进行异步操作，而非过时的 `aioredis` 包
- **连接池**: 必须使用连接池模式，通过 `redis.asyncio.ConnectionPool` 管理连接
- **键命名策略**: 使用统一的键前缀和命名规范，例如 `{prefix}:result:{job_id}`
- **TTL 管理**: 所有存储的键必须设置合理的过期时间，避免内存泄漏
- **原子操作**: 使用 Redis 管道 (pipeline) 确保多键操作的原子性
- **错误处理**: 优雅处理 Redis 连接错误，存储失败不应影响任务执行流程

### ARQ 风格任务和客户端使用模式

#### 任务定义模式
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

#### ARQ 风格任务提交和结果处理
```python
from rabbitmq_arq import RabbitMQClient, Job, ResultNotFound

# 1. 创建客户端
client = RabbitMQClient()
await client.connect()

# 2. 提交任务 - 返回 Job 对象（ARQ 风格）
job = await client.enqueue_job(
    "process_data", 
    {"user_id": 123},
    queue_name="default",
    _defer_by=30,  # ARQ 兼容参数：延迟30秒
    _job_id="unique_task_123"  # ARQ 兼容参数：自定义任务ID
)

# 3. ARQ 风格的任务操作
print(f"Job ID: {job.id}")  # ARQ 兼容属性
status = await job.status()   # 获取状态
info = await job.info()       # 获取完整信息
result = await job.result(timeout=60)  # 等待结果（ARQ 风格）

# 4. 任务管理
aborted = await job.abort()   # 中止任务
deleted = await job.delete()  # 删除结果

# 5. 异常处理（ARQ 兼容）
try:
    result = await job.result()
except ResultNotFound:
    print("任务未完成")
except JobNotFound:
    print("任务不存在")
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

### 可选服务
- **Redis**: 任务结果存储（如果启用Redis存储）：`docker run -d --name redis -p 6379:6379 redis:7-alpine`

### 关键 Python 依赖
- `aio-pika>=9.0.0`: 高性能异步 RabbitMQ 客户端
- `pydantic>=2.0.0`: 数据验证和配置管理
- `click>=8.0.0`: 命令行接口框架
- `asyncio`: Python 原生异步 I/O 支持
- `typing`: 现代类型注解支持

### 可选 Python 依赖
- `redis>=4.5.0`: Redis 异步客户端，用于任务结果存储（包含 asyncio 支持）

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

### ARQ 风格客户端使用
- **Job 对象中心化**：`enqueue_job()` 返回 Job 对象，统一管理任务操作
- **连接管理**：自动连接管理和重连机制
- **延迟任务**：支持 `_defer_by`（秒数）和 `_defer_until`（具体时间） - ARQ 兼容参数
- **任务唯一性**：支持 `_job_id` 参数防止重复任务 - ARQ 兼容
- **结果等待**：`job.result(timeout=...)` 支持等待任务完成 - ARQ 风格
- **任务中止**：`job.abort()` 支持任务中止 - ARQ 兼容
- **异常处理**：使用 `ResultNotFound`、`JobNotFound` 等 ARQ 风格异常
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

## 🚀 未来开发指导

### 新功能开发流程
1. **研究 ARQ 实现**：在开发任何新功能前，先查阅 [ARQ 文档](https://arq-docs.helpmanual.io/) 和 [源码](https://github.com/python-arq/arq)
2. **保持 API 一致性**：确保新功能的方法名、参数名、返回值与 ARQ 保持一致
3. **优先 Job 对象**：新的任务相关功能应添加到 Job 类，而非客户端
4. **兼容性测试**：确保 ARQ 用户可以无缝迁移使用

### ARQ 兼容性清单
在实现新功能时，检查以下兼容性要求：

- [ ] **方法命名**：是否与 ARQ 相同？（如 `enqueue_job`、`result`、`status`、`abort`）
- [ ] **参数命名**：是否使用 ARQ 风格？（如 `_defer_by`、`_defer_until`、`_job_id`）
- [ ] **返回类型**：`enqueue_job` 是否返回 Job 对象？
- [ ] **异常类型**：是否使用 `ResultNotFound`、`JobNotFound`？
- [ ] **Job 属性**：是否支持 `job.id` 和 `job.job_id`？
- [ ] **超时支持**：`job.result(timeout=...)` 是否支持等待？
- [ ] **文档示例**：是否提供与 ARQ 类似的使用示例？

### 代码审查标准
- **ARQ 兼容性**：所有 PR 必须通过 ARQ 兼容性检查
- **API 一致性**：新 API 设计必须有 ARQ 库的对应实现参考
- **向后兼容性**：确保现有代码在升级后不受影响
- **文档更新**：新功能必须包含 ARQ 风格的使用示例

### 推荐学习资源
- [ARQ 官方文档](https://arq-docs.helpmanual.io/)
- [ARQ GitHub 仓库](https://github.com/python-arq/arq)
- [ARQ 示例代码](https://github.com/python-arq/arq/tree/main/docs/examples)
- 本项目的 `docs/arq-style-job-api.md` - ARQ 风格 API 设计指南