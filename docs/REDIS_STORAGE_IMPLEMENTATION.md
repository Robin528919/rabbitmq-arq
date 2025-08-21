# Redis 任务结果存储实现

## 概述

本文档描述了 RabbitMQ-ARQ 中 Redis 任务结果存储功能的实现。该功能允许将任务执行结果持久化存储到 Redis 中，支持任务状态查询、结果获取和批量操作。

## 功能特性

### 核心特性
- ✅ **多存储后端支持**: 内存存储 + Redis 存储，通过工厂模式统一管理
- ✅ **Worker 自动存储**: Worker 执行任务后自动存储结果（成功/失败均存储）
- ✅ **Client 查询 API**: 客户端可查询任务状态、结果和批量操作
- ✅ **TTL 自动过期**: 支持结果过期时间设置，避免内存泄漏
- ✅ **连接池优化**: 使用 Redis 连接池，支持高并发访问
- ✅ **原子操作**: 使用 Redis 管道确保多键操作的原子性
- ✅ **优雅错误处理**: 存储错误不影响任务执行流程

### 性能特性
- ✅ **批量操作**: 支持 MGET 批量查询，减少网络开销
- ✅ **键命名策略**: 统一的键前缀和命名规范
- ✅ **内存管理**: LRU 策略（内存存储）+ TTL 过期（Redis存储）
- ✅ **异步操作**: 完全基于 asyncio 的异步实现

## 架构设计

### 模块结构
```
src/rabbitmq_arq/result_storage/
├── __init__.py          # 模块导出
├── base.py              # 抽象基类和异常定义
├── models.py            # 数据模型（JobResult, 配置类等）
├── factory.py           # 存储工厂，支持多后端
├── memory.py            # 内存存储实现（参考实现）
└── redis.py             # Redis 存储实现（生产推荐）
```

### 关键类设计

#### 1. JobResult 数据模型
```python
class JobResult(BaseModel):
    job_id: str                    # 任务ID
    status: JobStatus              # 任务状态
    result: Any | None             # 任务结果数据
    error: str | None              # 错误信息
    start_time: datetime           # 开始时间
    end_time: datetime | None      # 结束时间
    duration: float | None         # 执行时长
    worker_id: str                 # Worker ID
    queue_name: str                # 队列名称
    retry_count: int               # 重试次数
    function_name: str             # 函数名称
    args: list[Any]                # 函数参数
    kwargs: dict[str, Any]         # 函数关键字参数
    created_at: datetime           # 创建时间
    expires_at: datetime | None    # 过期时间
```

#### 2. ResultStore 抽象基类
```python
class ResultStore(ABC):
    @abstractmethod
    async def store_result(self, job_result: JobResult) -> None: ...
    @abstractmethod
    async def get_result(self, job_id: str) -> JobResult | None: ...
    @abstractmethod
    async def get_results(self, job_ids: list[str]) -> dict[str, JobResult | None]: ...
    @abstractmethod
    async def get_status(self, job_id: str) -> JobStatus | None: ...
    @abstractmethod
    async def delete_result(self, job_id: str) -> bool: ...
    @abstractmethod
    async def cleanup_expired(self) -> int: ...
```

## 配置使用

### Worker 配置
```python
from rabbitmq_arq import Worker, WorkerSettings
from rabbitmq_arq.connections import RabbitMQSettings

# Redis 存储配置
redis_config = {
    'redis_url': 'redis://localhost:6379/0',
    'key_prefix': 'my_app',
    'ttl': 86400  # 24小时过期
}

# Worker 设置
worker_settings = WorkerSettings(
    rabbitmq_settings=RabbitMQSettings(rabbitmq_url="amqp://localhost:5672"),
    functions=[my_task],
    # 结果存储配置
    enable_job_result_storage=True,
    job_result_store_type='redis',
    job_result_store_config=redis_config
)

worker = Worker(worker_settings)
```

### Client 配置
```python
from rabbitmq_arq import create_client

# 创建带结果查询功能的客户端
client = await create_client(
    rabbitmq_settings=rabbitmq_settings,
    result_store_type='redis',
    result_store_config=redis_config
)

# 提交任务
job = await client.enqueue_job('my_task', data={'key': 'value'}, queue_name='default')

# 查询结果
result = await client.get_job_result(job.job_id)
status = await client.get_job_status(job.job_id)

# 批量查询
batch_results = await client.get_job_results([job1.job_id, job2.job_id])
```

## Redis 键命名策略

### 键名模式
- **结果数据**: `{prefix}:result:{job_id}`
- **状态索引**: `{prefix}:status:{job_id}`  
- **队列分组**: `{prefix}:queue:{queue_name}:results`

### 示例
```
my_app:result:abc123                    # 任务结果完整数据
my_app:status:abc123                    # 任务状态快速查询
my_app:queue:default:results            # 队列任务ID集合
```

## 性能优化

### Redis 优化
1. **连接池**: 使用 redis.asyncio 的连接池机制
2. **批量操作**: MGET 命令支持批量查询
3. **管道事务**: 多键操作使用 Pipeline 确保原子性
4. **TTL 管理**: 自动过期避免内存泄漏

### 内存存储优化
1. **LRU 策略**: OrderedDict 实现 LRU 淘汰
2. **异步清理**: 后台定期清理过期结果
3. **并发控制**: asyncio.Lock 保护并发访问

## 依赖管理

### 核心依赖
- `redis>=4.5.0`: Redis 异步客户端（包含 asyncio 支持）
- `pydantic>=2.0.0`: 数据模型验证

### 安装方式
```bash
# 安装 Redis 存储支持
pip install "rabbitmq-arq[redis]"

# 或手动安装
pip install "redis>=4.5.0"
```

## 测试和监控

### 健康检查
```python
# 存储健康检查
health = await store.health_check()

# 统计信息
stats = await store.get_stats()
```

### 测试用例
- ✅ 基本 CRUD 操作测试
- ✅ 批量操作测试  
- ✅ Worker 集成测试
- ✅ Client API 测试
- ✅ 错误处理测试
- ✅ TTL 过期测试

## 故障排除

### 常见问题

1. **Redis 连接失败**
   ```python
   # 检查 Redis 是否运行
   docker run -d --name redis -p 6379:6379 redis:7-alpine
   ```

2. **存储失败不影响任务执行**
   ```python
   # Worker 会记录警告日志但继续执行任务
   logger.warning(f"存储任务结果失败 {job_id}: {e}")
   ```

3. **依赖缺失**
   ```bash
   # 安装 Redis 依赖
   pip install "redis>=4.5.0"
   ```

## 未来扩展

### 已规划的扩展点
- 📋 **MongoDB 存储**: 支持文档数据库存储
- 📋 **PostgreSQL 存储**: 关系数据库支持
- 📋 **S3 存储**: 对象存储支持
- 📋 **Elasticsearch 存储**: 搜索引擎支持

### 扩展方式
```python
# 注册新的存储类型
from rabbitmq_arq.result_storage.factory import ResultStoreFactory

class MyCustomStore(ResultStore):
    # 实现抽象方法
    pass

ResultStoreFactory.register_store_type('custom', MyCustomStore)
```

## 总结

Redis 任务结果存储功能为 RabbitMQ-ARQ 提供了完整的任务结果持久化解决方案。通过模块化设计、多后端支持和性能优化，该功能可以满足从开发测试到生产环境的各种需求。

主要优势：
- 🚀 **高性能**: 异步操作 + 连接池 + 批量查询
- 🔧 **易使用**: 简单配置即可启用
- 🏗️ **可扩展**: 工厂模式支持多种存储后端
- 🛡️ **可靠性**: 优雅错误处理，不影响任务执行
- 📊 **可观测**: 内置监控和统计功能