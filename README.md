# RabbitMQ ARQ

一个基于 RabbitMQ 的异步任务队列库，提供类似 [arq](https://github.com/samuelcolvin/arq) 的简洁 API。

## 特性

- 🚀 **高性能**: 支持 ≥5000 消息/秒的处理能力
- 🎯 **简洁 API**: 类似 arq 的装饰器风格，易于使用
- 💾 **结果存储**: Redis存储后端，URL自动识别配置
- 🔧 **易于迁移**: 提供从现有 Consumer 迁移的工具
- 🌐 **中文友好**: 支持中文日志输出
- 🔄 **高可用**: 内置重试机制和错误处理
- 📊 **监控支持**: 集成监控指标收集

## 快速开始

### 安装

```bash
# 基础安装（包含Redis支持）
pip install rabbitmq-arq

# 安装所有存储后端依赖（为未来版本做准备）
pip install "rabbitmq-arq[all]"

# 安装特定存储后端依赖
pip install "rabbitmq-arq[mongodb]"    # MongoDB（计划支持）
pip install "rabbitmq-arq[database]"   # 数据库支持（计划支持）
pip install "rabbitmq-arq[s3]"         # S3支持（计划支持）
```

> **注意**：当前版本只有Redis存储后端可用。其他存储后端的依赖包已预先配置，但功能将在未来版本中实现。

### 基本使用

#### 定义任务

```python
import asyncio
from rabbitmq_arq import JobContext, Retry

# 定义任务（带上下文的异步函数）
async def send_email(ctx: JobContext, to: str, subject: str, body: str) -> dict:
    """发送邮件任务"""
    print(f"发送邮件到 {to}: {subject}")
    print(f"任务ID: {ctx.job_id}, 尝试次数: {ctx.job_try}")
    
    # 模拟邮件发送逻辑
    await asyncio.sleep(1)
    
    # 模拟可能的失败和重试
    if "fail" in to and ctx.job_try <= 2:
        raise Retry(defer=5)  # 5秒后重试
    
    return {"to": to, "subject": subject, "sent_at": asyncio.get_event_loop().time()}

async def process_data(ctx: JobContext, data: dict) -> dict:
    """数据处理任务"""
    print(f"处理数据: {data}")
    print(f"任务ID: {ctx.job_id}")
    
    # 数据处理逻辑
    await asyncio.sleep(0.5)
    result = {"processed": True, "count": len(data), "processed_at": asyncio.get_event_loop().time()}
    return result
```

#### 发送任务

```python
import asyncio
from rabbitmq_arq import RabbitMQClient
from rabbitmq_arq.connections import RabbitMQSettings
from datetime import datetime, timedelta

async def main():
    # 创建客户端
    settings = RabbitMQSettings(rabbitmq_url="amqp://localhost:5672")
    client = RabbitMQClient(settings)
    
    # 连接并发送任务
    await client.connect()
    
    # 提交即时任务
    job = await client.enqueue_job(
        "send_email",  # 任务名称
        to="user@example.com",
        subject="欢迎使用 RabbitMQ ARQ",
        body="这是一个测试邮件",
        queue_name="default"  # 指定队列
    )
    print(f"即时任务已提交: {job.job_id}")
    
    # 提交延迟任务（延迟10秒）
    delayed_job = await client.enqueue_job(
        "process_data",
        data={"key": "value", "count": 100},
        queue_name="default",
        _defer_by=10  # 延迟10秒执行
    )
    print(f"延迟任务已提交: {delayed_job.job_id}")
    
    # 提交定时任务（指定时间执行）
    scheduled_job = await client.enqueue_job(
        "send_email",
        to="scheduled@example.com",
        subject="定时邮件",
        body="这是一个定时邮件",
        queue_name="default",
        defer_until=datetime.now() + timedelta(hours=1)  # 1小时后执行
    )
    print(f"定时任务已提交: {scheduled_job.job_id}")
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

#### 启动工作器

```python
import asyncio
from rabbitmq_arq import Worker, WorkerSettings
from rabbitmq_arq.connections import RabbitMQSettings

# 生命周期钩子函数
async def startup_hook(ctx: dict):
    """Worker 启动时执行"""
    print("🚀 Worker 启动中...")
    # 初始化资源，如数据库连接等
    ctx['start_time'] = asyncio.get_event_loop().time()

async def shutdown_hook(ctx: dict):
    """Worker 关闭时执行"""
    print("🛑 Worker 关闭中...")
    # 清理资源
    start_time = ctx.get('start_time', 0)
    runtime = asyncio.get_event_loop().time() - start_time
    print(f"运行时间: {runtime:.1f}秒")

async def main():
    # 配置设置
    rabbitmq_settings = RabbitMQSettings(
        rabbitmq_url="amqp://localhost:5672",
        prefetch_count=100,  # 消息预取数量
        connection_timeout=30
    )
    
    worker_settings = WorkerSettings(
        rabbitmq_settings=rabbitmq_settings,
        functions=[send_email, process_data],  # 任务函数列表
        worker_name="demo_worker",
        
        # 队列配置
        queue_name="default",
        dlq_name="default_dlq",  # 死信队列
        
        # 任务处理配置
        max_retries=3,
        retry_backoff=5.0,
        job_timeout=300,
        max_concurrent_jobs=10,
        
        # 生命周期钩子
        on_startup=startup_hook,
        on_shutdown=shutdown_hook,
        
        # 日志配置
        log_level="INFO"
    )
    
    # 创建并启动工作器
    worker = Worker(worker_settings)
    await worker.main()

if __name__ == "__main__":
    asyncio.run(main())
```

### 命令行工具

```bash
# 启动常规模式 Worker
rabbitmq-arq worker -m myapp.workers:worker_settings

# 启动 Burst 模式 Worker（处理完队列后自动退出）
rabbitmq-arq worker -m myapp.workers:worker_settings --burst

# 自定义配置启动 Worker
rabbitmq-arq worker -m myapp.workers:worker_settings \
    --rabbitmq-url amqp://user:pass@localhost:5672/ \
    --queue my_queue \
    --max-concurrent-jobs 20 \
    --burst-timeout 600

# 查看队列信息
rabbitmq-arq queue-info --queue default

# 清空队列
rabbitmq-arq purge-queue --queue default

# 验证 Worker 配置
rabbitmq-arq validate-config -m myapp.workers:worker_settings
```

## 高级特性

### 任务结果存储

RabbitMQ-ARQ 支持将任务结果持久化存储到Redis，便于后续查询和监控。通过 URL 自动识别Redis配置：

#### 配置存储后端

```python
from rabbitmq_arq import Worker, WorkerSettings, create_client
from rabbitmq_arq.connections import RabbitMQSettings

# Redis 存储配置（推荐）
rabbitmq_settings = RabbitMQSettings(rabbitmq_url="amqp://localhost:5672")

worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[your_tasks],
    worker_name="result_storage_worker",
    queue_name="default",
    
    # 任务结果存储配置
    enable_job_result_storage=True,
    job_result_store_url="redis://localhost:6379/0",  # 自动识别为 Redis
    job_result_ttl=86400,  # 结果保存24小时
)

# 客户端配置（用于查询结果）
client = await create_client(
    rabbitmq_settings=rabbitmq_settings,
    result_store_url="redis://localhost:6379/0"  # 与 Worker 使用相同的存储
)
```

#### 支持的存储后端

**当前支持（v0.2.0）**：
```python
# Redis（推荐，生产就绪）
"redis://localhost:6379/0"
"rediss://user:pass@localhost:6380/1"  # Redis SSL
```

**计划支持（未来版本）**：
```python
# 关系型数据库（计划中）
"postgresql://user:pass@localhost:5432/dbname"
"postgres://user:pass@localhost:5432/dbname"
"mysql://user:pass@localhost:3306/dbname"
"sqlite:///path/to/database.db"

# NoSQL 数据库（计划中）
"mongodb://localhost:27017/dbname"

# 云存储（计划中）
"s3://bucket-name/prefix"  # Amazon S3
```

> **注意**：当前版本（v0.2.0）只实现了Redis存储后端。其他存储后端将在后续版本中逐步添加。如果您需要其他存储后端支持，请在 [GitHub Issues](https://github.com/Robin528919/rabbitmq-mq/issues) 中提出需求。

#### 查询任务结果

```python
import asyncio
from rabbitmq_arq import create_client, JobContext
from rabbitmq_arq.connections import RabbitMQSettings

# 示例任务函数
async def data_processing_task(ctx: JobContext, data: dict) -> dict:
    """数据处理任务，返回处理结果"""
    await asyncio.sleep(1)  # 模拟处理时间
    return {
        "processed": True,
        "input_count": len(data),
        "result": f"processed_{data['id']}",
        "timestamp": asyncio.get_event_loop().time()
    }

async def main():
    # 创建客户端
    settings = RabbitMQSettings(rabbitmq_url="amqp://localhost:5672")
    client = await create_client(
        rabbitmq_settings=settings,
        result_store_url="redis://localhost:6379/0"
    )
    
    try:
        # 提交任务
        job = await client.enqueue_job(
            "data_processing_task",
            data={"id": "test_001", "value": "sample_data"},
            queue_name="default"
        )
        print(f"任务已提交: {job.job_id}")
        
        # 等待任务完成
        await asyncio.sleep(5)
        
        # 查询任务结果
        result = await client.get_job_result(job.job_id)
        if result:
            print(f"任务状态: {result.status}")
            print(f"任务结果: {result.result}")
            print(f"执行时长: {result.duration}秒")
            print(f"执行者: {result.worker_id}")
        else:
            print("任务结果未找到")
        
        # 查询任务状态（更轻量）
        status = await client.get_job_status(job.job_id)
        print(f"当前状态: {status}")
        
        # 批量查询结果
        batch_results = await client.get_job_results([job.job_id, "another_job_id"])
        print(f"批量查询结果: {len(batch_results)} 个结果")
        
        # 获取存储统计
        stats = await client.get_storage_stats()
        print(f"存储统计: {stats}")
        
        # 删除任务结果
        deleted = await client.delete_job_result(job.job_id)
        print(f"结果删除成功: {deleted}")
        
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

#### 结果存储配置选项

```python
worker_settings = WorkerSettings(
    # ... 其他配置 ...
    
    # 结果存储配置
    enable_job_result_storage=True,  # 是否启用结果存储
    job_result_store_url="redis://localhost:6379/0",  # 存储URL
    job_result_ttl=86400,  # 结果过期时间（秒），默认24小时
)
```

#### 存储的数据结构

任务结果包含以下信息：

```python
{
    "job_id": "abc123...",           # 任务ID
    "status": "completed",           # 任务状态
    "result": {...},                 # 任务返回结果
    "error": null,                   # 错误信息（如果失败）
    "start_time": "2025-01-15T10:30:00Z",  # 开始时间
    "end_time": "2025-01-15T10:30:05Z",    # 结束时间
    "duration": 5.2,                 # 执行时长（秒）
    "worker_id": "worker_001",       # 执行的Worker ID
    "queue_name": "default",         # 队列名称
    "retry_count": 0,                # 重试次数
    "function_name": "my_task",      # 函数名称
    "args": [1, 2, 3],              # 函数参数
    "kwargs": {"key": "value"},      # 函数关键字参数
    "created_at": "2025-01-15T10:30:00Z",  # 创建时间
    "expires_at": "2025-01-16T10:30:00Z"   # 过期时间
}
```

#### 最佳实践

1. **Redis存储配置**：
   - 开发环境：使用本地Redis实例 `redis://localhost:6379/0`
   - 生产环境：使用Redis集群或哨兵模式 `redis://user:pass@redis-cluster:6379/0`
   - 安全连接：使用SSL加密 `rediss://user:pass@redis.example.com:6380/0`

2. **设置合理的TTL**：
   ```python
   # 短期任务（1小时）
   job_result_ttl=3600
   
   # 中期任务（1天）
   job_result_ttl=86400
   
   # 长期任务（1周）
   job_result_ttl=604800
   ```

3. **监控存储使用**：
   ```python
   stats = await client.get_storage_stats()
   print(f"存储类型: {stats['store_type']}")
   print(f"总存储量: {stats['total_stored']}")
   print(f"成功率: {stats['success_rate']:.2%}")
   ```

### 错误处理和重试

RabbitMQ-ARQ 具有智能错误分类和自动重试机制：

```python
import random
from rabbitmq_arq import JobContext, Retry
from rabbitmq_arq.exceptions import MaxRetriesExceeded

async def reliable_task(ctx: JobContext, data: str) -> str:
    """具有重试机制的可靠任务"""
    print(f"任务执行，尝试次数: {ctx.job_try}")
    
    # 模拟可能失败的操作
    if random.random() < 0.3 and ctx.job_try <= 2:
        # 抛出 Retry 异常进行重试
        raise Retry(defer=5)  # 5秒后重试
    
    if ctx.job_try > 3:
        # 达到最大重试次数
        raise MaxRetriesExceeded(f"任务失败超过最大重试次数: {ctx.job_try}")
    
    return f"处理完成: {data}，尝试次数: {ctx.job_try}"

# Worker 的智能错误分类：
# ✅ 自动重试的错误：
#   - 网络连接错误（ConnectionError）
#   - 超时错误（TimeoutError）
#   - 临时服务不可用
#   - 显式的 Retry 异常
#
# ❌ 不重试的错误：
#   - 代码语法错误（SyntaxError）
#   - 类型错误（TypeError）
#   - 参数错误（ValueError）
#   - 权限错误（PermissionError）
```

### 延迟任务和定时任务

```python
import asyncio
from datetime import datetime, timedelta
from rabbitmq_arq import RabbitMQClient, JobContext
from rabbitmq_arq.connections import RabbitMQSettings

# 延迟任务函数
async def delayed_notification(ctx: JobContext, user_id: int, message: str):
    """延迟通知任务"""
    print(f"发送延迟通知给用户 {user_id}: {message}")
    print(f"任务ID: {ctx.job_id}，计划执行时间已到")
    return {"user_id": user_id, "message": message, "sent_at": datetime.now()}

async def main():
    settings = RabbitMQSettings(rabbitmq_url="amqp://localhost:5672")
    client = RabbitMQClient(settings)
    await client.connect()
    
    # 方式1: 延迟执行（使用 _defer_by 参数，单位：秒）
    job1 = await client.enqueue_job(
        "delayed_notification",
        user_id=123,
        message="这是一个延迟30秒的通知",
        queue_name="default",
        _defer_by=30  # 30秒后执行
    )
    print(f"延迟任务已提交: {job1.job_id}")
    
    # 方式2: 定时执行（使用 defer_until 参数）
    scheduled_time = datetime.now() + timedelta(hours=2)
    job2 = await client.enqueue_job(
        "delayed_notification",
        user_id=456,
        message="这是一个定时通知",
        queue_name="default",
        defer_until=scheduled_time  # 指定时间执行
    )
    print(f"定时任务已提交: {job2.job_id}，将在 {scheduled_time} 执行")
    
    # 方式3: 固定时间执行
    fixed_time = datetime(2025, 12, 31, 23, 59, 0)
    job3 = await client.enqueue_job(
        "delayed_notification",
        user_id=789,
        message="新年祝福",
        queue_name="default",
        defer_until=fixed_time
    )
    print(f"新年任务已提交: {job3.job_id}")
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## 性能优化

### 高并发配置

```python
from rabbitmq_arq import Worker, WorkerSettings
from rabbitmq_arq.connections import RabbitMQSettings

# 高性能 RabbitMQ 连接配置
rabbitmq_settings = RabbitMQSettings(
    rabbitmq_url="amqp://localhost:5672",
    prefetch_count=5000,     # 高预取数量，提升吞吐量
    connection_timeout=30,   # 连接超时时间
)

# 高性能 Worker 配置
worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[your_task_functions],
    worker_name="high_performance_worker",
    
    # 队列配置
    queue_name="high_performance",
    dlq_name="high_performance_dlq",
    
    # 高并发任务处理配置
    max_concurrent_jobs=50,   # 增加并发任务数
    job_timeout=600,         # 任务超时时间
    max_retries=3,
    retry_backoff=2.0,
    
    # Burst 模式配置（可选）
    burst_mode=False,        # 持续运行模式
    burst_check_interval=0.5, # 快速队列检查
    
    # 监控配置
    health_check_interval=30, # 健康检查间隔
    
    # 日志配置
    log_level="INFO"
)

worker = Worker(worker_settings)
```

### 批量任务提交

```python
import asyncio
from rabbitmq_arq import RabbitMQClient, JobContext
from rabbitmq_arq.connections import RabbitMQSettings

# 批量处理任务函数
async def batch_process_item(ctx: JobContext, item_id: int, data: str):
    """批量处理单个项目"""
    print(f"处理项目 {item_id}: {data}")
    await asyncio.sleep(0.1)  # 模拟处理时间
    return {"item_id": item_id, "processed": True, "result": f"processed_{data}"}

async def batch_submit_example():
    """批量提交任务示例"""
    settings = RabbitMQSettings(rabbitmq_url="amqp://localhost:5672")
    client = RabbitMQClient(settings)
    await client.connect()
    
    print("开始批量提交任务...")
    
    # 方式1: 并发提交任务（推荐）
    tasks = []
    for i in range(1000):
        task = client.enqueue_job(
            "batch_process_item",
            item_id=i,
            data=f"batch_data_{i}",
            queue_name="batch_queue"
        )
        tasks.append(task)
    
    # 等待所有任务提交完成
    jobs = await asyncio.gather(*tasks)
    print(f"✅ 成功提交了 {len(jobs)} 个任务")
    
    # 方式2: 分批提交（避免内存占用过大）
    batch_size = 100
    total_tasks = 1000
    submitted_count = 0
    
    for batch_start in range(0, total_tasks, batch_size):
        batch_tasks = []
        for i in range(batch_start, min(batch_start + batch_size, total_tasks)):
            task = client.enqueue_job(
                "batch_process_item",
                item_id=i + 1000,  # 避免ID重复
                data=f"batch_data_{i + 1000}",
                queue_name="batch_queue"
            )
            batch_tasks.append(task)
        
        # 等待当前批次提交完成
        batch_jobs = await asyncio.gather(*batch_tasks)
        submitted_count += len(batch_jobs)
        print(f"📦 已提交批次 {batch_start//batch_size + 1}，累计: {submitted_count} 个任务")
    
    print(f"🎉 批量提交完成，总计: {submitted_count} 个任务")
    await client.close()

if __name__ == "__main__":
    asyncio.run(batch_submit_example())
```

## 监控和日志

### 结构化日志和监控

```python
import logging
import asyncio
from rabbitmq_arq import JobContext

# 配置结构化日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('rabbitmq_arq.log')
    ]
)

# 创建专门的日志记录器
task_logger = logging.getLogger('rabbitmq_arq.task')
worker_logger = logging.getLogger('rabbitmq_arq.worker')
stats_logger = logging.getLogger('rabbitmq_arq.stats')

async def logged_task(ctx: JobContext, data: dict):
    """带有详细日志的任务"""
    task_logger.info(f"📋 任务开始: ID={ctx.job_id}, 尝试={ctx.job_try}")
    task_logger.info(f"📥 输入数据: {data}")
    
    start_time = asyncio.get_event_loop().time()
    
    try:
        # 处理逻辑
        await asyncio.sleep(1)  # 模拟处理时间
        result = {"processed": True, "data": data, "timestamp": start_time}
        
        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time
        
        task_logger.info(f"✅ 任务完成: 耗时 {duration:.2f}s")
        task_logger.info(f"📤 输出结果: {result}")
        
        return result
        
    except Exception as e:
        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time
        
        task_logger.error(f"❌ 任务失败: {str(e)}, 耗时 {duration:.2f}s")
        raise

# Worker 生命周期日志
async def startup_with_logging(ctx: dict):
    """带日志的启动钩子"""
    worker_logger.info("🚀 Worker 启动中...")
    worker_logger.info("📊 初始化监控指标...")
    
    ctx['stats'] = {
        'start_time': asyncio.get_event_loop().time(),
        'jobs_completed': 0,
        'jobs_failed': 0,
        'total_processing_time': 0.0
    }
    
    worker_logger.info("✅ Worker 启动完成")

async def shutdown_with_logging(ctx: dict):
    """带日志的关闭钩子"""
    worker_logger.info("🛑 Worker 正在关闭...")
    
    stats = ctx.get('stats', {})
    runtime = asyncio.get_event_loop().time() - stats.get('start_time', 0)
    
    stats_logger.info("📊 Worker 运行统计:")
    stats_logger.info(f"   总运行时间: {runtime:.1f}s")
    stats_logger.info(f"   完成任务数: {stats.get('jobs_completed', 0)}")
    stats_logger.info(f"   失败任务数: {stats.get('jobs_failed', 0)}")
    stats_logger.info(f"   总处理时间: {stats.get('total_processing_time', 0):.1f}s")
    
    worker_logger.info("✅ Worker 关闭完成")
```

### 监控指标

RabbitMQ-ARQ 自动收集以下监控指标：

- **任务指标**:
  - 任务执行时间和吞吐量
  - 成功/失败/重试率
  - 队列长度和积压情况
  - 任务类型分布

- **Worker 指标**:
  - Worker 状态和健康度
  - 并发任务数量
  - 内存和CPU使用情况
  - 连接状态

- **系统指标**:
  - RabbitMQ 连接池状态
  - 消息确认和拒绝率
  - 延迟任务调度准确性
  - 错误分类统计

可以通过命令行工具查看实时指标：

```bash
# 查看队列状态
rabbitmq-arq queue-info --queue default

# 监控 Worker 性能（如果配置了监控端点）
curl http://localhost:8080/metrics
```

## 开发

### 环境设置

```bash
# 克隆仓库
git clone https://github.com/your-username/rabbitmq-arq.git
cd rabbitmq-arq

# 创建并激活 conda 环境
conda create -n rabbitmq_arq python=3.12
conda activate rabbitmq_arq

# 安装开发依赖
pip install -e ".[dev]"

# 启动 RabbitMQ (使用 Docker)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### 运行测试

```bash
# 确保在正确的环境中
conda activate rabbitmq_arq

# 运行所有测试
pytest

# 运行带覆盖率的测试
pytest --cov=rabbitmq_arq --cov-report=html --cov-report=term-missing

# 运行特定类型的测试
pytest -m error_handling    # 错误处理测试
pytest -m integration       # 集成测试
pytest -m slow             # 长时间运行的测试

# 运行单个测试文件
pytest tests/test_error_handling.py
```

### 代码格式化

```bash
# 格式化代码
black src tests examples
isort src tests examples

# 类型检查
mypy src
```

## 配置

### 环境变量

支持以下环境变量配置：

- `RABBITMQ_URL`: RabbitMQ 连接 URL (默认: `amqp://guest:guest@localhost:5672/`)
- `RABBITMQ_PREFETCH_COUNT`: 消息预取数量 (默认: `100`)
- `RABBITMQ_CONNECTION_TIMEOUT`: 连接超时时间秒数 (默认: `30`)
- `ARQ_LOG_LEVEL`: 日志级别 (默认: `INFO`)
- `ARQ_MAX_CONCURRENT_JOBS`: 最大并发任务数 (默认: `10`)
- `ARQ_JOB_TIMEOUT`: 任务超时时间秒数 (默认: `300`)
- `ARQ_MAX_RETRIES`: 最大重试次数 (默认: `3`)
- `ARQ_RETRY_BACKOFF`: 重试退避时间秒数 (默认: `5.0`)
- `ARQ_WORKER_NAME`: Worker 名称 (默认: 自动生成)
- `ARQ_QUEUE_NAME`: 默认队列名称 (默认: `arq:queue`)
- `ARQ_BURST_MODE`: 是否启用 Burst 模式 (默认: `False`)
- `ARQ_BURST_TIMEOUT`: Burst 模式超时时间秒数 (默认: `300`)
- `ARQ_RESULT_STORE_URL`: 任务结果存储URL (默认: `redis://localhost:6379/0`)
- `ARQ_RESULT_STORE_TTL`: 结果存储TTL秒数 (默认: `86400`)
- `ARQ_ENABLE_RESULT_STORAGE`: 是否启用结果存储 (默认: `true`)

### 配置文件

```yaml
# config.yaml
rabbitmq:
  url: "amqp://localhost:5672"
  prefetch_count: 5000
  
worker:
  max_workers: 10
  queues: ["default", "high_priority"]
  result_storage:
    enabled: true
    store_url: "redis://localhost:6379/0"
    ttl: 86400  # 24小时
  
logging:
  level: "INFO"
  format: "structured"
```

## 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件。

## 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 这个仓库
2. 创建你的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交你的更改 (`git commit -m '添加一些很棒的特性'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 打开一个 Pull Request

## 更新日志

### v0.2.0 (最新版本)

**重大更新**:
- 🔄 **任务结果存储重构**: 简化配置方式，从多参数配置改为URL配置
- 🚀 **URL自动识别**: 通过URL自动识别Redis配置（redis://、rediss://）
- 🗑️ **移除内存存储**: 去除分布式环境下无用的内存存储选项
- 🔧 **架构优化**: 重构Worker类继承结构，解决属性依赖问题

**配置变更**:
```python
# 旧方式（已废弃）
worker_settings = WorkerSettings(
    job_result_store_type='redis',
    job_result_store_config={'redis_url': 'redis://localhost:6379/0'}
)

# 新方式（推荐）
worker_settings = WorkerSettings(
    job_result_store_url='redis://localhost:6379/0'  # 自动识别为Redis
)
```

**破坏性变更**:
- 移除了 `job_result_store_type` 配置参数
- 移除了 `job_result_store_config` 配置参数
- 移除了内存存储后端支持
- `create_client` 函数签名变更为URL配置

**迁移指南**:
```python
# 如果你之前使用了结果存储，请按以下方式更新配置：

# 旧配置
worker_settings = WorkerSettings(
    job_result_store_type='redis',
    job_result_store_config={
        'redis_url': 'redis://localhost:6379/0',
        'key_prefix': 'my_app'
    }
)

# 新配置
worker_settings = WorkerSettings(
    job_result_store_url='redis://localhost:6379/0'
    # 注意：key_prefix 等高级配置现在通过URL参数传递
)
```

### v0.1.0

**核心功能**:
- ✅ 基于 RabbitMQ 的异步任务队列实现
- ✅ 类似 ARQ 的简洁 API 设计
- ✅ 支持即时任务、延迟任务和定时任务
- ✅ 智能错误分类和自动重试机制
- ✅ 高性能工作器实现 (≥5000 消息/秒)

**高级特性**:
- ✅ JobContext 上下文支持，提供任务元信息
- ✅ Burst 模式支持（处理完队列后自动退出）
- ✅ 生命周期钩子函数（startup, shutdown, job_start, job_end）
- ✅ 死信队列 (DLQ) 支持
- ✅ 完整的命令行工具集

**开发体验**:
- ✅ 中文友好的日志和错误信息
- ✅ 详细的类型注解和文档
- ✅ 完整的测试覆盖
- ✅ 灵活的配置系统
- ✅ 监控和健康检查支持 