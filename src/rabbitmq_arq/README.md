# RabbitMQ-ARQ

基于 RabbitMQ 的异步任务队列库，提供类似 [arq](https://github.com/samuelcolvin/arq) 的简洁 API。

## 特性

- 🚀 **简洁的 API**：参考 arq 库设计，易于使用和理解
- 🔄 **自动重试**：支持任务失败自动重试，可配置重试策略
- ⏰ **延迟执行**：支持延迟和定时任务
- 🛡️ **死信队列**：失败任务自动转移到死信队列
- 📊 **任务统计**：实时任务执行统计
- 🔌 **生命周期钩子**：startup/shutdown/job_start/job_end 钩子
- 🌐 **中文日志**：完整的中文日志支持
- ⚡ **高性能**：支持高并发处理（prefetch_count 可配置）
- 🎯 **Burst 模式**：类似 arq 的 burst 参数，处理完队列后自动退出
- 🖥️ **命令行工具**：提供 CLI 工具支持，便于集成到 CI/CD
- ⏰ **企业级延迟队列**：基于 RabbitMQ TTL + DLX，非阻塞高性能延迟任务
- 🔧 **配置分离**：连接配置与业务配置分离，更好的可维护性

## 安装

```bash
pip install aio-pika pydantic click
```

## 快速开始

### 1. 定义任务函数

```python
from rabbitmq_arq import JobContext, Retry

async def process_data(ctx: JobContext, data_id: int, action: str):
    """处理数据的任务函数"""
    print(f"处理数据 {data_id}，操作: {action}")
    print(f"任务 ID: {ctx.job_id}")
    print(f"尝试次数: {ctx.job_try}")
    
    # 你的业务逻辑
    if action == "retry":
        # 请求重试
        raise Retry(defer=10)  # 10秒后重试
    
    return {"status": "success", "data_id": data_id}
```

### 2. 配置 Worker

```python
from rabbitmq_arq import Worker, WorkerSettings, RabbitMQSettings

# RabbitMQ 连接配置
rabbitmq_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    prefetch_count=100,
    connection_timeout=30
)

# Worker 配置
worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[process_data],
    worker_name="my_worker",
    
    # 队列配置
    queue_name="my_queue",
    dlq_name="my_queue_dlq",
    
    # 任务处理配置
    max_retries=3,
    retry_backoff=5.0,
    job_timeout=300,
    max_concurrent_jobs=10,
    
    # 日志配置
    log_level="INFO"
)

# 运行 Worker
if __name__ == "__main__":
    worker = Worker(worker_settings)
    import asyncio
    asyncio.run(worker.main())
```

### 3. 提交任务

```python
from rabbitmq_arq import RabbitMQClient, RabbitMQSettings
import asyncio

async def submit_tasks():
    # 创建客户端（只需要连接配置）
    rabbitmq_settings = RabbitMQSettings(
        rabbitmq_url="amqp://guest:guest@localhost:5672/"
    )
    client = RabbitMQClient(rabbitmq_settings)
    
    try:
        await client.connect()
        
        # 提交任务
        job = await client.enqueue_job(
            "process_data",
            data_id=123,
            action="process",
            queue_name="my_queue"  # 指定队列名
        )
        print(f"任务已提交: {job.job_id}")
        
        # 提交延迟任务
        delayed_job = await client.enqueue_job(
            "process_data",
            data_id=456,
            action="cleanup",
            queue_name="my_queue",
            _defer_by=60  # 60秒后执行
        )
        print(f"延迟任务已提交: {delayed_job.job_id}")
        
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(submit_tasks())
```

## 高级功能

### 生命周期钩子

```python
async def startup(ctx: dict):
    """Worker 启动时执行"""
    # 初始化数据库连接、HTTP 客户端等
    ctx['db'] = await create_db_connection()
    ctx['http_client'] = aiohttp.ClientSession()

async def shutdown(ctx: dict):
    """Worker 关闭时执行"""
    # 清理资源
    await ctx['db'].close()
    await ctx['http_client'].close()

async def on_job_start(ctx: dict):
    """每个任务开始前执行"""
    print(f"任务 {ctx['job_id']} 开始执行")

async def on_job_end(ctx: dict):
    """每个任务结束后执行"""
    print(f"任务 {ctx['job_id']} 执行结束")

# Worker 配置（使用新的配置结构）
worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[process_data],
    worker_name="my_worker",
    
    # 队列配置
    queue_name="my_queue",
    
    # 生命周期钩子
    on_startup=startup,
    on_shutdown=shutdown,
    on_job_start=on_job_start,
    on_job_end=on_job_end,
    
    # 其他配置...
)
```

### 批量提交任务

```python
jobs = await client.enqueue_jobs([
    {
        "function": "process_data",
        "args": [1, "action1"],
        "kwargs": {"priority": "high"}
    },
    {
        "function": "process_data",
        "args": [2, "action2"],
        "_defer_by": 30  # 延迟30秒
    }
])
```

### 错误处理和重试

```python
from rabbitmq_arq import Retry

async def unreliable_task(ctx: JobContext, url: str):
    """可能失败的任务"""
    try:
        result = await fetch_data(url)
    except NetworkError:
        # 网络错误，30秒后重试（使用 RabbitMQ TTL 延迟队列）
        raise Retry(defer=30)
    except InvalidDataError:
        # 数据错误，使用指数退避重试（非阻塞延迟）
        raise Retry(defer=ctx.job_try * 10)
    except FatalError:
        # 致命错误，不再重试
        raise
    
    return result
```

### 延迟任务（企业级实现）

RabbitMQ-ARQ 智能选择最佳延迟机制：

1. **优先使用 RabbitMQ 延迟插件** - 如果安装了 `rabbitmq_delayed_message_exchange`
2. **降级到 TTL + DLX 方案** - 如果插件未安装，自动使用备选方案

```python
# 延迟任务示例
async def send_reminder_email(ctx: JobContext, user_id: int):
    """发送提醒邮件"""
    await send_email(user_id, "请完成您的操作")

# 提交延迟任务
job = await client.enqueue_job(
    "send_reminder_email",
    user_id=123,
    _defer_by=3600  # 1小时后执行，Worker 不会阻塞
)

# 延迟到具体时间
from datetime import datetime, timedelta
future_time = datetime.now() + timedelta(hours=24)
job = await client.enqueue_job(
    "daily_report",
    _defer_until=future_time  # 24小时后执行
)
```

#### 延迟队列优势

- ✅ **非阻塞**：Worker 立即处理下一个任务
- ✅ **高并发**：支持数千个并发延迟任务  
- ✅ **可靠持久**：延迟状态存储在 RabbitMQ 中
- ✅ **分布式**：多个 Worker 节点无影响
- ✅ **原生支持**：基于 RabbitMQ 成熟功能

## 配置选项

### RabbitMQ 连接配置

```python
from rabbitmq_arq import RabbitMQSettings

# 连接配置（仅连接相关）
rabbitmq_settings = RabbitMQSettings(
    # 基础连接配置
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    connection_timeout=30,          # 连接超时时间（秒）
    heartbeat=60,                   # 心跳间隔（秒）
    
    # 连接池配置
    connection_pool_size=10,        # 连接池大小
    channel_pool_size=100,          # 通道池大小
    
    # 性能配置
    prefetch_count=100,             # 预取消息数量
    enable_compression=False,       # 是否启用消息压缩
    
    # 安全配置
    ssl_enabled=False,              # 是否启用SSL
    ssl_cert_path=None,             # SSL证书路径
    ssl_key_path=None,              # SSL私钥路径
    
    # 重连配置
    auto_reconnect=True,            # 是否自动重连
    reconnect_interval=5.0,         # 重连间隔（秒）
    max_reconnect_attempts=10,      # 最大重连次数
)
```

### Worker 配置

```python
from rabbitmq_arq import WorkerSettings

# Worker 配置（业务逻辑配置）
worker_settings = WorkerSettings(
    # 基础配置
    rabbitmq_settings=rabbitmq_settings,
    functions=[process_data],
    worker_name="my_worker",
    
    # 队列配置
    queue_name="my_queue",
    dlq_name="my_queue_dlq",
    queue_durable=True,
    queue_exclusive=False,
    queue_auto_delete=False,
    
    # 任务处理配置
    max_retries=3,                  # 最大重试次数
    retry_backoff=5.0,              # 重试退避时间（秒）
    job_timeout=300,                # 任务超时时间（秒）
    max_concurrent_jobs=10,         # 最大并发任务数
    
    # 任务结果配置
    enable_job_result_storage=True, # 是否存储任务结果
    job_result_ttl=86400,           # 任务结果保存时间（秒）
    
    # Worker运行模式配置
    health_check_interval=60,       # 健康检查间隔（秒）
    job_completion_wait=5,          # 关闭时等待任务完成时间（秒）
    graceful_shutdown_timeout=30,   # 优雅关闭总超时（秒）
    
    # 日志配置
    log_level="INFO",               # 日志级别
    log_format=None,                # 日志格式
    log_file=None,                  # 日志文件路径
    
    # 延迟任务配置
    enable_delayed_jobs=True,       # 启用延迟任务
    delay_mechanism="auto",         # 延迟机制（auto/plugin/ttl）
    
    # 调试配置
    debug_mode=False,               # 调试模式
    trace_tasks=False,              # 追踪任务执行
)
```

## Burst 模式

Burst 模式类似于 [arq](https://github.com/samuelcolvin/arq) 的 burst 参数，适用于批处理和定时任务场景。

### 特点

- 🎯 **自动退出**：处理完队列中的所有任务后自动退出
- ⏱️ **超时保护**：设置最大运行时间，防止无限期运行
- 🔄 **智能监控**：定期检查队列状态，动态决定是否退出
- ⚙️ **灵活配置**：可选择是否等待正在执行的任务完成

### 使用示例

```python
# Burst 模式 Worker 配置
burst_worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[process_data],
    worker_name="burst_worker",
    
    # 队列配置
    queue_name="batch_queue",
    
    # Burst 模式配置
    burst_mode=True,                # 启用 burst 模式
    burst_timeout=600,              # 最多运行 10 分钟
    burst_check_interval=2.0,       # 每 2 秒检查一次队列状态
    burst_wait_for_tasks=True,      # 退出前等待任务完成
    burst_exit_on_empty=True,       # 队列为空时是否退出
    
    # 其他配置...
)

# 运行 Burst Worker
worker = Worker(burst_worker_settings)
asyncio.run(worker.main())
```

### 适用场景

- **定时批处理**：每小时/每天处理积累的数据
- **数据迁移**：一次性处理大量数据迁移任务
- **CI/CD 流水线**：在部署流程中处理特定任务
- **报告生成**：定期生成和发送报告
- **清理任务**：定期清理临时文件和过期数据

## 与现有项目集成

### 迁移现有消费者

```python
# 旧代码
class FollowersConsumer:
    async def on_message(self, message):
        # 复杂的消息处理逻辑
        pass

# 新代码
async def process_followers(ctx: JobContext, follower_data: dict):
    # 只需要关注业务逻辑
    result = await save_to_mongodb(follower_data)
    return result

class WorkerSettings:
    functions = [process_followers]
    rabbitmq_settings = settings
```

### 与 FastAPI 集成

```python
from fastapi import FastAPI, Depends
from rabbitmq_arq import RabbitMQClient

app = FastAPI()
client = None

@app.on_event("startup")
async def startup_event():
    global client
    client = RabbitMQClient(settings)
    await client.connect()

@app.on_event("shutdown")
async def shutdown_event():
    if client:
        await client.close()

@app.post("/submit-task")
async def submit_task(data: dict):
    job = await client.enqueue_job("process_data", data)
    return {"job_id": job.job_id}
```

## 监控和调试

### 查看 Worker 状态

Worker 会定期输出统计信息：

```
2025-01-10 10:30:45 - 收到信号 SIGTERM ◆ 100 个任务完成 ◆ 5 个失败 ◆ 10 个重试 ◆ 2 个待完成
```

### 健康检查

Worker 定期进行健康检查，可以集成到 K8s 或其他监控系统。

## 命令行工具

RabbitMQ-ARQ 提供了便捷的命令行工具，支持分离的连接和业务配置：

### 安装后可用命令

```bash
# 启动常规模式 Worker
rabbitmq-arq worker -m myapp.workers:worker_settings

# 启动 Burst 模式 Worker
rabbitmq-arq worker -m myapp.workers:worker_settings --burst

# 使用自定义连接和Worker配置
rabbitmq-arq worker -m myapp.workers:worker_settings \
    --rabbitmq-url amqp://user:pass@localhost:5672/ \
    --prefetch-count 50 \
    --connection-timeout 60 \
    --queue my_queue \
    --max-retries 5 \
    --job-timeout 600 \
    --max-concurrent-jobs 20 \
    --burst \
    --burst-timeout 600 \
    --burst-no-wait

# 查看队列信息
rabbitmq-arq queue-info --queue my_queue --rabbitmq-url amqp://localhost

# 清空队列
rabbitmq-arq purge-queue --queue my_queue --rabbitmq-url amqp://localhost

# 验证Worker配置
rabbitmq-arq validate-config -m myapp.workers:worker_settings

# 查看所有可用选项
rabbitmq-arq worker --help
```

### 命令行参数详解

#### RabbitMQ 连接配置
- `--rabbitmq-url, -u`: RabbitMQ 连接 URL
- `--prefetch-count`: 预取消息数量（默认: 100）
- `--connection-timeout`: 连接超时时间（默认: 30秒）

#### Worker 配置
- `--worker-module, -m`: Worker 模块路径（必需）
- `--queue, -q`: 队列名称（默认: default）
- `--max-retries, -r`: 最大重试次数（默认: 3）
- `--job-timeout, -t`: 任务超时时间（默认: 300秒）
- `--max-concurrent-jobs`: 最大并发任务数（默认: 10）

#### Burst 模式配置
- `--burst, -b`: 启用 Burst 模式
- `--burst-timeout`: Burst 模式超时时间（默认: 300秒）
- `--burst-check-interval`: 队列检查间隔（默认: 1.0秒）
- `--burst-no-wait`: 不等待正在执行的任务完成

#### 日志配置
- `--log-level, -l`: 日志级别（DEBUG/INFO/WARNING/ERROR/CRITICAL）

### Worker 模块配置文件示例

创建 `myapp/workers.py`:

```python
from rabbitmq_arq import WorkerSettings, RabbitMQSettings

# 连接配置
rabbitmq_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/"
)

# Worker 配置
worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[process_data, send_email],  # 你的任务函数
    worker_name="production_worker",
    queue_name="default",
    max_retries=3,
    job_timeout=300,
)

# 或者，也可以直接导出函数列表
task_functions = [process_data, send_email]
```

然后使用：

```bash
# 使用 WorkerSettings
rabbitmq-arq worker -m myapp.workers:worker_settings

# 或使用函数列表
rabbitmq-arq worker -m myapp.workers:task_functions --queue my_queue
```

## 注意事项

1. **任务函数第一个参数必须是 `ctx: JobContext`**
2. **任务函数必须是可序列化的（不要使用 lambda 或闭包）**
3. **确保 RabbitMQ 服务正常运行**
4. **根据实际负载调整 `prefetch_count`**
5. **Burst 模式适用于批处理场景，常规业务建议使用标准模式**

## License

MIT 