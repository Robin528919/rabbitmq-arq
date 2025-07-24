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

## 安装

```bash
pip install aio-pika pydantic
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

### 2. 配置和运行 Worker

```python
from rabbitmq_arq import Worker, RabbitMQSettings

# 配置
settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="my_queue",
    max_retries=3,
    prefetch_count=100
)

# Worker 配置
class WorkerSettings:
    functions = [process_data]  # 注册任务函数
    rabbitmq_settings = settings

# 运行 Worker
Worker.run(WorkerSettings)
```

### 3. 提交任务

```python
import asyncio
from rabbitmq_arq import RabbitMQClient, RabbitMQSettings

async def main():
    # 创建客户端
    settings = RabbitMQSettings(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        rabbitmq_queue="my_queue"
    )
    client = RabbitMQClient(settings)
    
    # 提交任务
    job = await client.enqueue_job(
        "process_data",  # 函数名
        123,            # data_id
        "update"        # action
    )
    print(f"任务已提交: {job.job_id}")
    
    # 提交延迟任务
    job = await client.enqueue_job(
        "process_data",
        456,
        "delayed",
        _defer_by=60  # 60秒后执行
    )
    
    await client.close()

asyncio.run(main())
```

## 高级功能

### 生命周期钩子

```python
async def startup(ctx: dict):
    """Worker 启动时执行"""
    # 初始化数据库连接、HTTP 客户端等
    ctx['db'] = await create_db_connection()

async def shutdown(ctx: dict):
    """Worker 关闭时执行"""
    # 清理资源
    await ctx['db'].close()

async def on_job_start(ctx: dict):
    """每个任务开始前执行"""
    print(f"任务 {ctx['job_id']} 开始执行")

async def on_job_end(ctx: dict):
    """每个任务结束后执行"""
    print(f"任务 {ctx['job_id']} 执行结束")

class WorkerSettings:
    functions = [process_data]
    rabbitmq_settings = settings
    on_startup = startup
    on_shutdown = shutdown
    on_job_start = on_job_start
    on_job_end = on_job_end
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
        # 网络错误，稍后重试
        raise Retry(defer=30)
    except InvalidDataError:
        # 数据错误，使用指数退避重试
        raise Retry(defer=ctx.job_try * 10)
    except FatalError:
        # 致命错误，不再重试
        raise
    
    return result
```

## 配置选项

```python
settings = RabbitMQSettings(
    # 基础配置
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="my_queue",
    rabbitmq_dlq="my_queue_dlq",
    
    # 重试配置
    max_retries=3,              # 最大重试次数
    retry_backoff=5.0,          # 重试退避时间（秒）
    
    # 性能配置
    job_timeout=300,            # 任务超时时间（秒）
    prefetch_count=100,         # 预取消息数量
    
    # 其他配置
    enable_compression=False,   # 是否启用消息压缩
    health_check_interval=60,   # 健康检查间隔（秒）
    log_level="INFO"           # 日志级别
)
```

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

## 注意事项

1. **任务函数第一个参数必须是 `ctx: JobContext`**
2. **任务函数必须是可序列化的（不要使用 lambda 或闭包）**
3. **确保 RabbitMQ 服务正常运行**
4. **根据实际负载调整 `prefetch_count`**

## License

MIT 