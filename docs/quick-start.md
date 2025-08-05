# RabbitMQ-ARQ 快速开始指南

## 🚀 安装

### 从 PyPI 安装（推荐）

```bash
# 安装最新版本
pip install rabbitmq-arq

# 安装指定版本
pip install rabbitmq-arq==0.1.0

# 安装开发依赖
pip install rabbitmq-arq[dev]

# 安装额外功能
pip install rabbitmq-arq[redis,mongodb]
```

### 从源码安装

```bash
# 克隆仓库
git clone https://github.com/your-username/rabbitmq-arq.git
cd rabbitmq-arq

# 安装
pip install -e .

# 或者安装开发版本
pip install -e .[dev]
```

## 📋 环境要求

- **Python**: 3.8+
- **RabbitMQ**: 3.8+
- **操作系统**: Linux, macOS, Windows

### RabbitMQ 安装

```bash
# macOS (使用 Homebrew)
brew install rabbitmq
brew services start rabbitmq

# Ubuntu/Debian
sudo apt-get install rabbitmq-server
sudo systemctl start rabbitmq-server

# CentOS/RHEL
sudo yum install rabbitmq-server
sudo systemctl start rabbitmq-server

# Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## 🎯 快速示例

### 1. 定义任务

```python
# tasks.py
import asyncio
from rabbitmq_arq import JobContext

async def send_email(ctx: JobContext, to: str, subject: str, body: str):
    """发送邮件任务"""
    print(f"📧 发送邮件到 {to}")
    print(f"📋 主题: {subject}")
    print(f"📝 内容: {body}")
    
    # 模拟发送邮件
    await asyncio.sleep(1)
    
    return {"status": "sent", "to": to, "message_id": f"msg_{ctx.job_id[:8]}"}

async def process_data(ctx: JobContext, data: dict):
    """数据处理任务"""
    print(f"📊 处理数据: {data}")
    
    # 模拟数据处理
    await asyncio.sleep(2)
    
    return {"processed_items": len(data), "job_id": ctx.job_id}
```

### 2. 提交任务

```python
# submit_jobs.py
import asyncio
from rabbitmq_arq import RabbitMQClient, RabbitMQSettings

async def main():
    # 创建设置
    settings = RabbitMQSettings(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        rabbitmq_queue="my_queue"
    )
    
    # 创建客户端
    client = RabbitMQClient(settings)
    
    try:
        # 提交邮件任务
        email_job = await client.enqueue_job(
            "send_email",
            "user@example.com",
            "欢迎使用 RabbitMQ-ARQ",
            "这是一个测试邮件"
        )
        print(f"✅ 邮件任务已提交: {email_job.job_id}")
        
        # 提交数据处理任务
        data_job = await client.enqueue_job(
            "process_data",
            {"users": 100, "orders": 50}
        )
        print(f"✅ 数据任务已提交: {data_job.job_id}")
        
        # 提交延迟任务
        delayed_job = await client.enqueue_job(
            "send_email",
            "admin@example.com",
            "定时报告",
            "这是一个延迟任务",
            _defer_by=60  # 60秒后执行
        )
        print(f"✅ 延迟任务已提交: {delayed_job.job_id}")
        
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### 3. 运行 Worker

```python
# worker.py
from rabbitmq_arq import Worker, RabbitMQSettings
from tasks import send_email, process_data

# Worker 设置
class WorkerSettings:
    functions = [send_email, process_data]
    rabbitmq_settings = RabbitMQSettings(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        rabbitmq_queue="my_queue",
        prefetch_count=10
    )

if __name__ == "__main__":
    Worker.run(WorkerSettings)
```

### 4. 运行

```bash
# 终端 1: 启动 Worker
python worker.py

# 终端 2: 提交任务
python submit_jobs.py
```

## 🔧 命令行工具

### 启动 Worker

```bash
# 基本用法
rabbitmq-arq worker \
  --rabbitmq-url amqp://guest:guest@localhost:5672/ \
  --queue my_queue \
  --worker-settings worker.WorkerSettings

# Burst 模式（处理完任务后退出）
rabbitmq-arq worker \
  --worker-settings worker.WorkerSettings \
  --burst \
  --burst-timeout 300

# 显示详细信息
rabbitmq-arq worker \
  --worker-settings worker.WorkerSettings \
  --log-level DEBUG
```

### 队列管理

```bash
# 查看队列信息
rabbitmq-arq queue-info --queue my_queue

# 清空队列
rabbitmq-arq purge-queue --queue my_queue

# 验证配置
rabbitmq-arq validate-config --worker-settings worker.WorkerSettings
```

## 🎛️ 高级配置

### Worker 配置

```python
from rabbitmq_arq import RabbitMQSettings

settings = RabbitMQSettings(
    # 连接设置
    rabbitmq_url="amqp://user:pass@localhost:5672/vhost",
    rabbitmq_queue="my_queue",
    
    # 性能设置
    prefetch_count=100,          # 预取消息数
    max_concurrent_jobs=10,      # 最大并发任务数
    
    # 重试设置
    max_retries=3,              # 最大重试次数
    retry_backoff=5.0,          # 重试延迟（秒）
    
    # 超时设置
    job_timeout=300,            # 任务超时（秒）
    job_completion_wait=5.0,    # 任务完成等待时间
    
    # Burst 模式
    burst_mode=True,            # 启用 burst 模式
    burst_timeout=300,          # Burst 超时时间
    burst_wait_for_tasks=True,  # 等待任务完成
    
    # 日志设置
    log_level="INFO"            # 日志级别
)
```

### 生命周期钩子

```python
async def startup(ctx):
    """Worker 启动时执行"""
    print("🚀 Worker 启动中...")
    # 初始化数据库连接、HTTP 客户端等
    ctx['db'] = await create_db_connection()
    ctx['http'] = create_http_client()

async def shutdown(ctx):
    """Worker 关闭时执行"""
    print("🛑 Worker 关闭中...")
    # 清理资源
    await ctx['db'].close()
    await ctx['http'].close()

async def job_start(ctx):
    """每个任务开始前执行"""
    print(f"▶️ 任务 {ctx['job_id']} 开始")

async def job_end(ctx):
    """每个任务结束后执行"""
    print(f"✅ 任务 {ctx['job_id']} 结束")

class WorkerSettings:
    functions = [send_email, process_data]
    rabbitmq_settings = settings
    on_startup = startup
    on_shutdown = shutdown
    on_job_start = job_start
    on_job_end = job_end
```

### 错误处理和重试

```python
from rabbitmq_arq import JobContext, Retry

async def unreliable_task(ctx: JobContext, url: str):
    """可能失败的任务"""
    try:
        result = await fetch_data(url)
        return result
    except NetworkError:
        # 网络错误，30秒后重试
        raise Retry(defer=30)
    except RateLimitError:
        # 频率限制，使用指数退避
        delay = ctx.job_try * 60  # 1分钟、2分钟、3分钟...
        raise Retry(defer=delay)
    except DataError:
        # 数据错误，不重试
        raise
```

## 📊 监控和日志

### 日志配置

```python
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('worker.log')
    ]
)

# 设置特定模块的日志级别
logging.getLogger('rabbitmq-arq.worker').setLevel(logging.DEBUG)
logging.getLogger('rabbitmq-arq.client').setLevel(logging.INFO)
```

### 统计信息

Worker 会自动收集以下统计信息：

- 📊 **完成任务数**: `jobs_complete`
- ❌ **失败任务数**: `jobs_failed` 
- 🔄 **重试任务数**: `jobs_retried`
- ⏳ **运行中任务数**: `jobs_ongoing`

在钩子函数中可以访问这些统计信息：

```python
async def job_end(ctx):
    stats = ctx.get('worker_stats', {})
    print(f"📊 统计: 完成 {stats.get('jobs_complete', 0)} 个任务")
```

## 🔗 集成示例

### 与 FastAPI 集成

```python
from fastapi import FastAPI
from rabbitmq_arq import RabbitMQClient, RabbitMQSettings

app = FastAPI()

# 创建全局客户端
client = RabbitMQClient(RabbitMQSettings())

@app.on_event("startup")
async def startup():
    await client.connect()

@app.on_event("shutdown") 
async def shutdown():
    await client.close()

@app.post("/send-email")
async def send_email_endpoint(to: str, subject: str, body: str):
    job = await client.enqueue_job("send_email", to, subject, body)
    return {"job_id": job.job_id, "status": "queued"}
```

### 与 Django 集成

```python
# settings.py
RABBITMQ_ARQ_SETTINGS = {
    'rabbitmq_url': 'amqp://guest:guest@localhost:5672/',
    'rabbitmq_queue': 'django_queue',
}

# tasks.py  
from django.core.mail import send_mail
from rabbitmq_arq import JobContext

async def django_send_email(ctx: JobContext, subject: str, message: str, recipient: str):
    """Django 邮件任务"""
    from django.core.mail import send_mail
    send_mail(subject, message, 'from@example.com', [recipient])
    return {"status": "sent"}

# views.py
from django.http import JsonResponse
from .utils import get_rabbitmq_client

async def queue_email(request):
    client = await get_rabbitmq_client()
    job = await client.enqueue_job(
        "django_send_email",
        "Welcome",
        "Thank you for signing up!",
        "user@example.com"
    )
    return JsonResponse({"job_id": job.job_id})
```

## 📚 更多资源

- 📖 [完整文档](https://rabbitmq-arq.readthedocs.io)
- 🔧 [API 参考](https://rabbitmq-arq.readthedocs.io/api)
- 💡 [示例代码](https://github.com/your-username/rabbitmq-arq/tree/main/examples)
- 🐛 [问题反馈](https://github.com/your-username/rabbitmq-arq/issues)

## ❓ 常见问题

### Q: 如何处理大文件上传任务？
A: 使用分块处理和进度回调：

```python
async def process_large_file(ctx: JobContext, file_path: str):
    total_chunks = get_file_chunks(file_path)
    for i, chunk in enumerate(process_file_chunks(file_path)):
        await process_chunk(chunk)
        # 更新进度（可选）
        progress = (i + 1) / total_chunks * 100
        print(f"进度: {progress:.1f}%")
```

### Q: 如何实现任务优先级？
A: 使用不同的队列：

```python
# 高优先级队列
high_priority_settings = RabbitMQSettings(rabbitmq_queue="high_priority")
high_client = RabbitMQClient(high_priority_settings)

# 普通队列  
normal_client = RabbitMQClient(RabbitMQSettings(rabbitmq_queue="normal"))

# 提交到不同队列
await high_client.enqueue_job("urgent_task", data)
await normal_client.enqueue_job("normal_task", data)
```

### Q: 如何监控任务队列状态？
A: 使用 RabbitMQ 管理界面或命令行工具：

```bash
# Web 界面 (默认: http://localhost:15672)
rabbitmq-plugins enable rabbitmq_management

# 命令行查看队列
rabbitmqctl list_queues name messages consumers

# 使用 CLI 工具
rabbitmq-arq queue-info --queue my_queue
```

---

**🎉 恭喜！您已经掌握了 RabbitMQ-ARQ 的基本使用方法。开始构建您的异步任务系统吧！** 