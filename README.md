# RabbitMQ ARQ

一个基于 RabbitMQ 的异步任务队列库，提供类似 [arq](https://github.com/samuelcolvin/arq) 的简洁 API。

## 特性

- 🚀 **高性能**: 支持 ≥5000 消息/秒的处理能力
- 🎯 **简洁 API**: 类似 arq 的装饰器风格，易于使用
- 🔧 **易于迁移**: 提供从现有 Consumer 迁移的工具
- 🌐 **中文友好**: 支持中文日志输出
- 🔄 **高可用**: 内置重试机制和错误处理
- 📊 **监控支持**: 集成监控指标收集

## 快速开始

### 安装

```bash
pip install rabbitmq-arq
```

### 基本使用

#### 定义任务

```python
import asyncio
from rabbitmq_arq import ArqClient, task

# 定义任务
@task
async def send_email(to: str, subject: str, body: str) -> bool:
    # 你的邮件发送逻辑
    print(f"发送邮件到 {to}: {subject}")
    await asyncio.sleep(1)  # 模拟异步操作
    return True

@task
async def process_data(data: dict) -> dict:
    # 数据处理逻辑
    result = {"processed": True, "count": len(data)}
    return result
```

#### 发送任务

```python
import asyncio
from rabbitmq_arq import ArqClient

async def main():
    # 创建客户端
    client = ArqClient("amqp://localhost:5672")
    
    # 入队任务
    job = await client.enqueue(
        "send_email",
        to="user@example.com",
        subject="欢迎使用 RabbitMQ ARQ",
        body="这是一个测试邮件"
    )
    
    print(f"任务已提交: {job.id}")

if __name__ == "__main__":
    asyncio.run(main())
```

#### 启动工作器

```python
import asyncio
from rabbitmq_arq import Worker

async def main():
    worker = Worker(
        connection_url="amqp://localhost:5672",
        queues=["default"],
        prefetch_count=5000,  # 高并发处理
        max_workers=10
    )
    
    # 注册任务
    worker.register_task(send_email)
    worker.register_task(process_data)
    
    # 启动工作器
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
```

### 命令行工具

```bash
# 启动工作器
rabbitmq-arq worker --connection amqp://localhost:5672 --queues default --workers 10

# 监控队列状态
rabbitmq-arq monitor --connection amqp://localhost:5672
```

## 高级特性

### 错误处理和重试

```python
import random

@task(max_retries=3, retry_delay=60)
async def reliable_task(data: str) -> str:
    # 可能失败的任务，会自动重试
    if random.random() < 0.3:
        raise Exception("随机错误")
    return f"处理完成: {data}"
```

### 任务优先级

```python
import asyncio
from rabbitmq_arq import ArqClient

async def main():
    client = ArqClient("amqp://localhost:5672")
    
    # 高优先级任务
    await client.enqueue("urgent_task", priority=10)

    # 普通任务
    await client.enqueue("normal_task", priority=1)

if __name__ == "__main__":
    asyncio.run(main())
```

### 定时任务

```python
from datetime import datetime, timedelta

import asyncio
from rabbitmq_arq import ArqClient

async def main():
    client = ArqClient("amqp://localhost:5672")
    
    # 延迟执行
    await client.enqueue("delayed_task", defer_until=datetime.now() + timedelta(hours=1))

    # 定时执行
    await client.enqueue("scheduled_task", defer_until=datetime(2024, 1, 1, 9, 0, 0))

if __name__ == "__main__":
    asyncio.run(main())
```

## 从现有系统迁移

如果你正在使用自定义的 Consumer 类，可以轻松迁移到 rabbitmq-arq：

```python
# 旧代码
class FollowersConsumer(BaseConsumer):
    async def process_message(self, data):
        # 处理逻辑
        pass

# 新代码
@task
async def process_followers(data: dict):
    # 相同的处理逻辑
    pass
```

详细迁移指南请参考 [examples/migration_from_existing.py](examples/migration_from_existing.py)。

## 性能优化

### 批量处理

```python
import asyncio
from rabbitmq_arq import ArqClient, task

async def main():
    client = ArqClient("amqp://localhost:5672")
    
    # 批量处理
    await client.enqueue_many(
        [
            ("batch_task_1", {"item": "data1"}),
            ("batch_task_2", {"item": "data2"}),
            ("batch_task_3", {"item": "data3"}),
        ]
    )

if __name__ == "__main__":
    asyncio.run(main())
```

### 连接池配置

```python
import asyncio
from rabbitmq_arq import ArqClient

async def main():
    client = ArqClient(
        connection_url="amqp://localhost:5672",
        pool_size=20,
        max_overflow=30
    )

if __name__ == "__main__":
    asyncio.run(main())
```

## 监控和日志

### 结构化日志

```python
import structlog

logger = structlog.get_logger()

@task
async def logged_task(data: dict):
    logger.info("任务开始", task_data=data)
    # 处理逻辑
    logger.info("任务完成", result="success")
```

### 监控指标

rabbitmq-arq 自动收集以下指标：

- 任务执行时间
- 成功/失败率
- 队列长度
- 工作器状态

## 开发

### 环境设置

```bash
# 克隆仓库
git clone https://github.com/your-username/rabbitmq-arq.git
cd rabbitmq-arq

# 安装开发依赖
pip install -e ".[dev]"

# 启动 RabbitMQ (使用 Docker)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### 运行测试

```bash
# 运行所有测试
pytest

# 运行带覆盖率的测试
pytest --cov=rabbitmq_arq

# 运行性能测试
pytest -m performance
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

- `RABBITMQ_URL`: RabbitMQ 连接 URL (默认: `amqp://localhost:5672`)
- `ARQ_LOG_LEVEL`: 日志级别 (默认: `INFO`)
- `ARQ_MAX_WORKERS`: 最大工作器数量 (默认: `10`)
- `ARQ_PREFETCH_COUNT`: 预取消息数量 (默认: `5000`)

### 配置文件

```yaml
# config.yaml
rabbitmq:
  url: "amqp://localhost:5672"
  prefetch_count: 5000
  
worker:
  max_workers: 10
  queues: ["default", "high_priority"]
  
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

### v0.1.0

- 初始版本发布
- 基本的任务队列功能
- 装饰器风格的任务定义
- 高性能工作器实现
- 中文日志支持 