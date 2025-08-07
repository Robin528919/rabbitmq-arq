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

# 定义任务（普通异步函数）
async def send_email(to: str, subject: str, body: str) -> bool:
    # 你的邮件发送逻辑
    print(f"发送邮件到 {to}: {subject}")
    await asyncio.sleep(1)  # 模拟异步操作
    return True

async def process_data(data: dict) -> dict:
    # 数据处理逻辑
    result = {"processed": True, "count": len(data)}
    return result
```

#### 发送任务

```python
import asyncio
from rabbitmq_arq import RabbitMQClient, create_client
from rabbitmq_arq.connections import RabbitMQSettings

async def main():
    # 创建客户端
    settings = RabbitMQSettings(connection_url="amqp://localhost:5672")
    client = RabbitMQClient(settings)
    
    # 连接并发送任务
    await client.connect()
    
    job = await client.enqueue_job(
        "send_email",  # 任务名称
        to="user@example.com",
        subject="欢迎使用 RabbitMQ ARQ",
        body="这是一个测试邮件"
    )
    
    print(f"任务已提交: {job.job_id}")
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

#### 启动工作器

```python
import asyncio
from rabbitmq_arq import Worker, WorkerSettings
from rabbitmq_arq.connections import RabbitMQSettings

async def main():
    # 配置设置
    rabbitmq_settings = RabbitMQSettings(
        connection_url="amqp://localhost:5672"
    )
    worker_settings = WorkerSettings(
        queues=["default"],
        prefetch_count=5000,  # 高并发处理
        max_workers=10
    )
    
    # 创建工作器
    worker = Worker(rabbitmq_settings, worker_settings)
    
    # 注册任务函数
    worker.add_function(send_email)
    worker.add_function(process_data)
    
    # 启动工作器
    await worker.async_run()

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

RabbitMQ-ARQ 具有智能错误分类和自动重试机制：

```python
import random
from rabbitmq_arq.exceptions import Retry

async def reliable_task(data: str) -> str:
    # 可能失败的任务，会自动重试
    if random.random() < 0.3:
        # 抛出 Retry 异常进行重试
        raise Retry("临时错误，需要重试")
    return f"处理完成: {data}"

# 工作器会自动根据错误类型决定是否重试
# - 网络错误、超时等：自动重试
# - 代码错误、类型错误等：不重试，直接失败
```

### 延迟任务

```python
import asyncio
from datetime import datetime, timedelta
from rabbitmq_arq import RabbitMQClient
from rabbitmq_arq.connections import RabbitMQSettings

async def main():
    settings = RabbitMQSettings(connection_url="amqp://localhost:5672")
    client = RabbitMQClient(settings)
    await client.connect()
    
    # 延迟执行（1小时后）
    job = await client.enqueue_job(
        "delayed_task",
        data={"message": "延迟任务"},
        defer_until=datetime.now() + timedelta(hours=1)
    )
    
    # 定时执行（指定时间）
    job = await client.enqueue_job(
        "scheduled_task", 
        data={"message": "定时任务"},
        defer_until=datetime(2025, 1, 1, 9, 0, 0)
    )
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## 性能优化

### 高并发配置

```python
from rabbitmq_arq import Worker, WorkerSettings
from rabbitmq_arq.connections import RabbitMQSettings

# 高性能配置
rabbitmq_settings = RabbitMQSettings(
    connection_url="amqp://localhost:5672"
)
worker_settings = WorkerSettings(
    queues=["high_performance"],
    prefetch_count=5000,     # 高预取数量
    max_workers=20,          # 增加并发工作器
    burst_check_interval=1.0, # 快速检查
    health_check_interval=30  # 健康检查间隔
)

worker = Worker(rabbitmq_settings, worker_settings)
```

### 批量任务提交

```python
import asyncio
from rabbitmq_arq import RabbitMQClient
from rabbitmq_arq.connections import RabbitMQSettings

async def main():
    settings = RabbitMQSettings(connection_url="amqp://localhost:5672")
    client = RabbitMQClient(settings)
    await client.connect()
    
    # 批量提交任务
    tasks = []
    for i in range(100):
        task = client.enqueue_job(
            "batch_task",
            item_id=i,
            data=f"batch_data_{i}"
        )
        tasks.append(task)
    
    # 等待所有任务提交完成
    jobs = await asyncio.gather(*tasks)
    print(f"提交了 {len(jobs)} 个任务")
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## 监控和日志

### 结构化日志

RabbitMQ-ARQ 内置中文友好的日志系统：

```python
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def logged_task(data: dict):
    logger = logging.getLogger('rabbitmq-arq.task')
    logger.info(f"任务开始处理: {data}")
    
    # 处理逻辑
    result = {"processed": True, "data": data}
    
    logger.info(f"任务处理完成: {result}")
    return result
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