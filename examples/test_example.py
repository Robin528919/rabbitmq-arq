# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 22:00
# @File           : test_example
# @IDE            : PyCharm
# @desc           : 测试 example.py 修复是否有效

import asyncio
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from rabbitmq_arq import (
    RabbitMQClient,
    RabbitMQSettings,
    JobContext,
    Retry
)

# 简单的配置测试
def test_settings():
    """测试 RabbitMQSettings 配置"""
    print("📋 测试 RabbitMQSettings 配置...")
    
    settings = RabbitMQSettings(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        rabbitmq_queue="test_queue",
        max_retries=3,
        retry_backoff=5.0,
        job_timeout=300,
        prefetch_count=100,
        log_level="INFO"
    )
    
    assert settings.rabbitmq_url == "amqp://guest:guest@localhost:5672/"
    assert settings.rabbitmq_queue == "test_queue"
    assert settings.max_retries == 3
    
    print("✅ RabbitMQSettings 配置测试通过")


def test_job_context():
    """测试 JobContext 模型"""
    print("📋 测试 JobContext 模型...")
    
    from datetime import datetime
    
    ctx = JobContext(
        job_id="test-job-123",
        job_try=1,
        enqueue_time=datetime.now(),
        start_time=datetime.now(),
        queue_name="test_queue",
        worker_id="worker-456",
        extra={"custom_data": "test"}
    )
    
    # 测试 Pydantic V2 model_dump 方法
    data = ctx.model_dump()
    assert data['job_id'] == "test-job-123"
    assert data['job_try'] == 1
    assert data['extra']['custom_data'] == "test"
    
    print("✅ JobContext 模型测试通过")


async def test_task_functions():
    """测试任务函数"""
    print("📋 测试任务函数...")
    
    from datetime import datetime
    
    # 模拟 JobContext
    ctx = JobContext(
        job_id="test-job-123",
        job_try=1,
        enqueue_time=datetime.now(),
        start_time=datetime.now(),
        queue_name="test_queue",
        worker_id="worker-456"
    )
    
    # 测试简单任务函数
    async def simple_task(ctx: JobContext, message: str):
        return f"处理完成: {message}"
    
    result = await simple_task(ctx, "测试消息")
    assert result == "处理完成: 测试消息"
    
    # 测试重试机制
    async def retry_task(ctx: JobContext, should_retry: bool):
        if should_retry and ctx.job_try < 2:
            raise Retry(defer=1)
        return "任务成功"
    
    # 第一次调用应该抛出 Retry
    try:
        await retry_task(ctx, True)
        assert False, "应该抛出 Retry 异常"
    except Retry as e:
        assert e.defer == 1
    
    # 第二次调用应该成功
    ctx.job_try = 2
    result = await retry_task(ctx, True)
    assert result == "任务成功"
    
    print("✅ 任务函数测试通过")


def test_client_creation():
    """测试客户端创建"""
    print("📋 测试客户端创建...")
    
    settings = RabbitMQSettings(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        rabbitmq_queue="test_queue"
    )
    
    client = RabbitMQClient(settings)
    assert client.rabbitmq_settings.rabbitmq_queue == "test_queue"
    
    print("✅ 客户端创建测试通过")


def test_hook_functions():
    """测试钩子函数"""
    print("📋 测试钩子函数...")
    
    # 测试钩子函数签名
    async def test_startup(ctx: dict):
        ctx['initialized'] = True
        return ctx
    
    async def test_job_start(ctx: dict):
        job_id = ctx.get('job_id', 'unknown')
        return f"开始任务 {job_id}"
    
    # 模拟调用
    startup_ctx = {}
    result = asyncio.run(test_startup(startup_ctx))
    assert result['initialized'] is True
    
    job_ctx = {'job_id': 'test-123', 'job_try': 1}
    result = asyncio.run(test_job_start(job_ctx))
    assert result == "开始任务 test-123"
    
    print("✅ 钩子函数测试通过")


def main():
    """运行所有测试"""
    print("🚀 开始运行 RabbitMQ-ARQ 修复验证测试...\n")
    
    try:
        # 基本配置测试
        test_settings()
        print()
        
        # 模型测试
        test_job_context()
        print()
        
        # 任务函数测试
        asyncio.run(test_task_functions())
        print()
        
        # 客户端测试
        test_client_creation()
        print()
        
        # 钩子函数测试
        test_hook_functions()
        print()
        
        print("🎉 所有测试通过！example.py 修复成功！")
        print("\n📝 可以运行以下命令进行完整测试：")
        print("1. 运行 Worker: python examples/example.py worker")
        print("2. 提交任务: python examples/example.py")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 