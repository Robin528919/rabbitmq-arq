# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 21:00
# @File           : test_example
# @IDE            : PyCharm
# @desc           : 测试 RabbitMQ-ARQ 修复效果

import asyncio
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.rabbitmq_arq import (
    Worker,
    WorkerSettings,
    RabbitMQClient,
    RabbitMQSettings,
    JobContext,
    Retry
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger('test_example')

# RabbitMQ 连接配置
rabbitmq_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    prefetch_count=10,
    connection_timeout=30,
)


# === 测试任务函数 ===

async def test_basic_task(ctx: JobContext, task_name: str, data: dict):
    """基础任务测试"""
    logger.info(f"🔬 执行基础任务: {task_name}")
    logger.info(f"   任务ID: {ctx.job_id}")
    logger.info(f"   数据: {data}")
    
    await asyncio.sleep(0.5)
    
    logger.info(f"✅ 基础任务 {task_name} 完成")
    return {"task_name": task_name, "status": "completed", "data": data}


async def test_retry_task(ctx: JobContext, retry_count: int = 2):
    """重试任务测试"""
    logger.info(f"🔄 执行重试任务测试")
    logger.info(f"   任务ID: {ctx.job_id}")
    logger.info(f"   当前尝试: {ctx.job_try}")
    logger.info(f"   预期重试: {retry_count} 次")
    
    if ctx.job_try <= retry_count:
        logger.warning(f"💥 任务失败，进行重试 ({ctx.job_try}/{retry_count})")
        raise Retry(defer=1)  # 1秒后重试
    
    logger.info(f"✅ 重试任务最终成功")
    return {"retry_count": ctx.job_try - 1, "status": "completed"}


async def test_delayed_task(ctx: JobContext, message: str):
    """延迟任务测试"""
    logger.info(f"⏰ 执行延迟任务: {message}")
    logger.info(f"   任务ID: {ctx.job_id}")
    
    await asyncio.sleep(0.2)
    
    logger.info(f"✅ 延迟任务完成: {message}")
    return {"message": message, "status": "completed"}


# === 生命周期钩子 ===

async def test_startup(ctx: dict):
    """测试启动钩子"""
    logger.info("🚀 测试 Worker 启动中...")
    ctx['test_stats'] = {
        'start_time': asyncio.get_event_loop().time(),
        'jobs_processed': 0,
        'jobs_completed': 0,
        'jobs_failed': 0,
        'jobs_retried': 0
    }
    logger.info("✅ 测试 Worker 准备就绪")


async def test_shutdown(ctx: dict):
    """测试关闭钩子"""
    logger.info("🛑 测试 Worker 正在关闭...")
    
    stats = ctx.get('test_stats', {})
    start_time = stats.get('start_time', 0)
    current_time = asyncio.get_event_loop().time()
    runtime = current_time - start_time if start_time else 0
    
    logger.info("📊 测试运行统计:")
    logger.info(f"   运行时间: {runtime:.2f} 秒")
    logger.info(f"   处理任务: {stats.get('jobs_processed', 0)} 个")
    logger.info(f"   成功任务: {stats.get('jobs_completed', 0)} 个")
    logger.info(f"   失败任务: {stats.get('jobs_failed', 0)} 个")
    logger.info(f"   重试任务: {stats.get('jobs_retried', 0)} 个")
    
    logger.info("✅ 测试 Worker 已关闭")


async def job_start_hook(ctx: dict):
    """任务开始钩子"""
    stats = ctx.get('test_stats', {})
    stats['jobs_processed'] = stats.get('jobs_processed', 0) + 1


async def job_end_hook(ctx: dict):
    """任务结束钩子"""
    stats = ctx.get('test_stats', {})
    job_status = ctx.get('job_status')
    
    if job_status == 'completed':
        stats['jobs_completed'] = stats.get('jobs_completed', 0) + 1
    elif job_status == 'failed':
        stats['jobs_failed'] = stats.get('jobs_failed', 0) + 1
    elif job_status == 'retried':
        stats['jobs_retried'] = stats.get('jobs_retried', 0) + 1


# === Worker 配置 ===

# 测试 Worker 配置
test_worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[test_basic_task, test_retry_task, test_delayed_task],
    worker_name="test_worker",
    
    # 队列配置
    queue_name="test_queue",
    dlq_name="test_queue_dlq",
    
    # 任务处理配置
    max_retries=3,
    retry_backoff=1.0,
    job_timeout=30,
    max_concurrent_jobs=3,
    
    # Burst 模式配置（用于测试）
    burst_mode=True,
    burst_timeout=60,
    burst_check_interval=1.0,
    burst_wait_for_tasks=True,
    
    # 生命周期钩子
    on_startup=test_startup,
    on_shutdown=test_shutdown,
    on_job_start=job_start_hook,
    on_job_end=job_end_hook,
    
    # 日志配置
    log_level="INFO",
)


# === 测试函数 ===

async def test_basic_functionality():
    """测试基本功能"""
    logger.info("🧪 开始基本功能测试")
    
    client = RabbitMQClient(rabbitmq_settings)
    
    try:
        await client.connect()
        logger.info("✅ 客户端连接成功")
        
        # 测试基础任务提交
        job1 = await client.enqueue_job(
            "test_basic_task",
            task_name="基础测试",
            data={"test": True, "number": 123},
            queue_name="test_queue"
        )
        logger.info(f"✅ 基础任务已提交: {job1.job_id}")
        
        # 测试重试任务
        job2 = await client.enqueue_job(
            "test_retry_task",
            retry_count=2,
            queue_name="test_queue"
        )
        logger.info(f"✅ 重试任务已提交: {job2.job_id}")
        
        # 测试延迟任务
        job3 = await client.enqueue_job(
            "test_delayed_task",
            message="这是一个延迟3秒的任务",
            queue_name="test_queue",
            _defer_by=3
        )
        logger.info(f"✅ 延迟任务已提交: {job3.job_id}")
        
        logger.info("🎉 所有测试任务已提交")
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        raise
    finally:
        await client.close()
        logger.info("客户端连接已关闭")


async def run_test_worker():
    """运行测试 Worker"""
    logger.info("🚀 启动测试 Worker")
    worker = Worker(test_worker_settings)
    await worker.main()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "worker":
            # 运行测试 Worker
            asyncio.run(run_test_worker())
        else:
            logger.error(f"❌ 未知命令: {command}")
            logger.info("💡 可用命令:")
            logger.info("  python test_example.py        # 提交测试任务")
            logger.info("  python test_example.py worker # 启动测试 Worker")
    else:
        # 提交测试任务
        logger.info("启动测试模式...")
        asyncio.run(test_basic_functionality()) 