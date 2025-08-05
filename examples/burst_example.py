# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 22:00
# @File           : burst_example
# @IDE            : PyCharm
# @desc           : RabbitMQ-ARQ Burst 模式使用示例

import asyncio
import logging
from typing import Dict, Any
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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

logger = logging.getLogger('rabbitmq_arq.burst_example')

# RabbitMQ 连接配置
rabbitmq_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    prefetch_count=50,
    connection_timeout=30,
)


# === 任务函数定义 ===

async def simple_task(ctx: JobContext, task_id: int, message: str):
    """
    简单的任务函数，用于测试 Burst 模式
    """
    logger.info(f"📋 执行任务 {task_id}: {message}")
    logger.info(f"   任务ID: {ctx.job_id}")
    logger.info(f"   尝试次数: {ctx.job_try}")
    
    # 模拟任务处理时间
    await asyncio.sleep(0.5)
    
    logger.info(f"✅ 任务 {task_id} 完成")
    return {"task_id": task_id, "status": "completed", "message": message}


async def long_task(ctx: JobContext, duration: int):
    """
    长时间运行的任务，用于测试 Worker 等待机制
    """
    logger.info(f"⏳ 开始执行长任务，预计耗时 {duration} 秒")
    logger.info(f"   任务ID: {ctx.job_id}")
    
    for i in range(duration):
        await asyncio.sleep(1)
        logger.info(f"   长任务进度: {i+1}/{duration} 秒")
    
    logger.info(f"✅ 长任务完成，耗时 {duration} 秒")
    return {"duration": duration, "status": "completed"}


async def failing_task(ctx: JobContext, should_fail: bool = True):
    """
    会失败的任务，用于测试重试机制
    """
    logger.info(f"🎯 执行可能失败的任务")
    logger.info(f"   任务ID: {ctx.job_id}")
    logger.info(f"   尝试次数: {ctx.job_try}")
    
    await asyncio.sleep(0.2)
    
    if should_fail and ctx.job_try <= 2:
        logger.warning(f"💥 任务失败，准备重试 (尝试次数: {ctx.job_try})")
        raise Retry(defer=2)  # 2秒后重试
    
    logger.info(f"✅ 任务最终成功")
    return {"try_count": ctx.job_try, "status": "completed"}


# === 生命周期钩子 ===

async def burst_startup(ctx: dict):
    """Burst Worker 启动钩子"""
    logger.info("🚀 Burst Worker 启动中...")
    ctx['burst_stats'] = {
        'start_time': asyncio.get_event_loop().time(),
        'jobs_processed': 0,
        'jobs_completed': 0,
        'jobs_failed': 0
    }
    logger.info("✅ Burst Worker 准备就绪")


async def burst_shutdown(ctx: dict):
    """Burst Worker 关闭钩子"""
    logger.info("🏁 Burst Worker 正在关闭...")
    
    stats = ctx.get('burst_stats', {})
    start_time = stats.get('start_time', 0)
    current_time = asyncio.get_event_loop().time()
    runtime = current_time - start_time if start_time else 0
    
    logger.info("📊 Burst 运行统计:")
    logger.info(f"   运行时间: {runtime:.2f} 秒")
    logger.info(f"   处理任务: {stats.get('jobs_processed', 0)} 个")
    logger.info(f"   成功任务: {stats.get('jobs_completed', 0)} 个")
    logger.info(f"   失败任务: {stats.get('jobs_failed', 0)} 个")
    
    logger.info("✅ Burst Worker 已关闭")


async def job_start(ctx: dict):
    """任务开始钩子"""
    stats = ctx.get('burst_stats', {})
    stats['jobs_processed'] = stats.get('jobs_processed', 0) + 1


async def job_end(ctx: dict):
    """任务结束钩子"""
    stats = ctx.get('burst_stats', {})
    if ctx.get('job_status') == 'completed':
        stats['jobs_completed'] = stats.get('jobs_completed', 0) + 1
    elif ctx.get('job_status') == 'failed':
        stats['jobs_failed'] = stats.get('jobs_failed', 0) + 1


# === Worker 配置 ===

# Burst 模式 Worker 配置 - 快速处理
burst_worker_fast = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[simple_task, long_task, failing_task],
    worker_name="burst_worker_fast",
    
    # 队列配置
    queue_name="burst_test_queue",
    dlq_name="burst_test_queue_dlq",
    
    # 任务处理配置
    max_retries=3,
    retry_backoff=2.0,
    job_timeout=60,
    max_concurrent_jobs=5,
    
    # Burst 模式配置 - 快速退出
    burst_mode=True,
    burst_timeout=60,  # 1分钟超时
    burst_check_interval=0.5,  # 0.5秒检查一次
    burst_wait_for_tasks=False,  # 不等待任务完成
    burst_exit_on_empty=True,
    
    # 生命周期钩子
    on_startup=burst_startup,
    on_shutdown=burst_shutdown,
    on_job_start=job_start,
    on_job_end=job_end,
    
    # 日志配置
    log_level="INFO",
)

# Burst 模式 Worker 配置 - 等待任务完成
burst_worker_patient = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[simple_task, long_task, failing_task],
    worker_name="burst_worker_patient",
    
    # 队列配置
    queue_name="burst_test_queue",
    dlq_name="burst_test_queue_dlq",
    
    # 任务处理配置
    max_retries=3,
    retry_backoff=2.0,
    job_timeout=120,
    max_concurrent_jobs=3,
    
    # Burst 模式配置 - 等待任务完成
    burst_mode=True,
    burst_timeout=300,  # 5分钟超时
    burst_check_interval=1.0,  # 1秒检查一次
    burst_wait_for_tasks=True,  # 等待任务完成
    burst_exit_on_empty=True,
    
    # 生命周期钩子
    on_startup=burst_startup,
    on_shutdown=burst_shutdown,
    on_job_start=job_start,
    on_job_end=job_end,
    
    # 日志配置
    log_level="INFO",
)


# === 主函数：提交测试任务 ===

async def submit_test_tasks():
    """提交一批测试任务"""
    logger.info("📤 开始提交 Burst 模式测试任务")
    
    client = RabbitMQClient(rabbitmq_settings)
    
    try:
        await client.connect()
        logger.info("✅ 已连接到 RabbitMQ")
        
        # 提交快速任务
        logger.info("📋 提交快速任务...")
        for i in range(5):
            job = await client.enqueue_job(
                "simple_task",
                task_id=i + 1,
                message=f"快速任务 {i + 1}",
                queue_name="burst_test_queue"
            )
            logger.info(f"   ✅ 快速任务 {i + 1} 已提交: {job.job_id}")
        
        # 提交长时间任务
        logger.info("⏳ 提交长时间任务...")
        long_job = await client.enqueue_job(
            "long_task",
            duration=10,
            queue_name="burst_test_queue"
        )
        logger.info(f"   ✅ 长时间任务已提交: {long_job.job_id}")
        
        # 提交会失败重试的任务
        logger.info("💥 提交失败重试任务...")
        for i in range(2):
            fail_job = await client.enqueue_job(
                "failing_task",
                should_fail=True,
                queue_name="burst_test_queue"
            )
            logger.info(f"   ✅ 失败重试任务 {i + 1} 已提交: {fail_job.job_id}")
        
        logger.info("🎉 所有测试任务已提交完成")
        logger.info("   快速任务: 5 个")
        logger.info("   长时间任务: 1 个")
        logger.info("   失败重试任务: 2 个")
        logger.info("")
        logger.info("💡 接下来你可以:")
        logger.info("   python burst_example.py fast      # 启动快速 Burst Worker (不等待)")
        logger.info("   python burst_example.py patient   # 启动耐心 Burst Worker (等待完成)")
        
    except Exception as e:
        logger.error(f"❌ 任务提交失败: {e}")
        raise
    finally:
        await client.close()
        logger.info("客户端连接已关闭")


async def run_burst_worker(worker_type: str):
    """运行指定类型的 Burst Worker"""
    if worker_type == "fast":
        logger.info("🚀 启动快速 Burst Worker (不等待任务完成)")
        worker = Worker(burst_worker_fast)
    elif worker_type == "patient":
        logger.info("🚀 启动耐心 Burst Worker (等待任务完成)")
        worker = Worker(burst_worker_patient)
    else:
        raise ValueError(f"不支持的 Worker 类型: {worker_type}")
    
    await worker.main()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "fast":
            # 运行快速 Burst Worker
            asyncio.run(run_burst_worker("fast"))
            
        elif command == "patient":
            # 运行耐心 Burst Worker
            asyncio.run(run_burst_worker("patient"))
            
        else:
            logger.error(f"❌ 未知命令: {command}")
            logger.info("💡 可用命令:")
            logger.info("  python burst_example.py          # 提交测试任务")
            logger.info("  python burst_example.py fast     # 启动快速 Burst Worker")
            logger.info("  python burst_example.py patient  # 启动耐心 Burst Worker")
    else:
        # 提交测试任务
        logger.info("启动任务提交模式...")
        asyncio.run(submit_test_tasks()) 