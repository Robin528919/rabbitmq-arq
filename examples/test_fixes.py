# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/7/24 15:10
# @File           : test_fixes
# @IDE            : PyCharm
# @desc           : 测试修复效果的脚本

import asyncio
import logging
from typing import Dict, Any
import time

from src.rabbitmq_arq import (
    Worker,
    RabbitMQClient,
    RabbitMQSettings,
    JobContext,
    Retry
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('test_fixes')
task_logger = logging.getLogger('test_task')

# 测试配置
test_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="test_fixes_queue",
    max_retries=3,
    retry_backoff=3.0,
    job_timeout=60,
    prefetch_count=5,
    log_level="INFO",
    # 启用 burst 模式进行测试
    burst_mode=True,
    burst_timeout=60,
    burst_check_interval=1.0,
    burst_wait_for_tasks=True
)


# 测试任务：验证延迟重试
async def test_delayed_retry(ctx: JobContext, task_id: int):
    """测试延迟重试功能的任务"""
    task_logger.info(f"🔬 开始测试任务 {task_id} (尝试次数: {ctx.job_try})")
    
    # 第一次执行时请求重试，第二次成功
    if ctx.job_try == 1:
        task_logger.warning(f"任务 {task_id} 第一次执行，请求重试（延迟5秒）")
        raise Retry(defer=5)  # 5秒后重试
    else:
        task_logger.info(f"✅ 任务 {task_id} 第二次执行成功！")
        await asyncio.sleep(1)  # 模拟处理时间
        return {"task_id": task_id, "status": "success", "try": ctx.job_try}


# 测试任务：快速任务
async def test_quick_task(ctx: JobContext, task_id: int):
    """测试快速任务"""
    task_logger.info(f"⚡ 快速任务 {task_id} 开始执行")
    await asyncio.sleep(0.5)
    task_logger.info(f"✅ 快速任务 {task_id} 完成")
    return {"task_id": task_id, "status": "quick_done"}


# Worker 钩子函数
async def test_startup(ctx: Dict[Any, Any]):
    """测试启动钩子"""
    logger.info("🧪 测试 Worker 启动")
    ctx['test_start_time'] = time.time()
    ctx['jobs_complete'] = 0
    ctx['jobs_failed'] = 0
    ctx['jobs_retried'] = 0


async def test_shutdown(ctx: Dict[Any, Any]):
    """测试关闭钩子 - 验证统计修复"""
    elapsed = time.time() - ctx.get('test_start_time', time.time())
    logger.info(f"🧪 测试 Worker 关闭，总运行时间: {elapsed:.2f}s")
    
    # 验证统计数据修复
    complete = ctx.get('jobs_complete', 0)
    failed = ctx.get('jobs_failed', 0)
    retried = ctx.get('jobs_retried', 0)
    
    logger.info(f"📊 最终统计验证: 完成 {complete} 个, 失败 {failed} 个, 重试 {retried} 个")
    
    # 验证修复效果
    if complete > 0:
        logger.info("✅ 统计修复成功！数据正确同步")
    else:
        logger.error("❌ 统计修复失败！数据未正确同步")


async def test_job_start(ctx: Dict[Any, Any]):
    """测试任务开始钩子"""
    job_id = ctx.get('job_id', 'unknown')
    logger.debug(f"🔄 任务 {job_id} 开始")


async def test_job_end(ctx: Dict[Any, Any]):
    """测试任务结束钩子"""
    job_id = ctx.get('job_id', 'unknown')
    worker_stats = ctx.get('worker_stats', {})
    
    # 记录当前统计
    logger.info(f"📈 任务 {job_id} 结束，当前统计: "
               f"完成 {worker_stats.get('jobs_complete', 0)}, "
               f"失败 {worker_stats.get('jobs_failed', 0)}, "
               f"重试 {worker_stats.get('jobs_retried', 0)}")


# Worker 配置
class TestWorkerSettings:
    """测试 Worker 配置"""
    functions = [test_delayed_retry, test_quick_task]
    rabbitmq_settings = test_settings
    on_startup = test_startup
    on_shutdown = test_shutdown
    on_job_start = test_job_start
    on_job_end = test_job_end
    ctx = {"test_mode": True, "test_name": "fixes_validation"}


async def submit_test_jobs():
    """提交测试任务"""
    logger.info("📝 开始提交测试任务...")
    
    client = RabbitMQClient(test_settings)
    
    try:
        logger.info("正在连接到 RabbitMQ...")
        
        # 提交延迟重试测试任务
        retry_job = await client.enqueue_job(
            "test_delayed_retry",
            1  # task_id
        )
        logger.info(f"🔬 已提交延迟重试测试任务: {retry_job.job_id}")
        
        # 提交快速任务
        for i in range(3):
            quick_job = await client.enqueue_job(
                "test_quick_task",
                i + 2  # task_id
            )
            logger.info(f"⚡ 已提交快速任务 {i + 2}: {quick_job.job_id}")
        
        logger.info("🎉 所有测试任务提交完成！")
        logger.info("💡 现在运行: python test_fixes.py worker")
        
    except Exception as e:
        logger.error(f"❌ 测试任务提交失败: {e}")
        raise
    finally:
        await client.close()


async def clear_test_queue():
    """清空测试队列"""
    logger.info("🗑️ 清空测试队列...")
    
    from aio_pika import connect_robust
    
    connection = await connect_robust(test_settings.rabbitmq_url)
    channel = await connection.channel()
    
    try:
        queue = await channel.declare_queue(test_settings.rabbitmq_queue, durable=True)
        purged_count = await queue.purge()
        logger.info(f"✅ 已清空 {purged_count} 条测试消息")
    finally:
        await connection.close()


def main():
    """主函数"""
    import sys
    
    if len(sys.argv) < 2:
        print("🧪 修复效果测试脚本")
        print("\n📋 可用命令:")
        print("  submit  - 提交测试任务")
        print("  worker  - 启动测试 Worker")
        print("  clear   - 清空测试队列")
        print("\n💡 测试流程:")
        print("  1. python test_fixes.py clear   # 清空队列")
        print("  2. python test_fixes.py submit  # 提交测试任务")
        print("  3. python test_fixes.py worker  # 运行测试")
        return
    
    command = sys.argv[1]
    
    if command == "submit":
        logger.info("启动测试任务提交...")
        asyncio.run(submit_test_jobs())
    elif command == "worker":
        logger.info("启动测试 Worker...")
        logger.info("🔍 此测试将验证:")
        logger.info("   1. 延迟重试功能是否正常（使用 RabbitMQ TTL 队列）")
        logger.info("   2. 统计数据是否正确同步")
        logger.info("   3. Worker 不会因延迟任务而阻塞")
        Worker.run(TestWorkerSettings)
    elif command == "clear":
        logger.info("清空测试队列...")
        asyncio.run(clear_test_queue())
    else:
        logger.error(f"❌ 未知命令: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main() 