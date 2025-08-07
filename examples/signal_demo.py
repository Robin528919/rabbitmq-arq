#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
信号处理演示脚本

用于测试Worker的优雅关闭机制，展示不同信号的处理效果。
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

# 添加 src 目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rabbitmq_arq import Worker, WorkerSettings, RabbitMQSettings
from rabbitmq_arq.models import JobContext

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# RabbitMQ 连接配置
rabbitmq_settings = RabbitMQSettings(
    host="localhost",
    port=5672,
    username="guest",
    password="guest",
    vhost="/",
    prefetch_count=10
)

async def simple_task(ctx: JobContext, message: str = "Hello"):
    """简单的测试任务"""
    logger.info(f"🔧 执行任务 {ctx.job_id}: {message}")
    await asyncio.sleep(2)  # 模拟工作
    logger.info(f"✅ 任务 {ctx.job_id} 完成")
    return f"Task completed: {message}"

async def long_task(ctx: JobContext, duration: int = 10):
    """长时间运行的任务，用于测试信号处理"""
    logger.info(f"🔧 开始执行长任务 {ctx.job_id} (持续 {duration} 秒)")
    
    for i in range(duration):
        await asyncio.sleep(1)
        logger.info(f"⏳ 任务 {ctx.job_id} 进度: {i+1}/{duration}")
    
    logger.info(f"✅ 长任务 {ctx.job_id} 完成")
    return f"Long task completed after {duration} seconds"

# Worker 配置
worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[simple_task, long_task],
    worker_name="signal_demo_worker",
    queue_name="signal_test_queue",
    dlq_name="signal_test_queue.dlq",
    max_retries=2,
    job_timeout=30,
    wait_for_job_completion_on_signal_second=15,  # 信号处理等待时间
)

def print_usage():
    """打印使用说明"""
    print()
    print("🚀 信号处理演示脚本")
    print("=" * 50)
    print("用法:")
    print("  python signal_demo.py worker    # 启动 Worker")
    print("  python signal_demo.py client    # 提交测试任务")
    print()
    print("📋 信号测试说明:")
    print("  Ctrl+C        → 发送 SIGINT (优雅关闭)")
    print("  kill -TERM    → 发送 SIGTERM (优雅关闭)")
    print("  kill -KILL    → 发送 SIGKILL (强制终止，不可捕获)")
    print()
    print("💡 推荐的停止方式:")
    print("  1. 在终端按 Ctrl+C")
    print("  2. 或在另一个终端执行: kill -TERM <worker_pid>")
    print("  3. 避免使用: kill -9 <worker_pid> 或 IDE 的强制停止")
    print()

async def submit_test_tasks():
    """提交测试任务"""
    from rabbitmq_arq import RabbitMQClient
    
    client = RabbitMQClient(rabbitmq_settings)
    
    try:
        await client.connect()
        
        logger.info("📤 提交测试任务...")
        
        # 提交几个简单任务
        for i in range(3):
            job = await client.enqueue_job(
                "simple_task",
                message=f"任务{i+1}",
                queue_name="signal_test_queue"
            )
            logger.info(f"   ✅ 简单任务 {job.job_id} 已提交")
        
        # 提交一个长任务
        long_job = await client.enqueue_job(
            "long_task", 
            duration=15,
            queue_name="signal_test_queue"
        )
        logger.info(f"   ✅ 长任务 {long_job.job_id} 已提交 (15秒)")
        
        logger.info("✅ 所有测试任务已提交")
        logger.info("💡 现在可以启动 Worker 并测试信号处理")
        
    finally:
        await client.close()

async def run_worker():
    """运行Worker"""
    logger.info("🚀 启动信号处理演示 Worker...")
    
    # 显示进程ID，方便测试kill命令
    import os
    pid = os.getpid()
    logger.info(f"📍 Worker 进程ID: {pid}")
    logger.info(f"🔧 测试命令: kill -TERM {pid}  (优雅关闭)")
    logger.info(f"⚠️  避免使用: kill -9 {pid}   (强制终止)")
    
    worker = Worker(worker_settings)
    
    try:
        await worker.main()
    except KeyboardInterrupt:
        logger.info("🛑 收到键盘中断，Worker 将优雅关闭")
    except Exception as e:
        logger.error(f"❌ Worker 运行错误: {e}")
        raise

def main():
    """主函数"""
    if len(sys.argv) < 2:
        print_usage()
        return
    
    command = sys.argv[1].lower()
    
    if command == "worker":
        asyncio.run(run_worker())
    elif command == "client":
        asyncio.run(submit_test_tasks())
    else:
        print(f"❌ 未知命令: {command}")
        print_usage()

if __name__ == "__main__":
    main()