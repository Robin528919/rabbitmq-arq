# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 20:30
# @File           : example
# @IDE            : PyCharm
# @desc           : RabbitMQ-ARQ 使用示例

import asyncio
import logging
import os
import sys
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.rabbitmq_arq import (
    Worker,
    WorkerSettings,
    RabbitMQClient,
    RabbitMQSettings,
    JobContext,
    Retry,
    default_queue_name
)

# 配置中文日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

# 创建专门的日志对象
logger = logging.getLogger('rabbitmq_arq.example')
worker_logger = logging.getLogger('rabbitmq_arq.worker')
task_logger = logging.getLogger('rabbitmq_arq.task')
stats_logger = logging.getLogger('rabbitmq_arq.stats')

# 设置日志级别
logger.setLevel(logging.INFO)
worker_logger.setLevel(logging.INFO)
task_logger.setLevel(logging.INFO)
stats_logger.setLevel(logging.INFO)

# RabbitMQ 连接配置（仅连接相关）
rabbitmq_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    prefetch_count=100,
    connection_timeout=30,
)


# === 任务函数定义 ===

async def process_user_data(ctx: JobContext, user_id: int, data: Dict[str, Any], *args, **kwargs):
    """
    处理用户数据的异步任务函数
    
    Args:
        ctx: 任务上下文
        user_id: 用户ID
        data: 用户数据
    """
    task_logger.info(f"📊 开始处理用户 {user_id} 的数据")
    task_logger.info(f"任务ID: {ctx.job_id}")
    task_logger.info(f"任务尝试次数: {ctx.job_try}")

    try:
        # 模拟数据处理逻辑
        task_logger.info(f"正在验证用户数据...")
        await asyncio.sleep(1)  # 模拟处理时间

        # 模拟一些可能失败的操作
        if data.get('should_fail', False):
            raise Exception("模拟的处理失败")

        task_logger.info(f"正在保存处理结果...")
        await asyncio.sleep(0.5)

        result = {
            'user_id': user_id,
            'processed_at': asyncio.get_event_loop().time(),
            'data_size': len(str(data)),
            'status': 'completed'
        }

        task_logger.info(f"✅ 用户 {user_id} 的数据处理完成")
        return result

    except Exception as e:
        task_logger.error(f"❌ 用户 {user_id} 的数据处理失败: {e}")

        # 如果是第1次尝试，我们可以重试
        if ctx.job_try <= 2:
            task_logger.info(f"🔄 准备重试任务 (尝试次数: {ctx.job_try})")
            raise Retry(defer=5)  # 5秒后重试
        else:
            task_logger.error(f"💥 任务失败，已达到最大重试次数")
            raise


async def send_email(ctx: JobContext, to: str, subject: str, body: str):
    """
    发送邮件的异步任务函数
    
    Args:
        ctx: 任务上下文
        to: 收件人
        subject: 邮件主题
        body: 邮件内容
    """
    task_logger.info(f"📧 准备发送邮件到 {to}")
    task_logger.info(f"任务ID: {ctx.job_id}")
    task_logger.info(f"主题: {subject}")

    try:
        # 模拟邮件发送逻辑
        task_logger.info("正在连接邮件服务器...")
        await asyncio.sleep(1)

        task_logger.info("正在发送邮件...")
        await asyncio.sleep(5)

        # 模拟一些可能失败的情况
        if "fail" in to.lower():
            raise Exception("邮件服务器连接失败")

        task_logger.info(f"✅ 邮件已成功发送到 {to}")
        return {
            'to': to,
            'subject': subject,
            'sent_at': asyncio.get_event_loop().time(),
            'status': 'sent'
        }

    except Exception as e:
        task_logger.error(f"❌ 邮件发送失败: {e}")

        # 邮件发送失败时的重试逻辑
        if ctx.job_try <= 3:
            task_logger.info(f"🔄 邮件将在 10 秒后重试 (尝试次数: {ctx.job_try})")
            raise Retry(defer=10)
        else:
            task_logger.error("💥 邮件发送最终失败")
            raise


# === 生命周期钩子函数 ===

async def startup(ctx: dict):
    """Worker 启动时的钩子函数"""
    worker_logger.info("🚀 Worker 正在启动...")
    worker_logger.info("初始化数据库连接...")
    worker_logger.info("初始化缓存连接...")

    # 在上下文中设置统计信息
    ctx['worker_stats'] = {
        'jobs_complete': 0,
        'jobs_failed': 0,
        'jobs_retried': 0,
        'start_time': asyncio.get_event_loop().time()
    }

    worker_logger.info("✅ Worker 启动完成")


async def shutdown(ctx: dict):
    """Worker 关闭时的钩子函数"""
    worker_logger.info("🛑 Worker 正在关闭...")

    # 获取统计信息
    stats = ctx.get('worker_stats', {})
    start_time = stats.get('start_time', 0)
    current_time = asyncio.get_event_loop().time()
    running_time = current_time - start_time if start_time else 0

    stats_logger.info("📊 Worker 运行统计:")
    stats_logger.info(f"   运行时间: {running_time:.1f} 秒")
    stats_logger.info(f"   完成任务: {stats.get('jobs_complete', 0)} 个")
    stats_logger.info(f"   失败任务: {stats.get('jobs_failed', 0)} 个")
    stats_logger.info(f"   重试任务: {stats.get('jobs_retried', 0)} 个")

    worker_logger.info("清理数据库连接...")
    worker_logger.info("清理缓存连接...")
    worker_logger.info("✅ Worker 关闭完成")


async def job_start(ctx: dict):
    """每个任务开始前的钩子函数"""
    job_id = ctx.get('job_id', 'unknown')
    task_logger.info(f"▶️ 任务 {job_id} 开始执行")


async def job_end(ctx: dict):
    """每个任务结束后的钩子函数"""
    job_id = ctx.get('job_id', 'unknown')
    task_logger.info(f"⏹️ 任务 {job_id} 执行结束")

    # 更新统计信息
    stats = ctx.get('worker_stats', {})
    if ctx.get('job_status') == 'completed':
        stats['jobs_complete'] = stats.get('jobs_complete', 0) + 1
    elif ctx.get('job_status') == 'failed':
        stats['jobs_failed'] = stats.get('jobs_failed', 0) + 1
    elif ctx.get('job_status') == 'retried':
        stats['jobs_retried'] = stats.get('jobs_retried', 0) + 1


# === Worker 配置 ===

# 常规模式 Worker 配置
worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[process_user_data, send_email],
    worker_name="demo_worker",

    # 队列配置
    queue_name=default_queue_name,
    dlq_name=f"{default_queue_name}.dlq",

    # 任务处理配置
    max_retries=3,
    retry_backoff=5.0,
    job_timeout=300,
    max_concurrent_jobs=5,

    # 生命周期钩子
    on_startup=startup,
    on_shutdown=shutdown,
    on_job_start=job_start,
    on_job_end=job_end,

    # 日志配置
    log_level="INFO",
)

# Burst 模式 Worker 配置
burst_worker_settings = WorkerSettings(
    rabbitmq_settings=rabbitmq_settings,
    functions=[process_user_data, send_email],
    worker_name="demo_burst_worker",

    # 队列配置
    queue_name=default_queue_name,
    dlq_name=f"{default_queue_name}.dlq",

    # 任务处理配置
    max_retries=3,
    retry_backoff=5.0,
    job_timeout=300,
    max_concurrent_jobs=3,

    # Burst 模式配置
    burst_mode=True,
    burst_timeout=300,
    burst_check_interval=1.0,
    burst_wait_for_tasks=True,

    # 生命周期钩子
    on_startup=startup,
    on_shutdown=shutdown,
    on_job_start=job_start,
    on_job_end=job_end,

    # 日志配置
    log_level="INFO",
)


# === 主函数：提交任务 ===

async def main():
    """提交一些示例任务"""
    logger.info("🚀 开始任务提交示例")

    # 创建客户端
    client = RabbitMQClient(rabbitmq_settings)

    try:
        # 连接到 RabbitMQ
        await client.connect()
        logger.info("✅ 已连接到 RabbitMQ")

        # 提交数据处理任务
        logger.info("📤 提交用户数据处理任务...")

        user_data_jobs = []
        # for i in range(3):
        #     job = await client.enqueue_job(
        #         "process_user_data",
        #         user_id=1000 + i,
        #         data={
        #             "name": f"用户{i}",
        #             "email": f"user{i}@example.com",
        #             "age": 20 + i,
        #             "should_fail": i == 1  # 让第二个任务失败，测试重试机制
        #         },
        #         queue_name=default_queue_name
        #     )
        #     user_data_jobs.append(job)
        #     logger.info(f"   任务 {job.job_id} 已提交 (用户{i})")

        # 提交邮件发送任务
        logger.info("📤 提交邮件发送任务...")

        email_jobs = []
        emails = [
            ("user1@example.com", "欢迎使用 RabbitMQ-ARQ", "这是一个欢迎邮件"),
            ("user2@example.com", "系统通知", "您的账户信息已更新"),
            # ("fail@example.com", "测试失败", "这封邮件会发送失败"),  # 测试失败重试
        ]

        for to, subject, body in emails:
            job = await client.enqueue_job(
                "send_email",
                to=to,
                subject=subject,
                body=body,
                queue_name=default_queue_name
            )
            email_jobs.append(job)
            logger.info(f"   邮件任务 {job.job_id} 已提交 (发送到 {to})")
        # 参数错误
        # job = await client.enqueue_job(
        #     "send_email",
        #     aaa="123",
        #     queue_name=default_queue_name
        # )
        # email_jobs.append(job)
        # # 提交一些延迟任务
        # logger.info("📤 提交延迟任务...")
        #
        # delayed_job = await client.enqueue_job(
        #     "send_email",
        #     to="delayed@example.com",
        #     subject="延迟邮件",
        #     body="这是一封延迟 10 秒发送的邮件",
        #     queue_name=default_queue_name,
        #     _defer_by=10
        # )
        # logger.info(f"   延迟任务 {delayed_job.job_id} 已提交 (10秒后执行)")

        logger.info("✅ 所有任务已提交完成")
        logger.info(f"   数据处理任务: {len(user_data_jobs)} 个")
        logger.info(f"   邮件发送任务: {len(email_jobs)} 个")
        logger.info(f"   延迟任务: 1 个")
        logger.info("")
        logger.info("💡 接下来你可以:")
        logger.info("   python example.py worker       # 启动常规模式 Worker")
        logger.info("   python example.py burst-worker # 启动 Burst 模式 Worker")

    except Exception as e:
        logger.error(f"❌ 任务提交失败: {e}")
        raise
    finally:
        logger.info("正在关闭客户端连接...")
        await client.close()
        logger.info("客户端连接已关闭")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "worker":
            # 运行常规模式 Worker
            logger.info("启动常规模式 Worker...")
            worker = Worker(worker_settings)
            asyncio.run(worker.main())

        elif command == "burst-worker":
            # 运行 Burst 模式 Worker
            logger.info("启动 Burst 模式 Worker...")
            logger.info("🚀 Burst 模式: 处理完队列中的所有任务后自动退出")
            worker = Worker(burst_worker_settings)
            asyncio.run(worker.main())

        else:
            logger.error(f"❌ 未知命令: {command}")
            logger.info("💡 可用命令:")
            logger.info("  python example.py              # 提交任务")
            logger.info("  python example.py worker       # 启动常规模式 Worker")
            logger.info("  python example.py burst-worker # 启动 Burst 模式 Worker")
    else:
        # 提交任务
        logger.info("启动任务提交模式...")
        asyncio.run(main())
