# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 21:00
# @File           : test_example
# @IDE            : PyCharm
# @desc           : 测试 RabbitMQ-ARQ 修复效果

import asyncio
import logging
import os
import sys

# 添加项目根目录到路径（用于开发环境）
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rabbitmq_arq import (
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

async def basic_task_test(ctx: JobContext, task_name: str, data: dict):
    """基础任务测试"""
    logger.info(f"🔬 执行基础任务: {task_name}")
    logger.info(f"   任务ID: {ctx.job_id}")
    logger.info(f"   数据: {data}")

    await asyncio.sleep(30)

    logger.info(f"✅ 基础任务 {task_name} 延迟30秒 完成")
    return {"task_name": task_name, "status": "completed", "data": data}


async def retry_task_test(ctx: JobContext, retry_count: int = 2):
    """重试任务测试 - ARQ 风格"""
    logger.info(f"🔄 执行重试任务测试")
    logger.info(f"   任务ID: {ctx.job_id}")
    logger.info(f"   当前尝试: {ctx.job_try}")
    logger.info(f"   预期重试: {retry_count} 次")

    if ctx.job_try <= retry_count:
        logger.warning(f"💥 任务需要重试 ({ctx.job_try}/{retry_count})")
        # ARQ 风格：显式抛出 Retry 异常
        raise Retry(defer=1)  # 1秒后重试

    logger.info(f"✅ 重试任务最终成功")
    return {"retry_count": ctx.job_try - 1, "status": "completed"}


async def delayed_task_test(ctx: JobContext, message: str):
    """延迟任务测试"""
    logger.info(f"⏰ 执行延迟任务: {message}")
    logger.info(f"   任务ID: {ctx.job_id}")

    await asyncio.sleep(0.2)

    logger.info(f"✅ 延迟任务完成: {message}")
    return {"message": message, "status": "completed"}


async def error_task_test(ctx: JobContext, error_message: str = "测试错误"):
    """错误任务测试 - ARQ 风格，抛出不可重试的异常"""
    logger.info(f"💥 执行错误任务测试")
    logger.info(f"   任务ID: {ctx.job_id}")
    logger.info(f"   错误信息: {error_message}")
    
    # 模拟一些处理时间
    await asyncio.sleep(0.1)
    
    # ARQ 风格：抛出明确的不可重试异常
    logger.warning(f"❌ 任务即将抛出 ValueError: {error_message}")
    raise ValueError(error_message)  # ValueError 是不可重试的异常


async def network_retry_task_test(ctx: JobContext, should_succeed_on_try: int = 3):
    """网络重试任务测试 - 模拟网络错误的重试"""
    logger.info(f"🌐 执行网络重试任务测试")
    logger.info(f"   任务ID: {ctx.job_id}")
    logger.info(f"   当前尝试: {ctx.job_try}")
    logger.info(f"   预期在第 {should_succeed_on_try} 次尝试成功")
    
    # 模拟网络请求
    await asyncio.sleep(0.1)
    
    if ctx.job_try < should_succeed_on_try:
        logger.warning(f"🌐 模拟网络连接错误 (尝试 {ctx.job_try}/{should_succeed_on_try})")
        # 抛出系统级错误，这些错误会自动重试
        raise OSError(f"网络连接失败 (尝试 {ctx.job_try})")
    
    logger.info(f"✅ 网络任务在第 {ctx.job_try} 次尝试成功")
    return {"success_on_try": ctx.job_try, "status": "completed"}


# === 生命周期钩子 ===

async def startup_test(ctx: dict):
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


async def shutdown_test(ctx: dict):
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
    functions=[basic_task_test, retry_task_test, delayed_task_test, error_task_test, network_retry_task_test],
    worker_name="test_worker",

    # 队列配置
    queue_name="test_queue",
    dlq_name="test_queue_dlq",

    # 任务处理配置
    max_retries=3,
    retry_backoff=1.0,
    # job_timeout=30,
    max_concurrent_jobs=3,

    # Burst 模式配置（用于测试）
    burst_mode=False,
    burst_timeout=60,
    burst_check_interval=1.0,
    burst_wait_for_tasks=True,

    # 生命周期钩子
    on_startup=startup_test,
    on_shutdown=shutdown_test,
    on_job_start=job_start_hook,
    on_job_end=job_end_hook,

    # 日志配置
    log_level="INFO",

    job_result_store_url="redis://:Admin123@127.0.0.1:46379/0",
)


# === 测试函数 ===

async def basic_functionality_test():
    """测试基本功能"""
    logger.info("🧪 开始基本功能测试")

    client = RabbitMQClient(
        rabbitmq_settings,
        result_store_url="redis://:Admin123@127.0.0.1:46379/0"
    )

    try:
        await client.connect()
        logger.info("✅ 客户端连接成功")

        # 测试基础任务提交
        job1 = await client.enqueue_job(
            "basic_task_test",
            task_name="基础测试",
            data={"test": True, "number": 123},
            queue_name="test_queue"
        )
        logger.info(f"✅ 基础任务已提交: {job1.job_id}")
        logger.info(f"✅ 基础任务结果: {await job1.result()}")

        # 测试重试任务
        job2 = await client.enqueue_job(
            "retry_task_test",
            retry_count=2,
            queue_name="test_queue"
        )
        logger.info(f"✅ 重试任务已提交: {job2.job_id}")

        # 测试延迟任务
        job3 = await client.enqueue_job(
            "delayed_task_test",
            message="这是一个延迟3秒的任务",
            queue_name="test_queue",
            _defer_by=3
        )
        logger.info(f"✅ 延迟任务已提交: {job3.job_id}")

        # 测试错误任务
        job4 = await client.enqueue_job(
            "error_task_test",
            error_message="这是一个测试错误",
            queue_name="test_queue"
        )
        logger.info(f"✅ 错误任务已提交: {job4.job_id}")

        # 测试网络重试任务
        job5 = await client.enqueue_job(
            "network_retry_task_test",
            should_succeed_on_try=3,
            queue_name="test_queue"
        )
        logger.info(f"✅ 网络重试任务已提交: {job5.job_id}")

        logger.info("🎉 所有测试任务已提交")

        # 测试 ARQ 风格的任务状态和结果 API
        logger.info("📋 开始测试 ARQ 风格任务操作...")

        # 演示任务状态查询
        jobs = [job1, job2, job3, job4, job5]
        job_names = ["基础任务", "重试任务", "延迟任务", "错误任务", "网络重试任务"]

        for job, name in zip(jobs, job_names):
            try:
                # 获取任务状态
                status = await job.status()
                logger.info(f"📊 {name} 状态: {status}")

                # 获取任务信息
                info = await job.info()
                logger.info(f"ℹ️  {name} 信息: {info}")
            except Exception as e:
                logger.warning(f"⚠️ 获取 {name} 状态失败: {e}")

        # 等待并获取任务结果
        logger.info("⏳ 开始等待任务结果...")

        # 基础任务结果
        try:
            logger.info(f"🔄 等待基础任务完成: {job1.job_id}")
            result1 = await job1.result(timeout=10)
            logger.info(f"✅ 基础任务结果: {result1}")
        except Exception as e:
            logger.error(f"❌ 获取基础任务结果失败: {e}")

        # 重试任务结果  
        try:
            logger.info(f"🔄 等待重试任务完成: {job2.job_id}")
            result2 = await job2.result(timeout=15)  # 重试任务需要更长时间
            logger.info(f"✅ 重试任务结果: {result2}")
        except Exception as e:
            logger.error(f"❌ 获取重试任务结果失败: {e}")

        # 延迟任务结果
        try:
            logger.info(f"🔄 等待延迟任务完成: {job3.job_id}")
            result3 = await job3.result(timeout=10)  # 延迟3秒 + 执行时间
            logger.info(f"✅ 延迟任务结果: {result3}")
        except Exception as e:
            logger.error(f"❌ 获取延迟任务结果失败: {e}")

        # 错误任务结果（预期失败）
        try:
            logger.info(f"🔄 等待错误任务完成: {job4.job_id}")
            result4 = await job4.result(timeout=10)
            logger.warning(f"⚠️ 错误任务意外成功: {result4}")
        except Exception as e:
            logger.info(f"✅ 错误任务按预期失败: {e}")

        # 网络重试任务结果
        try:
            logger.info(f"🔄 等待网络重试任务完成: {job5.job_id}")
            result5 = await job5.result(timeout=15)  # 网络重试需要更长时间
            logger.info(f"✅ 网络重试任务结果: {result5}")
        except Exception as e:
            logger.error(f"❌ 获取网络重试任务结果失败: {e}")

        # 最后再次检查所有任务的最终状态
        logger.info("🏁 检查最终任务状态...")
        for job, name in zip(jobs, job_names):
            try:
                final_status = await job.status()
                logger.info(f"📈 {name} 最终状态: {final_status}")
            except Exception as e:
                logger.warning(f"⚠️ 获取 {name} 最终状态失败: {e}")

        logger.info("🎊 ARQ 风格任务操作测试完成")

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
        asyncio.run(basic_functionality_test())
