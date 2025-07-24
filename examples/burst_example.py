# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 23:00
# @File           : burst_example
# @IDE            : PyCharm
# @desc           : RabbitMQ-ARQ Burst 模式使用示例

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

# 配置中文日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

# 创建专门的日志对象
logger = logging.getLogger('burst_example')
task_logger = logging.getLogger('burst_task')

# 设置日志级别
logger.setLevel(logging.INFO)
task_logger.setLevel(logging.INFO)

# Burst 模式配置
burst_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="burst_demo_queue",
    max_retries=2,
    retry_backoff=3.0,
    job_timeout=60,
    prefetch_count=10,
    log_level="INFO",
    # 关键：启用 burst 模式
    burst_mode=True,
    burst_timeout=120,  # 最多运行2分钟
    burst_check_interval=2.0,  # 每2秒检查一次
    burst_wait_for_tasks=True  # 退出前等待任务完成
)

# 常规模式配置（对比用）
normal_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="burst_demo_queue",
    max_retries=2,
    retry_backoff=3.0,
    job_timeout=60,
    prefetch_count=10,
    log_level="INFO",
    burst_mode=False  # 常规模式
)


# 定义批处理任务函数
async def process_data_batch(ctx: JobContext, batch_id: int, data_count: int, processing_time: float = 1.0):
    """批量数据处理任务"""
    task_logger.info(f"📦 开始处理批次 {batch_id}，包含 {data_count} 条数据")
    task_logger.info(f"任务 ID: {ctx.job_id}")
    task_logger.info(f"尝试次数: {ctx.job_try}")
    
    # 模拟数据处理
    start_time = time.time()
    await asyncio.sleep(processing_time)
    elapsed = time.time() - start_time
    
    # 模拟偶尔失败的情况
    if batch_id % 7 == 0 and ctx.job_try < 2:
        task_logger.warning(f"批次 {batch_id} 处理失败，需要重试")
        raise Retry(defer=2)  # 2秒后重试
    
    result = {
        "batch_id": batch_id,
        "data_count": data_count,
        "processing_time": elapsed,
        "status": "completed"
    }
    
    task_logger.info(f"✅ 批次 {batch_id} 处理完成，耗时 {elapsed:.2f}s")
    return result


async def generate_report(ctx: JobContext, report_type: str, data_range: str):
    """生成报告任务"""
    task_logger.info(f"📊 开始生成 {report_type} 报告，数据范围: {data_range}")
    
    # 模拟报告生成
    await asyncio.sleep(2)
    
    result = {
        "report_type": report_type,
        "data_range": data_range,
        "generated_at": asyncio.get_event_loop().time(),
        "status": "generated"
    }
    
    task_logger.info(f"✅ {report_type} 报告生成完成")
    return result


async def cleanup_task(ctx: JobContext, cleanup_type: str):
    """清理任务"""
    task_logger.info(f"🧹 开始执行 {cleanup_type} 清理")
    
    # 模拟清理工作
    await asyncio.sleep(0.5)
    
    task_logger.info(f"✅ {cleanup_type} 清理完成")
    return {"cleanup_type": cleanup_type, "status": "cleaned"}


# Worker 钩子函数
async def burst_startup(ctx: Dict[Any, Any]):
    """Burst Worker 启动钩子"""
    logger.info("🚀 Burst 模式 Worker 启动")
    ctx['start_time'] = time.time()
    ctx['batch_processed'] = 0


async def burst_shutdown(ctx: Dict[Any, Any]):
    """Burst Worker 关闭钩子"""
    elapsed = time.time() - ctx.get('start_time', time.time())
    logger.info(f"🏁 Burst 模式 Worker 关闭，总运行时间: {elapsed:.2f}s")
    logger.info(f"📊 处理的批次数: {ctx.get('batch_processed', 0)}")


async def burst_job_start(ctx: Dict[Any, Any]):
    """Burst 任务开始钩子"""
    job_id = ctx.get('job_id', 'unknown')
    worker_stats = ctx.get('worker_stats', {})
    
    logger.info(f"🔄 开始处理任务 {job_id}")
    logger.debug(f"当前统计: {worker_stats}")


async def burst_job_end(ctx: Dict[Any, Any]):
    """Burst 任务结束钩子"""
    job_id = ctx.get('job_id', 'unknown')
    worker_stats = ctx.get('worker_stats', {})
    
    # 更新批次计数
    if 'batch_processed' in ctx:
        ctx['batch_processed'] += 1
    
    logger.info(f"✅ 任务 {job_id} 处理完成")
    logger.info(f"📈 进度: 完成 {worker_stats.get('jobs_complete', 0)} 个任务")


# Burst Worker 配置
class BurstWorkerSettings:
    """Burst 模式 Worker 配置"""
    functions = [process_data_batch, generate_report, cleanup_task]
    rabbitmq_settings = burst_settings
    on_startup = burst_startup
    on_shutdown = burst_shutdown
    on_job_start = burst_job_start
    on_job_end = burst_job_end
    ctx = {"mode": "burst", "environment": "demo"}


# 常规 Worker 配置（对比用）
class NormalWorkerSettings:
    """常规模式 Worker 配置"""
    functions = [process_data_batch, generate_report, cleanup_task]
    rabbitmq_settings = normal_settings
    on_startup = burst_startup
    on_shutdown = burst_shutdown
    on_job_start = burst_job_start
    on_job_end = burst_job_end
    ctx = {"mode": "normal", "environment": "demo"}


async def submit_batch_jobs():
    """提交批处理任务示例"""
    logger.info("📝 开始提交批处理任务...")
    
    # 创建客户端（使用任意一个设置，只是为了提交任务）
    client = RabbitMQClient(burst_settings)
    
    try:
        logger.info("正在连接到 RabbitMQ...")
        
        # 提交多个批处理任务
        batch_jobs = []
        for i in range(1, 16):  # 提交15个批次
            job = await client.enqueue_job(
                "process_data_batch",
                i,  # batch_id
                100 + i * 10,  # data_count
                1.0 + (i % 3) * 0.5  # processing_time
            )
            batch_jobs.append(job)
            logger.info(f"📦 已提交批次 {i} 处理任务: {job.job_id}")
        
        # 提交报告生成任务
        report_jobs = []
        for report_type in ["daily", "weekly", "monthly"]:
            job = await client.enqueue_job(
                "generate_report",
                report_type,
                "2025-01-01 to 2025-01-31"
            )
            report_jobs.append(job)
            logger.info(f"📊 已提交 {report_type} 报告任务: {job.job_id}")
        
        # 提交清理任务
        cleanup_job = await client.enqueue_job(
            "cleanup_task",
            "temp_files"
        )
        logger.info(f"🧹 已提交清理任务: {cleanup_job.job_id}")
        
        total_jobs = len(batch_jobs) + len(report_jobs) + 1
        logger.info(f"🎉 所有任务提交完成！总计 {total_jobs} 个任务")
        logger.info("💡 现在可以运行 Worker 来处理这些任务:")
        logger.info("   python burst_example.py burst-worker   # Burst 模式")
        logger.info("   python burst_example.py normal-worker  # 常规模式")
        
    except Exception as e:
        logger.error(f"❌ 任务提交失败: {e}")
        raise
    finally:
        logger.info("正在关闭客户端连接...")
        await client.close()
        logger.info("客户端连接已关闭")


async def clear_queue():
    """清空队列（用于测试）"""
    logger.info("🗑️ 清空队列中的所有消息...")
    
    from aio_pika import connect_robust
    
    connection = await connect_robust(burst_settings.rabbitmq_url)
    channel = await connection.channel()
    
    try:
        queue = await channel.declare_queue(burst_settings.rabbitmq_queue, durable=True)
        purged_count = await queue.purge()
        logger.info(f"✅ 已清空 {purged_count} 条消息")
    finally:
        await connection.close()


def main():
    """主函数"""
    import sys
    
    if len(sys.argv) < 2:
        print("🚀 RabbitMQ-ARQ Burst 模式示例")
        print("\n📋 可用命令:")
        print("  submit        - 提交批处理任务")
        print("  burst-worker  - 启动 Burst 模式 Worker")
        print("  normal-worker - 启动常规模式 Worker")
        print("  clear         - 清空队列")
        print("\n💡 使用示例:")
        print("  python burst_example.py submit        # 提交任务")
        print("  python burst_example.py burst-worker  # 处理任务（Burst 模式）")
        return
    
    command = sys.argv[1]
    
    if command == "submit":
        logger.info("启动任务提交模式...")
        asyncio.run(submit_batch_jobs())
    elif command == "burst-worker":
        logger.info("启动 Burst 模式 Worker...")
        Worker.run(BurstWorkerSettings)
    elif command == "normal-worker":
        logger.info("启动常规模式 Worker...")
        Worker.run(NormalWorkerSettings)
    elif command == "clear":
        logger.info("清空队列模式...")
        asyncio.run(clear_queue())
    else:
        logger.error(f"❌ 未知命令: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main() 