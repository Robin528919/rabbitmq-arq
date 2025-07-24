# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 20:30
# @File           : example
# @IDE            : PyCharm
# @desc           : RabbitMQ-ARQ 使用示例

import asyncio
import logging
from typing import Dict, Any

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
logger = logging.getLogger('rabbitmq_arq.example')
worker_logger = logging.getLogger('rabbitmq_arq.worker')
task_logger = logging.getLogger('rabbitmq_arq.task')
stats_logger = logging.getLogger('rabbitmq_arq.stats')

# 设置日志级别
logger.setLevel(logging.INFO)
worker_logger.setLevel(logging.INFO)
task_logger.setLevel(logging.INFO)
stats_logger.setLevel(logging.INFO)

# RabbitMQ 配置
settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="demo_queue",
    max_retries=3,
    retry_backoff=5.0,
    job_timeout=300,
    prefetch_count=100,
    log_level="INFO"
)

# Burst 模式配置（处理完队列后自动退出）
burst_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="demo_queue",
    max_retries=3,
    retry_backoff=5.0,
    job_timeout=300,
    prefetch_count=100,
    log_level="INFO",
    # 启用 burst 模式
    burst_mode=True,
    burst_timeout=180,  # 最多运行3分钟
    burst_check_interval=1.0,  # 每秒检查一次
    burst_wait_for_tasks=True  # 退出前等待任务完成
)


# 定义任务函数
async def process_user_data(ctx: JobContext, user_id: int, action: str, **kwargs):
    """处理用户数据"""
    task_logger.info(f"开始处理用户 {user_id} 的 {action} 操作")
    task_logger.info(f"任务 ID: {ctx.job_id}")
    task_logger.info(f"尝试次数: {ctx.job_try}")
    task_logger.debug(f"额外参数: {kwargs}")
    
    # 模拟一些处理逻辑
    await asyncio.sleep(2)
    
    # 模拟需要重试的情况
    if action == "retry_action" and ctx.job_try < 2:
        task_logger.warning(f"任务 {ctx.job_id} 需要重试，当前尝试次数: {ctx.job_try}")
        raise Retry(defer=10)  # 10秒后重试
    
    # 模拟处理失败
    if action == "fail_action":
        task_logger.error(f"任务 {ctx.job_id} 处理失败: 业务逻辑错误")
        raise ValueError("处理失败")
    
    result = {"status": "success", "user_id": user_id, "action": action}
    task_logger.info(f"用户 {user_id} 的 {action} 操作处理完成")
    return result


async def send_email(ctx: JobContext, to: str, subject: str, body: str):
    """发送邮件任务"""
    task_logger.info(f"开始发送邮件到 {to}")
    task_logger.info(f"邮件主题: {subject}")
    task_logger.debug(f"邮件内容: {body}")
    
    # 模拟发送邮件
    await asyncio.sleep(1)
    
    result = {"sent": True, "to": to}
    task_logger.info(f"邮件已成功发送到 {to}")
    return result


# Worker 启动和关闭钩子
async def startup(ctx: Dict[Any, Any]):
    """Worker 启动时执行"""
    worker_logger.info("🚀 Worker 启动中...")
    # 可以在这里初始化数据库连接、HTTP 客户端等
    ctx['app_name'] = "Demo App"
    ctx['version'] = "1.0.0"
    # 初始化任务统计
    ctx['jobs_complete'] = 0
    ctx['jobs_failed'] = 0
    ctx['jobs_retried'] = 0
    ctx['jobs_ongoing'] = 0
    
    worker_logger.info(f"应用初始化完成: {ctx['app_name']} v{ctx['version']}")


async def shutdown(ctx: Dict[Any, Any]):
    """Worker 关闭时执行"""
    worker_logger.info("🛑 Worker 关闭中...")
    stats_logger.info(f"📊 最终统计: 完成 {ctx.get('jobs_complete', 0)} 个任务, "
                     f"失败 {ctx.get('jobs_failed', 0)} 个任务, "
                     f"重试 {ctx.get('jobs_retried', 0)} 个任务")
    # 清理资源
    worker_logger.info("Worker 已安全关闭")


# 全局统计变量
_job_stats = {
    'jobs_complete': 0,
    'jobs_failed': 0,
    'jobs_retried': 0,
    'jobs_ongoing': 0
}


# 任务开始钩子
async def job_start(ctx: Dict[Any, Any]):
    """每个任务开始前执行"""
    global _job_stats
    job_id = ctx.get('job_id', 'unknown')
    job_try = ctx.get('job_try', 1)

    # 增加运行中任务数量
    _job_stats['jobs_ongoing'] += 1

    task_logger.info(f"🚀 开始执行任务 {job_id} (第 {job_try} 次尝试)")
    stats_logger.info(f"📈 当前状态: 完成 {_job_stats['jobs_complete']} 个, "
                     f"失败 {_job_stats['jobs_failed']} 个, "
                     f"重试 {_job_stats['jobs_retried']} 个, "
                     f"运行中 {_job_stats['jobs_ongoing']} 个")


# 任务结束钩子
async def job_end(ctx: Dict[Any, Any]):
    """每个任务结束后执行"""
    global _job_stats
    job_id = ctx.get('job_id', 'unknown')
    job_try = ctx.get('job_try', 1)

    # 减少运行中任务数量
    _job_stats['jobs_ongoing'] = max(0, _job_stats['jobs_ongoing'] - 1)

    # 从 Worker 统计中获取最新数据
    # 由于这是在任务完成后调用，我们需要从 ctx 中获取统计信息
    worker_stats = ctx.get('worker_stats', {})
    if worker_stats:
        _job_stats['jobs_complete'] = worker_stats.get('jobs_complete', _job_stats['jobs_complete'])
        _job_stats['jobs_failed'] = worker_stats.get('jobs_failed', _job_stats['jobs_failed'])
        _job_stats['jobs_retried'] = worker_stats.get('jobs_retried', _job_stats['jobs_retried'])

    task_logger.info(f"✅ 任务 {job_id} 执行完成 (第 {job_try} 次尝试)")
    stats_logger.info(f"📊 当前统计: 完成 {_job_stats['jobs_complete']} 个, "
                     f"失败 {_job_stats['jobs_failed']} 个, "
                     f"重试 {_job_stats['jobs_retried']} 个, "
                     f"运行中 {_job_stats['jobs_ongoing']} 个")


# 任务结束后额外钩子
async def after_job_end(ctx: Dict[Any, Any]):
    """每个任务结束后的额外处理"""
    global _job_stats
    job_id = ctx.get('job_id', 'unknown')

    # 这里可以添加额外的统计逻辑，比如写入数据库
    # 或者发送监控指标等

    # 示例：每完成10个任务输出一次详细统计
    jobs_complete = _job_stats['jobs_complete']
    if jobs_complete > 0 and jobs_complete % 10 == 0:
        total_jobs = jobs_complete + _job_stats['jobs_failed']
        success_rate = (jobs_complete / total_jobs * 100) if total_jobs > 0 else 0
        stats_logger.info(f"🎯 里程碑: 已完成 {jobs_complete} 个任务!")
        stats_logger.info(f"📈 成功率: {success_rate:.1f}%")


# 更新统计的函数
def update_job_stats(complete: int = 0, failed: int = 0, retried: int = 0):
    """更新全局统计信息"""
    global _job_stats
    _job_stats['jobs_complete'] += complete
    _job_stats['jobs_failed'] += failed
    _job_stats['jobs_retried'] += retried
    stats_logger.debug(f"统计更新: +{complete} 完成, +{failed} 失败, +{retried} 重试")


# 获取统计的函数
def get_job_stats():
    """获取当前统计信息"""
    global _job_stats
    return _job_stats.copy()


# Worker 配置类
class WorkerSettings:
    """常规模式 Worker 配置"""
    functions = [process_user_data, send_email]
    rabbitmq_settings = settings
    on_startup = startup
    on_shutdown = shutdown
    on_job_start = job_start
    on_job_end = job_end
    after_job_end = after_job_end
    ctx = {"environment": "development"}


class BurstWorkerSettings:
    """Burst 模式 Worker 配置"""
    functions = [process_user_data, send_email]
    rabbitmq_settings = burst_settings
    on_startup = startup
    on_shutdown = shutdown
    on_job_start = job_start
    on_job_end = job_end
    after_job_end = after_job_end
    ctx = {"environment": "development", "mode": "burst"}


async def main():
    """提交任务的示例"""
    logger.info("🚀 开始提交任务示例...")
    
    # 创建客户端
    client = RabbitMQClient(settings)
    
    try:
        logger.info("正在连接到 RabbitMQ...")
        
        # 提交单个任务
        job1 = await client.enqueue_job(
            "process_user_data",
            123,  # user_id
            "update_profile",  # action
            extra_data={"name": "张三", "age": 25}
        )
        logger.info(f"✅ 已提交用户数据处理任务: {job1.job_id}")
        
        # 提交需要重试的任务
        job2 = await client.enqueue_job(
            "process_user_data",
            456,
            "retry_action"
        )
        logger.info(f"✅ 已提交重试测试任务: {job2.job_id}")
        
        # 提交延迟执行的任务
        job3 = await client.enqueue_job(
            "send_email",
            "user@example.com",
            "欢迎注册",
            "感谢您注册我们的服务！",
            _defer_by=30  # 30秒后执行
        )
        logger.info(f"✅ 已提交延迟邮件任务: {job3.job_id} (30秒后执行)")
        
        # 批量提交任务
        jobs = await client.enqueue_jobs([
            {
                "function": "process_user_data",
                "args": [789, "batch_action"],
                "kwargs": {"batch": True}
            },
            {
                "function": "send_email",
                "args": ["admin@example.com", "批量任务通知", "已提交批量任务"],
                "_defer_by": 10
            }
        ])
        logger.info(f"✅ 已批量提交 {len(jobs)} 个任务")
        
        logger.info("🎉 所有任务提交完成！")
        
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
            # python example.py worker
            logger.info("启动常规模式 Worker...")
            Worker.run(WorkerSettings)
        elif command == "burst-worker":
            # 运行 Burst 模式 Worker
            # python example.py burst-worker
            logger.info("启动 Burst 模式 Worker...")
            logger.info("🚀 Burst 模式: 处理完队列中的所有任务后自动退出")
            Worker.run(BurstWorkerSettings)
        else:
            logger.error(f"❌ 未知命令: {command}")
            logger.info("💡 可用命令:")
            logger.info("  python example.py              # 提交任务")
            logger.info("  python example.py worker       # 启动常规模式 Worker")
            logger.info("  python example.py burst-worker # 启动 Burst 模式 Worker")
    else:
        # 提交任务
        # python example.py
        logger.info("启动任务提交模式...")
        asyncio.run(main())
