# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/7/24 15:30
# @File           : test_delay_mechanism
# @IDE            : PyCharm
# @desc           : 测试智能延迟机制

import asyncio
import logging
from datetime import datetime

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

logger = logging.getLogger('test_delay')

# 测试配置
test_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="delay_test_queue",
    max_retries=3,
    retry_backoff=5.0,
    burst_mode=True,
    burst_timeout=120
)


# 测试任务
async def delay_test_task(ctx: JobContext, task_name: str, delay_seconds: int):
    """测试延迟任务"""
    current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    
    if ctx.job_try == 1:
        logger.info(f"⏰ [{current_time}] 任务 '{task_name}' 首次执行，请求延迟 {delay_seconds} 秒")
        raise Retry(defer=delay_seconds)
    else:
        logger.info(f"✅ [{current_time}] 任务 '{task_name}' 延迟后执行成功！(第 {ctx.job_try} 次尝试)")
        return {"task": task_name, "executed_at": current_time}


# Worker 钩子
async def startup(ctx):
    logger.info("🚀 延迟机制测试 Worker 启动")
    logger.info("👀 请观察日志，查看使用的延迟机制：")
    logger.info("   - ✅ 延迟交换机 = 安装了延迟插件")
    logger.info("   - ⚠️ TTL 队列 = 使用降级方案")


async def shutdown(ctx):
    logger.info("🏁 延迟机制测试完成")


# Worker 配置
class DelayTestWorkerSettings:
    functions = [delay_test_task]
    rabbitmq_settings = test_settings
    on_startup = startup
    on_shutdown = shutdown


async def submit_test_tasks():
    """提交测试任务"""
    logger.info("📝 提交延迟测试任务...")
    
    client = RabbitMQClient(test_settings)
    
    try:
        # 提交不同延迟时间的任务
        delays = [3, 5, 10]  # 秒
        
        for i, delay in enumerate(delays):
            job = await client.enqueue_job(
                "delay_test_task",
                f"Task_{i+1}",
                delay
            )
            logger.info(f"📤 已提交任务 {i+1}，将延迟 {delay} 秒执行")
        
        logger.info(f"✅ 已提交 {len(delays)} 个延迟测试任务")
        
    finally:
        await client.close()


async def clear_queue():
    """清空队列"""
    from aio_pika import connect_robust
    
    connection = await connect_robust(test_settings.rabbitmq_url)
    channel = await connection.channel()
    
    try:
        # 清空主队列
        queue = await channel.declare_queue(test_settings.rabbitmq_queue, durable=True)
        count = await queue.purge()
        logger.info(f"🗑️ 已清空主队列 {count} 条消息")
        
        # 尝试清空延迟队列（如果存在）
        try:
            delay_queue = await channel.declare_queue(f"{test_settings.rabbitmq_queue}_delay", durable=True)
            delay_count = await delay_queue.purge()
            logger.info(f"🗑️ 已清空延迟队列 {delay_count} 条消息")
        except:
            pass
            
    finally:
        await connection.close()


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("🧪 智能延迟机制测试")
        print("\n📋 使用方法:")
        print("  clear  - 清空队列")
        print("  submit - 提交测试任务")
        print("  worker - 运行测试 Worker")
        print("\n💡 测试步骤:")
        print("  1. python test_delay_mechanism.py clear")
        print("  2. python test_delay_mechanism.py submit")
        print("  3. python test_delay_mechanism.py worker")
        return
    
    command = sys.argv[1]
    
    if command == "clear":
        asyncio.run(clear_queue())
    elif command == "submit":
        asyncio.run(submit_test_tasks())
    elif command == "worker":
        Worker.run(DelayTestWorkerSettings)
    else:
        print(f"❌ 未知命令: {command}")


if __name__ == "__main__":
    main() 