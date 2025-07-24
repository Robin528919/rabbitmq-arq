# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/7/24 17:00
# @File           : test_client_delay
# @IDE            : PyCharm
# @desc           : 测试客户端延迟机制修复

import asyncio
import logging
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

logger = logging.getLogger('test_client_delay')

# 测试配置
test_settings = RabbitMQSettings(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    rabbitmq_queue="client_delay_test_queue",
    max_retries=3,
    retry_backoff=3.0,
    burst_mode=True,
    burst_timeout=60
)


# 测试任务
async def immediate_task(ctx: JobContext, task_id: str):
    """立即执行的任务"""
    current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    logger.info(f"⚡ [{current_time}] 立即任务 '{task_id}' 执行成功！")
    return {"task_id": task_id, "executed_at": current_time, "type": "immediate"}


async def client_delayed_task(ctx: JobContext, task_id: str, delay_info: str):
    """客户端延迟的任务"""
    current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    logger.info(f"⏰ [{current_time}] 客户端延迟任务 '{task_id}' 执行成功！延迟信息: {delay_info}")
    return {"task_id": task_id, "executed_at": current_time, "type": "client_delayed", "delay_info": delay_info}


async def retry_task(ctx: JobContext, task_id: str):
    """测试重试的任务"""
    current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    
    if ctx.job_try == 1:
        logger.warning(f"🔄 [{current_time}] 重试任务 '{task_id}' 第一次执行，请求重试（延迟5秒）")
        raise Retry(defer=5)
    else:
        logger.info(f"✅ [{current_time}] 重试任务 '{task_id}' 第二次执行成功！")
        return {"task_id": task_id, "executed_at": current_time, "type": "retry_success", "try": ctx.job_try}


# Worker 钩子函数
async def test_startup(ctx):
    """启动钩子"""
    logger.info("🚀 客户端延迟测试 Worker 启动中...")
    ctx['start_time'] = datetime.now()


async def test_shutdown(ctx):
    """关闭钩子"""
    duration = (datetime.now() - ctx['start_time']).total_seconds()
    logger.info(f"🛑 客户端延迟测试 Worker 关闭，运行时长: {duration:.1f} 秒")


# Worker 设置
class ClientDelayTestWorkerSettings:
    functions = [immediate_task, client_delayed_task, retry_task]
    rabbitmq_settings = test_settings
    on_startup = test_startup
    on_shutdown = test_shutdown


async def submit_test_tasks():
    """提交测试任务"""
    logger.info("📝 开始提交客户端延迟测试任务...")
    
    client = RabbitMQClient(test_settings)
    
    try:
        current_time = datetime.now()
        logger.info(f"📅 当前时间: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
        
        # 1. 提交立即执行任务
        immediate_job = await client.enqueue_job(
            "immediate_task",
            "immediate_1"
        )
        logger.info(f"📤 已提交立即任务: {immediate_job.job_id}")
        
        # 2. 提交客户端延迟任务（延迟5秒）
        delay_job1 = await client.enqueue_job(
            "client_delayed_task",
            "delayed_1",
            "延迟5秒执行",
            _defer_by=5  # 5秒后执行
        )
        logger.info(f"📤 已提交5秒延迟任务: {delay_job1.job_id}")
        
        # 3. 提交客户端延迟任务（延迟10秒）
        delay_job2 = await client.enqueue_job(
            "client_delayed_task",
            "delayed_2", 
            "延迟10秒执行",
            _defer_by=10  # 10秒后执行
        )
        logger.info(f"📤 已提交10秒延迟任务: {delay_job2.job_id}")
        
        # 4. 提交需要重试的任务
        retry_job = await client.enqueue_job(
            "retry_task",
            "retry_1"
        )
        logger.info(f"📤 已提交重试测试任务: {retry_job.job_id}")
        
        # 5. 提交到未来时间的任务
        future_time = current_time + timedelta(seconds=8)
        future_job = await client.enqueue_job(
            "client_delayed_task",
            "future_1",
            f"延迟到 {future_time.strftime('%H:%M:%S')}",
            _defer_until=future_time
        )
        logger.info(f"📤 已提交未来时间任务: {future_job.job_id}")
        
        logger.info("🎉 所有客户端延迟测试任务提交完成！")
        logger.info("💡 现在运行: python test_client_delay.py worker")
        
    except Exception as e:
        logger.error(f"❌ 测试任务提交失败: {e}")
        raise
    finally:
        await client.close()


async def clear_queue():
    """清空测试队列"""
    client = RabbitMQClient(test_settings)
    try:
        await client.connect()
        queue = await client.channel.get_queue(test_settings.rabbitmq_queue)
        purged = await queue.purge()
        logger.info(f"🧹 已清空队列，删除了 {purged} 条消息")
    except Exception as e:
        logger.error(f"❌ 清空队列失败: {e}")
    finally:
        await client.close()


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("🧪 客户端延迟机制测试")
        print("\n📋 使用方法:")
        print("  clear  - 清空队列")
        print("  submit - 提交测试任务")
        print("  worker - 运行测试 Worker")
        print("\n💡 测试步骤:")
        print("  1. python test_client_delay.py clear")
        print("  2. python test_client_delay.py submit")
        print("  3. python test_client_delay.py worker")
        print("\n🎯 预期结果:")
        print("  - 立即任务应该马上执行")
        print("  - 延迟任务应该在指定时间后执行")
        print("  - 重试任务应该延迟5秒后重试")
        print("  - 客户端延迟日志应该显示使用延迟交换机")
        return
    
    command = sys.argv[1]
    
    if command == "clear":
        asyncio.run(clear_queue())
    elif command == "submit":
        asyncio.run(submit_test_tasks())
    elif command == "worker":
        Worker.run(ClientDelayTestWorkerSettings)
    else:
        print(f"❌ 未知命令: {command}")


if __name__ == "__main__":
    main() 