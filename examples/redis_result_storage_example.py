#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Redis 结果存储示例

展示如何配置和使用 Redis 作为任务结果存储
"""

import asyncio
import logging
from datetime import datetime

from rabbitmq_arq import Worker, WorkerSettings, create_client
from rabbitmq_arq.connections import RabbitMQSettings
from rabbitmq_arq import JobContext

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# 示例任务函数
async def process_data(ctx: JobContext, data: dict) -> dict:
    """处理数据任务"""
    logger.info(f"开始处理数据: {data}")
    
    # 模拟一些处理时间
    await asyncio.sleep(1)
    
    # 返回处理结果
    result = {
        "input": data,
        "processed_at": datetime.now().isoformat(),
        "worker_id": ctx.worker_id,
        "job_id": ctx.job_id,
        "result": f"处理了 {len(data)} 个字段"
    }
    
    logger.info(f"数据处理完成: {result}")
    return result


async def slow_calculation(ctx: JobContext, x: int, y: int) -> int:
    """慢速计算任务"""
    logger.info(f"开始计算: {x} + {y}")
    
    # 模拟长时间计算
    await asyncio.sleep(2)
    
    result = x + y
    logger.info(f"计算完成: {x} + {y} = {result}")
    return result


async def might_fail_task(ctx: JobContext, should_fail: bool = False) -> str:
    """可能失败的任务"""
    if should_fail:
        raise ValueError("任务故意失败")
    
    return "任务成功完成"


async def main():
    """主函数：演示 Redis 结果存储的完整流程"""
    
    # RabbitMQ 配置
    rabbitmq_settings = RabbitMQSettings(
        rabbitmq_url="amqp://localhost:5672"  # 根据实际情况修改
    )
    
    # Worker 配置（使用新的URL配置方式）
    worker_settings = WorkerSettings(
        rabbitmq_settings=rabbitmq_settings,
        functions=[process_data, slow_calculation, might_fail_task],
        worker_name="redis_demo_worker",
        queue_name="redis_demo_queue",
        max_concurrent_jobs=5,
        # 结果存储配置（新的URL方式）
        enable_job_result_storage=True,
        job_result_store_url='redis://localhost:6379/0',  # 根据实际情况修改
        job_result_ttl=3600  # 1小时过期
    )
    
    logger.info("=== Redis 结果存储演示开始 ===")
    
    # 创建客户端（使用相同的 Redis 配置）
    client = await create_client(
        rabbitmq_settings=rabbitmq_settings,
        result_store_url='redis://localhost:6379/0'  # 与Worker使用相同的存储URL
    )
    
    try:
        # 1. 提交一些任务
        logger.info("\n--- 步骤1: 提交任务 ---")
        
        # 提交数据处理任务
        job1 = await client.enqueue_job(
            'process_data',
            {'name': 'Alice', 'age': 30, 'city': 'Beijing'},
            queue_name='redis_demo_queue'
        )
        logger.info(f"已提交任务 job1: {job1.job_id}")
        
        # 提交计算任务
        job2 = await client.enqueue_job(
            'slow_calculation',
            100, 200,
            queue_name='redis_demo_queue'
        )
        logger.info(f"已提交任务 job2: {job2.job_id}")
        
        # 提交一个会失败的任务
        job3 = await client.enqueue_job(
            'might_fail_task',
            should_fail=True,
            queue_name='redis_demo_queue'
        )
        logger.info(f"已提交任务 job3 (会失败): {job3.job_id}")
        
        # 提交一个会成功的任务
        job4 = await client.enqueue_job(
            'might_fail_task',
            should_fail=False,
            queue_name='redis_demo_queue'
        )
        logger.info(f"已提交任务 job4 (会成功): {job4.job_id}")
        
        # 2. 启动 Worker（在后台处理任务）
        logger.info("\n--- 步骤2: 启动 Worker 处理任务 ---")
        worker = Worker(worker_settings)
        
        # 使用 asyncio.create_task 在后台运行 worker
        worker_task = asyncio.create_task(worker.main())
        
        # 等待一段时间让任务执行
        logger.info("等待任务执行...")
        await asyncio.sleep(8)  # 等待足够的时间让所有任务完成
        
        # 3. 查询任务结果
        logger.info("\n--- 步骤3: 查询任务结果 ---")
        
        job_ids = [job1.job_id, job2.job_id, job3.job_id, job4.job_id]
        
        # 单个查询
        for job_id in job_ids:
            result = await client.get_job_result(job_id)
            if result:
                logger.info(f"任务 {job_id}: {result.status} - {result.result}")
            else:
                logger.warning(f"任务 {job_id}: 结果未找到")
        
        # 批量查询
        logger.info("\n--- 批量查询结果 ---")
        batch_results = await client.get_job_results(job_ids)
        for job_id, result in batch_results.items():
            if result:
                logger.info(f"批量查询 {job_id}: {result.status}")
            else:
                logger.warning(f"批量查询 {job_id}: 无结果")
        
        # 4. 查看存储统计
        logger.info("\n--- 步骤4: 存储统计信息 ---")
        stats = await client.get_storage_stats()
        logger.info(f"存储统计: {stats}")
        
        # 5. 清理演示数据
        logger.info("\n--- 步骤5: 清理演示数据 ---")
        for job_id in job_ids:
            deleted = await client.delete_job_result(job_id)
            if deleted:
                logger.info(f"已删除任务结果: {job_id}")
        
        # 停止 worker
        logger.info("\n--- 停止 Worker ---")
        await worker.graceful_shutdown("演示完成")
        
        # 等待 worker 任务完成
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Worker 任务停止超时")
            worker_task.cancel()
        logger.info("=== Redis 结果存储演示完成 ===")

    finally:
        await client.close()
    


if __name__ == "__main__":
    # 运行演示
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("演示被用户中断")
    except Exception as e:
        logger.error(f"演示出错: {e}")
        raise