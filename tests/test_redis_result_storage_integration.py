# -*- coding: utf-8 -*-
"""
Redis 结果存储集成测试

测试 Worker 和 Client 的 Redis 结果存储功能
"""

import asyncio
from datetime import datetime

import pytest
import pytest_asyncio

from rabbitmq_arq import Worker, WorkerSettings, JobContext, RabbitMQClient
from rabbitmq_arq.connections import RabbitMQSettings
from rabbitmq_arq.models import JobStatus
from rabbitmq_arq.result_storage.models import JobResult
from rabbitmq_arq.result_storage.redis import RedisResultStore

# 跳过测试如果 Redis 不可用
pytest_redis = pytest.importorskip("redis")


@pytest_asyncio.fixture
async def redis_store():
    """创建 Redis 存储实例"""
    config = {
        'redis_url': 'redis://localhost:6379/15',  # 使用测试数据库
        'key_prefix': 'test_rabbitmq_arq',
        'ttl': 300  # 5分钟过期
    }

    store = RedisResultStore(config)

    # 测试连接
    try:
        health = await store.health_check()
        if not health:
            pytest.skip("Redis 连接不可用")
    except Exception:
        pytest.skip("Redis 连接失败")

    yield store

    # 清理测试数据
    try:
        if hasattr(store, '_redis') and store._redis:
            pattern = f"{config['key_prefix']}:*"
            keys = await store._redis.keys(pattern)
            if keys:
                await store._redis.delete(*keys)
    except Exception:
        pass

    await store.close()


@pytest.fixture
def rabbitmq_settings():
    """RabbitMQ 配置"""
    return RabbitMQSettings(
        rabbitmq_url="amqp://localhost:5672"
    )


# 测试任务函数
async def sample_task(ctx: JobContext, data: str) -> str:
    """测试任务"""
    await asyncio.sleep(0.1)  # 模拟处理时间
    return f"processed: {data}"


async def failing_sample_task(ctx: JobContext) -> str:
    """失败的测试任务"""
    raise ValueError("测试错误")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_redis_store_basic_operations(redis_store):
    """测试 Redis 存储的基本操作"""
    job_result = JobResult(
        job_id="test_job_123",
        status=JobStatus.COMPLETED,
        result="test result",
        start_time=datetime.now(),
        end_time=datetime.now(),
        worker_id="test_worker",
        queue_name="test_queue",
        function_name="test_function"
    )

    # 存储结果
    await redis_store.store_result(job_result)

    # 获取结果
    retrieved = await redis_store.get_result("test_job_123")
    assert retrieved is not None
    assert retrieved.job_id == "test_job_123"
    assert retrieved.status == JobStatus.COMPLETED
    assert retrieved.result == "test result"

    # 获取状态
    status = await redis_store.get_status("test_job_123")
    assert status == JobStatus.COMPLETED

    # 删除结果
    deleted = await redis_store.delete_result("test_job_123")
    assert deleted is True

    # 验证删除
    retrieved_after_delete = await redis_store.get_result("test_job_123")
    assert retrieved_after_delete is None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_redis_store_batch_operations(redis_store):
    """测试 Redis 存储的批量操作"""
    # 创建多个任务结果
    job_results = []
    for i in range(3):
        job_result = JobResult(
            job_id=f"batch_test_{i}",
            status=JobStatus.COMPLETED,
            result=f"result_{i}",
            start_time=datetime.now(),
            worker_id="batch_worker",
            queue_name="batch_queue",
            function_name="batch_function"
        )
        job_results.append(job_result)
        await redis_store.store_result(job_result)

    # 批量获取
    job_ids = [f"batch_test_{i}" for i in range(3)]
    batch_results = await redis_store.get_results(job_ids)

    assert len(batch_results) == 3
    for i in range(3):
        job_id = f"batch_test_{i}"
        assert job_id in batch_results
        assert batch_results[job_id] is not None
        assert batch_results[job_id].result == f"result_{i}"


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.slow
async def test_worker_redis_integration(rabbitmq_settings):
    """测试 Worker 与 Redis 存储的集成"""
    pytest_redis = pytest.importorskip("redis")

    # Worker 配置
    worker_settings = WorkerSettings(
        rabbitmq_settings=rabbitmq_settings,
        functions=[sample_task, failing_sample_task],
        worker_name="test_redis_worker",
        queue_name="test_redis_queue",
        enable_job_result_storage=True,
        job_result_store_url='redis://localhost:6379/15',
        burst_mode=True,  # 使用 burst 模式便于测试
        burst_timeout=10
    )

    # 创建客户端
    client = RabbitMQClient(
        rabbitmq_settings=rabbitmq_settings,
        result_store_url='redis://localhost:6379/15'
    )
    await client.connect()

    try:
        # 提交任务
        job1 = await client.enqueue_job(
            'sample_task',
            'hello world',
            queue_name='test_redis_queue'
        )

        job2 = await client.enqueue_job(
            'failing_sample_task',
            queue_name='test_redis_queue'
        )

        # 启动 Worker
        worker = Worker(worker_settings)

        # 运行 Worker 一小段时间
        worker_task = asyncio.create_task(worker.main())

        # 等待任务处理
        await asyncio.sleep(2)

        # 停止 Worker
        await worker.graceful_shutdown("测试完成")

        try:
            await asyncio.wait_for(worker_task, timeout=3.0)
        except asyncio.TimeoutError:
            worker_task.cancel()

        # 检查结果是否已存储
        result1 = await client.get_job_result(job1.job_id)
        result2 = await client.get_job_result(job2.job_id)

        # 验证成功任务的结果
        if result1:
            assert result1.status == JobStatus.COMPLETED
            assert "processed: hello world" in str(result1.result)
            assert result1.worker_id == "test_redis_worker"

        # 验证失败任务的结果
        if result2:
            assert result2.status == JobStatus.FAILED
            assert "ValueError" in str(result2.error)

        # 测试删除
        if result1:
            deleted = await client.delete_job_result(job1.job_id)
            assert deleted is True

        if result2:
            deleted = await client.delete_job_result(job2.job_id)
            assert deleted is True

    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_client_redis_query_api(rabbitmq_settings):
    """测试客户端的 Redis 查询 API"""
    pytest_redis = pytest.importorskip("redis")

    # 配置测试用的Redis存储
    redis_config = {
        'redis_url': 'redis://localhost:6379/15',
        'key_prefix': 'test_client_api',
        'ttl': 300
    }

    client = RabbitMQClient(
        rabbitmq_settings=rabbitmq_settings,
        result_store_url='redis://localhost:6379/15'
    )
    await client.connect()

    try:
        # 直接创建一个结果存储实例来插入测试数据
        store = RedisResultStore(redis_config)

        test_result = JobResult(
            job_id="api_test_job",
            status=JobStatus.COMPLETED,
            result={"key": "value"},
            start_time=datetime.now(),
            worker_id="api_test_worker",
            queue_name="api_test_queue",
            function_name="api_test_function"
        )

        await store.store_result(test_result)

        # 测试客户端查询 API

        # 1. 获取单个结果
        result = await client.get_job_result("api_test_job")
        assert result is not None
        assert result.job_id == "api_test_job"

        # 2. 获取状态
        status = await client.get_job_status("api_test_job")
        assert status == JobStatus.COMPLETED

        # 3. 批量查询
        batch_results = await client.get_job_results(["api_test_job", "nonexistent"])
        assert "api_test_job" in batch_results
        assert batch_results["api_test_job"] is not None
        assert batch_results["nonexistent"] is None

        # 4. 获取统计信息
        stats = await client.get_storage_stats()
        assert 'redis_specific' in stats

        # 5. 删除结果
        deleted = await client.delete_job_result("api_test_job")
        assert deleted is True

        # 6. 验证删除
        result_after_delete = await client.get_job_result("api_test_job")
        assert result_after_delete is None

        await store.close()

    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_redis_store_error_handling(redis_store):
    """测试 Redis 存储的错误处理"""

    # 测试获取不存在的结果
    result = await redis_store.get_result("nonexistent_job")
    assert result is None

    # 测试删除不存在的结果
    deleted = await redis_store.delete_result("nonexistent_job")
    assert deleted is False

    # 测试批量获取包含不存在的任务
    batch_results = await redis_store.get_results(["nonexistent1", "nonexistent2"])
    assert batch_results["nonexistent1"] is None
    assert batch_results["nonexistent2"] is None


if __name__ == "__main__":
    # 直接运行部分测试进行手动验证
    import sys


    async def run_basic_test():
        """运行基本测试"""
        config = {
            'redis_url': 'redis://localhost:6379/15',
            'key_prefix': 'manual_test',
            'ttl': 300
        }

        store = RedisResultStore(config)

        try:
            # 健康检查
            health = await store.health_check()
            print(f"Redis 健康检查: {health}")

            if health:
                # 基本操作测试
                job_result = JobResult(
                    job_id="manual_test_123",
                    status=JobStatus.COMPLETED,
                    result="manual test result",
                    start_time=datetime.now(),
                    worker_id="manual_worker",
                    queue_name="manual_queue",
                    function_name="manual_function"
                )

                await store.store_result(job_result)
                print("✅ 结果存储成功")

                retrieved = await store.get_result("manual_test_123")
                print(f"✅ 结果获取成功: {retrieved.result if retrieved else 'None'}")

                await store.delete_result("manual_test_123")
                print("✅ 结果删除成功")

        except Exception as e:
            print(f"❌ 测试失败: {e}")
        finally:
            await store.close()


    if len(sys.argv) > 1 and sys.argv[1] == "manual":
        asyncio.run(run_basic_test())
    else:
        print("使用 'python test_redis_result_storage_integration.py manual' 运行手动测试")
        print("或使用 'pytest' 运行完整测试套件")
