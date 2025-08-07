# -*- coding: utf-8 -*-
"""
错误处理测试模块 - Pytest 异步测试框架

使用 pytest 和 pytest-asyncio 测试各种错误类型的任务处理，验证错误分类和重试逻辑。

测试覆盖：
- 不可重试错误（TypeError、ValueError等）
- 可重试错误（ConnectionError、TimeoutError等）  
- 业务异常（Exception）
- 显式重试（Retry）
- 重试次数验证
- Worker 行为验证

运行方式：
    pytest tests/test_error_handling.py -v                    # 运行所有测试
    pytest tests/test_error_handling.py::test_type_error -v   # 运行单个测试
    pytest tests/test_error_handling.py -k "error" -v        # 运行匹配的测试
"""

import asyncio
import logging
import os
import sys
from typing import Dict

import pytest
import pytest_asyncio

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rabbitmq_arq import RabbitMQClient, Worker, RabbitMQSettings, WorkerSettings, default_queue_name
from rabbitmq_arq.exceptions import Retry, MaxRetriesExceeded, RabbitMQConnectionError


# ==================== 错误任务函数定义 ====================

async def task_type_error(ctx, missing_param):
    """
    测试 TypeError - 不可重试错误
    这个任务故意缺少必需参数，会引发 TypeError
    """
    print(f"🔴 TypeError任务执行: {missing_param}")
    # 这里会因为调用时缺少参数而引发 TypeError
    return f"不应该执行到这里: {missing_param}"


async def task_value_error(ctx, invalid_value: str):
    """
    测试 ValueError - 不可重试错误
    """
    print(f"🔴 ValueError任务开始: {invalid_value}")

    if invalid_value == "invalid":
        raise ValueError("无效的参数值，这是一个不可重试的错误")

    return f"处理完成: {invalid_value}"


async def task_attribute_error(ctx, obj_name: str):
    """
    测试 AttributeError - 不可重试错误
    """
    print(f"🔴 AttributeError任务开始: {obj_name}")

    # 故意访问不存在的属性
    none_obj = None
    result = none_obj.non_existent_attribute  # 这会引发 AttributeError

    return f"不应该执行到这里: {result}"


async def task_connection_error(ctx, attempt_count: int):
    """
    测试 ConnectionError - 可重试错误
    """
    print(f"🟡 ConnectionError任务开始: 尝试 {attempt_count}")

    # 模拟网络连接失败
    raise RabbitMQConnectionError(f"网络连接失败 - 尝试 {attempt_count}")


async def task_timeout_error(ctx, timeout_seconds: int):
    """
    测试 TimeoutError - 可重试错误
    """
    print(f"🟡 TimeoutError任务开始: {timeout_seconds}秒超时")

    # 模拟超时错误
    raise TimeoutError(f"操作超时 {timeout_seconds} 秒")


async def task_business_exception(ctx, user_id: int, fail_count: int = 3):
    """
    测试业务异常 - 需要检查重试次数的错误
    """
    current_try = ctx.get('job_try', 1)
    print(f"🟠 业务异常任务开始: 用户 {user_id}, 第 {current_try} 次尝试")

    if current_try <= fail_count:
        raise Exception(f"用户 {user_id} 业务处理失败 - 第 {current_try} 次尝试")

    return f"用户 {user_id} 处理成功（第 {current_try} 次尝试）"


async def task_explicit_retry(ctx, retry_count: int = 2):
    """
    测试显式重试 - Retry 异常
    """
    current_try = ctx.get('job_try', 1)
    print(f"🔄 显式重试任务开始: 第 {current_try} 次尝试，最多重试 {retry_count} 次")

    if current_try <= retry_count:
        # 自定义延迟重试
        delay = 3 + current_try  # 递增延迟
        raise Retry(defer=delay)

    return f"重试任务成功完成（第 {current_try} 次尝试）"


async def task_random_errors(ctx, error_type: str):
    """
    根据参数触发不同类型的错误
    """
    print(f"🎲 随机错误任务: {error_type}")

    if error_type == "TypeError":
        # 模拟函数调用错误
        int("not_a_number", "invalid_base")  # 错误的参数数量
    elif error_type == "ValueError":
        int("not_a_number")  # 无效值
    elif error_type == "AttributeError":
        none_obj = None
        none_obj.some_attr
    elif error_type == "ConnectionError":
        raise RabbitMQConnectionError("模拟网络错误")
    elif error_type == "TimeoutError":
        raise TimeoutError("模拟超时")
    elif error_type == "Exception":
        raise Exception("模拟业务异常")
    elif error_type == "Retry":
        raise Retry(defer=5)
    else:
        return f"成功处理: {error_type}"


async def task_success(ctx, message: str):
    """
    正常成功的任务
    """
    print(f"✅ 成功任务: {message}")
    await asyncio.sleep(0.1)  # 模拟一些处理时间
    return f"任务完成: {message}"


# ==================== Pytest Fixtures ====================

@pytest_asyncio.fixture
async def rabbitmq_settings():
    """RabbitMQ 连接设置 fixture"""
    return RabbitMQSettings()


@pytest_asyncio.fixture
async def test_client(rabbitmq_settings):
    """测试客户端 fixture"""
    client = RabbitMQClient(rabbitmq_settings)
    await client.connect()
    yield client
    await client.close()


@pytest_asyncio.fixture
async def test_worker_settings(rabbitmq_settings):
    """测试 Worker 设置 fixture"""
    return WorkerSettings(
        rabbitmq_settings=rabbitmq_settings,
        queue_name=default_queue_name,
        max_retries=3,  # 最大重试3次
        retry_backoff=1,  # 退避时间1秒（加快测试）
        max_concurrent_jobs=2,
        job_timeout=10,  # 短超时时间
        burst_mode=False
    )


@pytest_asyncio.fixture
async def test_worker(test_worker_settings):
    """测试 Worker fixture"""
    # 注册测试任务函数
    functions = {
        'task_type_error': task_type_error,
        'task_value_error': task_value_error,
        'task_attribute_error': task_attribute_error,
        'task_connection_error': task_connection_error,
        'task_timeout_error': task_timeout_error,
        'task_business_exception': task_business_exception,
        'task_explicit_retry': task_explicit_retry,
        'task_random_errors': task_random_errors,
        'task_success': task_success,
    }

    worker = Worker(test_worker_settings, functions)
    yield worker
    await worker.close()


# ==================== 测试辅助类 ====================

class TaskResult:
    """任务执行结果记录"""

    def __init__(self):
        self.job_id = None
        self.completed = False
        self.failed = False
        self.retry_count = 0
        self.error_type = None
        self.execution_time = 0
        self.final_status = None


class MockTaskTracker:
    """模拟任务跟踪器，用于验证任务执行行为"""

    def __init__(self):
        self.task_results: Dict[str, TaskResult] = {}
        self.total_attempts = 0

    def track_attempt(self, job_id: str, error_type: str = None):
        """记录任务尝试"""
        if job_id not in self.task_results:
            self.task_results[job_id] = TaskResult()
            self.task_results[job_id].job_id = job_id

        result = self.task_results[job_id]
        result.retry_count += 1
        if error_type:
            result.error_type = error_type
        self.total_attempts += 1

    def mark_completed(self, job_id: str):
        """标记任务完成"""
        if job_id in self.task_results:
            self.task_results[job_id].completed = True
            self.task_results[job_id].final_status = "completed"

    def mark_failed(self, job_id: str):
        """标记任务失败"""
        if job_id in self.task_results:
            self.task_results[job_id].failed = True
            self.task_results[job_id].final_status = "failed"


# ==================== 测试客户端类 ====================

class ErrorTestClient:
    """错误测试客户端，用于发送各种错误任务"""

    def __init__(self):
        self.settings = RabbitMQSettings()
        self.client = RabbitMQClient(self.settings)

    async def connect(self):
        """连接到 RabbitMQ"""
        await self.client.connect()
        print("🔌 测试客户端已连接到 RabbitMQ")

    async def close(self):
        """关闭连接"""
        await self.client.close()
        print("🔌 测试客户端连接已关闭")

    async def send_type_error_task(self):
        """发送 TypeError 任务（缺少必需参数）"""
        print("\n📤 发送 TypeError 任务...")
        job = await self.client.enqueue_job(
            'task_type_error',
            queue_name=default_queue_name,
            # 故意不提供 missing_param 参数
        )
        print(f"   ✓ TypeError 任务已提交: {job.job_id}")
        return job

    async def send_value_error_task(self):
        """发送 ValueError 任务"""
        print("\n📤 发送 ValueError 任务...")
        job = await self.client.enqueue_job(
            'task_value_error',
            queue_name=default_queue_name,
            invalid_value="invalid"
        )
        print(f"   ✓ ValueError 任务已提交: {job.job_id}")
        return job

    async def send_attribute_error_task(self):
        """发送 AttributeError 任务"""
        print("\n📤 发送 AttributeError 任务...")
        job = await self.client.enqueue_job(
            'task_attribute_error',
            queue_name=default_queue_name,
            obj_name="test_object"
        )
        print(f"   ✓ AttributeError 任务已提交: {job.job_id}")
        return job

    async def send_connection_error_task(self):
        """发送 ConnectionError 任务"""
        print("\n📤 发送 ConnectionError 任务...")
        job = await self.client.enqueue_job(
            'task_connection_error',
            queue_name=default_queue_name,
            attempt_count=1
        )
        print(f"   ✓ ConnectionError 任务已提交: {job.job_id}")
        return job

    async def send_timeout_error_task(self):
        """发送 TimeoutError 任务"""
        print("\n📤 发送 TimeoutError 任务...")
        job = await self.client.enqueue_job(
            'task_timeout_error',
            queue_name=default_queue_name,
            timeout_seconds=30
        )
        print(f"   ✓ TimeoutError 任务已提交: {job.job_id}")
        return job

    async def send_business_exception_task(self, user_id: int = 9001):
        """发送业务异常任务"""
        print("\n📤 发送业务异常任务...")
        job = await self.client.enqueue_job(
            'task_business_exception',
            queue_name=default_queue_name,
            user_id=user_id,
            fail_count=3  # 前3次都失败
        )
        print(f"   ✓ 业务异常任务已提交: {job.job_id}")
        return job

    async def send_explicit_retry_task(self):
        """发送显式重试任务"""
        print("\n📤 发送显式重试任务...")
        job = await self.client.enqueue_job(
            'task_explicit_retry',
            queue_name=default_queue_name,
            retry_count=2
        )
        print(f"   ✓ 显式重试任务已提交: {job.job_id}")
        return job

    async def send_random_error_tasks(self):
        """发送各种随机错误任务"""
        error_types = [
            "TypeError", "ValueError", "AttributeError",
            "ConnectionError", "TimeoutError", "Exception",
            "Retry", "success"
        ]

        jobs = []
        for error_type in error_types:
            print(f"\n📤 发送 {error_type} 任务...")
            job = await self.client.enqueue_job(
                'task_random_errors',
                queue_name=default_queue_name,
                error_type=error_type
            )
            print(f"   ✓ {error_type} 任务已提交: {job.job_id}")
            jobs.append(job)

        return jobs

    async def send_success_tasks(self, count: int = 3):
        """发送成功任务"""
        jobs = []
        for i in range(count):
            print(f"\n📤 发送成功任务 {i + 1}...")
            job = await self.client.enqueue_job(
                'task_success',
                queue_name=default_queue_name,
                message=f"测试消息 {i + 1}"
            )
            print(f"   ✓ 成功任务已提交: {job.job_id}")
            jobs.append(job)

        return jobs

    async def send_all_error_tests(self):
        """发送所有错误测试任务"""
        print("\n🚀 开始发送所有错误测试任务...")

        all_jobs = []

        # 不可重试错误
        all_jobs.append(await self.send_type_error_task())
        all_jobs.append(await self.send_value_error_task())
        all_jobs.append(await self.send_attribute_error_task())

        # 可重试错误
        all_jobs.append(await self.send_connection_error_task())
        all_jobs.append(await self.send_timeout_error_task())

        # 业务异常
        all_jobs.append(await self.send_business_exception_task(9001))
        all_jobs.append(await self.send_business_exception_task(9002))

        # 显式重试
        all_jobs.append(await self.send_explicit_retry_task())

        # 成功任务
        all_jobs.extend(await self.send_success_tasks(2))

        print(f"\n✅ 总共提交了 {len(all_jobs)} 个测试任务")
        return all_jobs


# ==================== Pytest 测试用例 ====================

class TestErrorHandling:
    """错误处理测试类"""

    @pytest.mark.asyncio
    async def test_type_error_immediate_failure(self, test_client):
        """测试 TypeError - 应该立即失败，不重试"""
        # 发送缺少必需参数的任务
        job = await test_client.enqueue_job(
            'task_type_error',
            queue_name=default_queue_name,
            # 故意不提供 missing_param 参数
        )

        assert job.job_id is not None
        assert job.function == 'task_type_error'

        # 验证任务被正确提交
        logging.info(f"✓ TypeError 任务已提交: {job.job_id}")

    @pytest.mark.asyncio
    async def test_value_error_immediate_failure(self, test_client):
        """测试 ValueError - 应该立即失败，不重试"""
        job = await test_client.enqueue_job(
            'task_value_error',
            queue_name=default_queue_name,
            invalid_value="invalid"
        )

        assert job.job_id is not None
        assert job.function == 'task_value_error'
        logging.info(f"✓ ValueError 任务已提交: {job.job_id}")

    @pytest.mark.asyncio
    async def test_attribute_error_immediate_failure(self, test_client):
        """测试 AttributeError - 应该立即失败，不重试"""
        job = await test_client.enqueue_job(
            'task_attribute_error',
            queue_name=default_queue_name,
            obj_name="test_object"
        )

        assert job.job_id is not None
        assert job.function == 'task_attribute_error'
        logging.info(f"✓ AttributeError 任务已提交: {job.job_id}")

    @pytest.mark.asyncio
    async def test_connection_error_retry_behavior(self, test_client):
        """测试 ConnectionError - 应该重试指定次数"""
        job = await test_client.enqueue_job(
            'task_connection_error',
            queue_name=default_queue_name,
            attempt_count=1
        )

        assert job.job_id is not None
        assert job.function == 'task_connection_error'
        logging.info(f"✓ ConnectionError 任务已提交: {job.job_id}")

    @pytest.mark.asyncio
    async def test_timeout_error_retry_behavior(self, test_client):
        """测试 TimeoutError - 应该重试指定次数"""
        job = await test_client.enqueue_job(
            'task_timeout_error',
            queue_name=default_queue_name,
            timeout_seconds=30
        )

        assert job.job_id is not None
        assert job.function == 'task_timeout_error'
        logging.info(f"✓ TimeoutError 任务已提交: {job.job_id}")

    @pytest.mark.asyncio
    async def test_business_exception_retry_behavior(self, test_client):
        """测试业务异常 - 应该重试指定次数后失败"""
        job = await test_client.enqueue_job(
            'task_business_exception',
            queue_name=default_queue_name,
            user_id=9001,
            fail_count=5  # 超过最大重试次数
        )

        assert job.job_id is not None
        assert job.function == 'task_business_exception'
        logging.info(f"✓ 业务异常任务已提交: {job.job_id}")

    @pytest.mark.asyncio
    async def test_explicit_retry_success(self, test_client):
        """测试显式重试 - 应该最终成功"""
        job = await test_client.enqueue_job(
            'task_explicit_retry',
            queue_name=default_queue_name,
            retry_count=2
        )

        assert job.job_id is not None
        assert job.function == 'task_explicit_retry'
        logging.info(f"✓ 显式重试任务已提交: {job.job_id}")

    @pytest.mark.asyncio
    async def test_success_task(self, test_client):
        """测试成功任务 - 应该正常完成"""
        job = await test_client.enqueue_job(
            'task_success',
            queue_name=default_queue_name,
            message="pytest测试消息"
        )

        assert job.job_id is not None
        assert job.function == 'task_success'
        logging.info(f"✓ 成功任务已提交: {job.job_id}")

    @pytest.mark.asyncio
    async def test_batch_error_tasks(self, test_client):
        """测试批量错误任务"""
        error_types = [
            "TypeError", "ValueError", "AttributeError",
            "ConnectionError", "TimeoutError", "Exception",
            "Retry", "success"
        ]

        jobs = []
        for error_type in error_types:
            job = await test_client.enqueue_job(
                'task_random_errors',
                queue_name=default_queue_name,
                error_type=error_type
            )
            jobs.append(job)

        assert len(jobs) == len(error_types)
        assert all(job.job_id is not None for job in jobs)
        logging.info(f"✓ 批量提交了 {len(jobs)} 个错误测试任务")


class TestWorkerBehavior:
    """Worker 行为测试"""

    @pytest.mark.asyncio
    async def test_worker_initialization(self, test_worker_settings):
        """测试 Worker 初始化"""
        functions = {'task_success': task_success}
        worker = Worker(test_worker_settings, functions)

        assert worker.worker_settings == test_worker_settings
        assert 'task_success' in worker.functions
        assert worker.worker_settings.max_retries == 3
        assert worker.worker_settings.retry_backoff == 1

        await worker.close()

    @pytest.mark.asyncio
    async def test_worker_task_registration(self, test_worker):
        """测试任务函数注册"""
        expected_tasks = [
            'task_type_error', 'task_value_error', 'task_attribute_error',
            'task_connection_error', 'task_timeout_error', 'task_business_exception',
            'task_explicit_retry', 'task_random_errors', 'task_success'
        ]

        for task_name in expected_tasks:
            assert task_name in test_worker.functions
            assert callable(test_worker.functions[task_name])

    @pytest.mark.asyncio
    async def test_worker_settings_validation(self, rabbitmq_settings):
        """测试 Worker 设置验证"""
        # 测试有效设置
        valid_settings = WorkerSettings(
            rabbitmq_settings=rabbitmq_settings,
            queue_name="test_queue",
            max_retries=5,
            retry_backoff=2
        )

        assert valid_settings.max_retries == 5
        assert valid_settings.retry_backoff == 2
        assert valid_settings.queue_name == "test_queue"


class TestErrorClassification:
    """错误分类测试"""

    @pytest.mark.asyncio
    async def test_non_retriable_errors(self):
        """测试不可重试错误分类"""
        from rabbitmq_arq.worker import ErrorClassification

        non_retriable_errors = [
            TypeError("test"),
            ValueError("test"),
            AttributeError("test"),
            ImportError("test"),
            ModuleNotFoundError("test"),
            SyntaxError("test"),
            NameError("test"),
            KeyError("test"),
            MaxRetriesExceeded(max_retries=3, job_id="test")
        ]

        for error in non_retriable_errors:
            assert not ErrorClassification.is_retriable_error(error)
            assert ErrorClassification.get_error_category(error) == "non_retriable"

    @pytest.mark.asyncio
    async def test_retriable_errors(self):
        """测试可重试错误分类"""
        from rabbitmq_arq.worker import ErrorClassification

        retriable_errors = [
            RabbitMQConnectionError("test"),
            TimeoutError("test"),
            OSError("test"),
            IOError("test"),
            Retry(defer=5)
        ]

        for error in retriable_errors:
            assert ErrorClassification.is_retriable_error(error)
            assert ErrorClassification.get_error_category(error) == "retriable"

    @pytest.mark.asyncio
    async def test_business_errors(self):
        """测试业务异常分类"""
        from rabbitmq_arq.worker import ErrorClassification

        business_errors = [
            Exception("business error"),
        ]

        for error in business_errors:
            assert ErrorClassification.is_retriable_error(error)
            assert ErrorClassification.get_error_category(error) == "business_retriable"


class TestIntegration:
    """集成测试"""

    @pytest.mark.asyncio
    async def test_full_error_handling_flow(self, test_client):
        """测试完整的错误处理流程"""
        # 发送各种类型的测试任务
        tasks = [
            ('task_type_error', {}),
            ('task_value_error', {'invalid_value': 'invalid'}),
            ('task_connection_error', {'attempt_count': 1}),
            ('task_business_exception', {'user_id': 9999, 'fail_count': 5}),
            ('task_success', {'message': 'integration_test'})
        ]

        submitted_jobs = []
        for task_name, kwargs in tasks:
            job = await test_client.enqueue_job(
                task_name,
                queue_name=default_queue_name,
                **kwargs
            )
            submitted_jobs.append(job)

        assert len(submitted_jobs) == len(tasks)
        assert all(job.job_id is not None for job in submitted_jobs)

        # 验证任务ID唯一性
        job_ids = [job.job_id for job in submitted_jobs]
        assert len(set(job_ids)) == len(job_ids)

        logging.info(f"✓ 集成测试：成功提交 {len(submitted_jobs)} 个任务")

    @pytest.mark.asyncio
    async def test_burst_mode_behavior(self, test_client, rabbitmq_settings):
        """测试 Burst 模式行为"""
        # 先发送一些测试任务
        for i in range(3):
            await test_client.enqueue_job(
                'task_success',
                queue_name=default_queue_name,
                message=f"burst_test_{i}"
            )

        # 创建 Burst 模式 Worker
        burst_settings = WorkerSettings(
            rabbitmq_settings=rabbitmq_settings,
            queue_name=default_queue_name,
            max_retries=1,
            retry_backoff=0.5,
            burst_mode=True  # 启用 Burst 模式
        )

        assert burst_settings.burst_mode is True
        logging.info("✓ Burst 模式 Worker 设置验证完成")


# ==================== 手动测试函数（保留原功能） ====================

async def manual_test_send_error_tasks():
    """手动测试发送各种错误任务（保留原功能）"""
    print("🧪 开始错误任务测试...")
    print("=" * 60)

    client = ErrorTestClient()

    try:
        # 连接客户端
        await client.connect()

        # 发送所有测试任务
        jobs = await client.send_all_error_tests()

        print("\n" + "=" * 60)
        print("📋 任务发送完成！请启动 Worker 来处理这些任务。")
        print(f"💡 提示：运行 'python examples/example.py worker' 来启动 Worker")
        print("\n预期结果：")
        print("  🔴 TypeError/ValueError/AttributeError → 立即发送到死信队列")
        print("  🟡 ConnectionError/TimeoutError → 重试3次后发送到死信队列")
        print("  🟠 Exception业务异常 → 重试3次后发送到死信队列")
        print("  🔄 Retry显式重试 → 按指定次数重试")
        print("  ✅ 成功任务 → 正常完成")

    finally:
        await client.close()


async def manual_test_run_worker():
    """手动运行测试 Worker（保留原功能）"""
    print("🔧 启动错误处理测试 Worker...")
    print("=" * 60)

    # RabbitMQ 连接设置
    rabbitmq_settings = RabbitMQSettings()

    # Worker 设置
    worker_settings = WorkerSettings(
        functions=[task_type_error, task_value_error, task_attribute_error, task_connection_error, task_timeout_error, task_business_exception,
                   task_explicit_retry, task_random_errors, task_success],
        rabbitmq_settings=rabbitmq_settings,
        queue_name=default_queue_name,
        max_retries=3,  # 最大重试3次
        retry_backoff=2,  # 退避时间2秒
        max_concurrent_jobs=5,
        job_timeout=30,
        burst_mode=False
    )


    try:
        await Worker.run(worker_settings)
    except KeyboardInterrupt:
        print("\n⏹️  Worker 已停止")


async def manual_test_burst_mode():
    """手动测试 Burst 模式（保留原功能）"""
    print("⚡ 启动 Burst 模式测试...")
    print("=" * 60)

    # 先发送测试任务
    client = ErrorTestClient()
    await client.connect()
    jobs = await client.send_all_error_tests()
    await client.close()

    # 启动 Burst Worker
    rabbitmq_settings = RabbitMQSettings()
    worker_settings = WorkerSettings(
        rabbitmq_settings=rabbitmq_settings,
        queue_name=default_queue_name,
        max_retries=3,
        retry_backoff=1,  # 更快的测试
        burst_mode=True  # 启用 Burst 模式
    )

    functions = {
        'task_type_error': task_type_error,
        'task_value_error': task_value_error,
        'task_attribute_error': task_attribute_error,
        'task_connection_error': task_connection_error,
        'task_timeout_error': task_timeout_error,
        'task_business_exception': task_business_exception,
        'task_explicit_retry': task_explicit_retry,
        'task_random_errors': task_random_errors,
        'task_success': task_success,
    }

    worker = Worker(worker_settings, functions)

    try:
        await worker.run()
        print("\n🎉 Burst 模式测试完成！")
    finally:
        await worker.close()


# ==================== CLI 入口（保留原功能） ====================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RabbitMQ-ARQ 错误处理测试")
    parser.add_argument("action", choices=["send", "worker", "burst", "pytest"],
                        help="执行动作: send=发送测试任务, worker=启动Worker, burst=Burst模式测试, pytest=运行pytest测试")

    args = parser.parse_args()

    if args.action == "send":
        asyncio.run(manual_test_send_error_tasks())
    elif args.action == "worker":
        asyncio.run(manual_test_run_worker())
    elif args.action == "burst":
        asyncio.run(manual_test_burst_mode())
    elif args.action == "pytest":
        import subprocess

        subprocess.run(["pytest", __file__, "-v"])
