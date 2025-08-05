# -*- coding: utf-8 -*-
"""
Pytest 配置文件

包含通用的 fixtures、测试设置和配置。
"""

import asyncio
import sys
import os
import logging
from typing import Generator

import pytest
import pytest_asyncio

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rabbitmq_arq import RabbitMQSettings, default_queue_name


# ==================== Pytest 配置 ====================

def pytest_configure(config):
    """Pytest 配置钩子"""
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 添加自定义标记
    config.addinivalue_line(
        "markers", "asyncio: 标记异步测试函数"
    )
    config.addinivalue_line(
        "markers", "integration: 标记集成测试"
    )
    config.addinivalue_line(
        "markers", "slow: 标记慢速测试"
    )


def pytest_collection_modifyitems(config, items):
    """修改测试项目收集"""
    # 为异步测试添加 asyncio 标记
    for item in items:
        if asyncio.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)


# ==================== 通用 Fixtures ====================

@pytest.fixture(scope="session")
def event_loop():
    """会话级别的事件循环 fixture"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_queue_name():
    """测试队列名称 fixture"""
    return f"test_{default_queue_name}"


@pytest.fixture
def mock_settings():
    """模拟设置 fixture"""
    return {
        'host': 'localhost',
        'port': 5672,
        'username': 'guest',
        'password': 'guest',
        'vhost': '/',
        'max_retries': 3,
        'retry_backoff': 1
    }


# ==================== 清理 Fixtures ====================

@pytest.fixture(autouse=True)
async def cleanup_test_environment():
    """自动清理测试环境 fixture"""
    # 测试前准备
    yield
    
    # 测试后清理
    # 这里可以添加清理队列、重置状态等操作
    pass


# ==================== 跳过条件 ====================

def pytest_runtest_setup(item):
    """测试运行前设置"""
    # 检查 RabbitMQ 连接
    if hasattr(item, 'requires_rabbitmq'):
        try:
            # 这里可以添加 RabbitMQ 连接检查
            pass
        except Exception:
            pytest.skip("RabbitMQ 不可用") 