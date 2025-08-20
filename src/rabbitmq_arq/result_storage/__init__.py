# -*- coding: utf-8 -*-
"""
RabbitMQ-ARQ 任务结果存储模块

提供多种存储后端支持任务结果的持久化、查询和管理
"""

from .base import (
    ResultStore,
    ResultStorageError,
    ResultStoreConnectionError,
    ResultStoreTimeoutError,
    ResultNotFoundError,
    ResultSerializationError,
    ResultStorageConfigError,
)

from .models import (
    JobResult,
    ResultStorageConfig,
    RedisStorageConfig,
    ResultStorageStats,
)

from .factory import (
    ResultStoreFactory,
    create_result_store_from_settings,
)

from .url_parser import (
    parse_store_type_from_url,
    parse_storage_config,
    URL_SCHEME_MAPPING,
)

__all__ = [
    # 抽象基类和接口
    'ResultStore',
    
    # 异常类
    'ResultStorageError',
    'ResultStoreConnectionError', 
    'ResultStoreTimeoutError',
    'ResultNotFoundError',
    'ResultSerializationError',
    'ResultStorageConfigError',
    
    # 数据模型
    'JobResult',
    'ResultStorageConfig',
    'RedisStorageConfig',
    'ResultStorageStats',
    
    # 工厂和实现
    'ResultStoreFactory',
    'create_result_store_from_settings',
    
    # URL 解析
    'parse_store_type_from_url',
    'parse_storage_config',
    'URL_SCHEME_MAPPING',
]