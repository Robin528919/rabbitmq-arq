# -*- coding: utf-8 -*-
"""
结果存储工厂模块

提供统一的结果存储创建接口，支持多种存储后端
"""

from __future__ import annotations

import logging
from typing import Any

from .base import ResultStore
from .url_parser import parse_store_type_from_url, parse_storage_config

logger = logging.getLogger('rabbitmq-arq.result_storage.factory')


class ResultStoreFactory:
    """结果存储工厂类
    
    根据配置创建对应的存储后端实例
    """

    # 注册的存储后端类型
    _STORE_TYPES = {}

    @classmethod
    def register_store_type(cls, store_type: str, store_class: type[ResultStore]) -> None:
        """注册新的存储后端类型
        
        Args:
            store_type: 存储类型名称
            store_class: 存储类
        """
        cls._STORE_TYPES[store_type] = store_class
        logger.debug(f"已注册存储后端类型: {store_type} -> {store_class.__name__}")

    @classmethod
    def create_store(cls,
                     store_type: str = 'redis',
                     config: dict[str, Any] | None = None) -> ResultStore:
        """创建结果存储实例
        
        Args:
            store_type: 存储类型 ('redis', 'database', 'mongodb' 等)
            config: 存储配置字典
            
        Returns:
            对应的存储实例
            
        Raises:
            ValueError: 不支持的存储类型
            ImportError: 缺少依赖包（如 Redis）
        """
        if store_type not in cls._STORE_TYPES:
            available_types = ', '.join(cls._STORE_TYPES.keys())
            raise ValueError(f"不支持的存储类型: {store_type}。可用类型: {available_types}")

        store_class = cls._STORE_TYPES[store_type]

        try:
            return store_class(config)
        except ImportError as e:
            logger.error(f"创建 {store_type} 存储失败，缺少依赖: {e}")
            raise
        except Exception as e:
            logger.error(f"创建 {store_type} 存储失败: {e}")
            raise


# 注册 Redis 存储（可选依赖）
try:
    from .redis import RedisResultStore

    ResultStoreFactory.register_store_type('redis', RedisResultStore)
    logger.debug("Redis 存储后端已注册")
except ImportError:
    logger.debug("Redis 依赖未安装，跳过注册 Redis 存储后端")


def create_result_store_from_settings(
        store_url: str = "redis://localhost:6379/0",
        ttl: int = 86400,
        **kwargs: Any
) -> ResultStore | None:
    """从设置创建结果存储实例
    
    基于URL自动识别存储类型并创建相应的存储实例
    
    Args:
        store_url: 存储URL，通过URL自动识别存储类型
        ttl: 结果过期时间（秒）
        **kwargs: 其他配置参数
        
    Returns:
        存储实例，如果 enabled=False 则返回 None
    """

    if not store_url:
        logger.warning("存储URL为空，禁用结果存储")
        return None

    # 通过URL解析存储类型和配置
    try:
        store_type = parse_store_type_from_url(store_url)
        config = parse_storage_config(store_url)
        config.update({'ttl': ttl, **kwargs})
    except Exception as e:
        logger.error(f"解析存储URL失败: {e}")
        raise

    try:
        store = ResultStoreFactory.create_store(store_type, config)
        logger.info(f"结果存储已创建: {store_type} ({store_url})")
        return store
    except Exception as e:
        logger.error(f"创建结果存储失败: {e}")
        # 在分布式环境下，不提供内存降级，直接抛出异常
        raise
