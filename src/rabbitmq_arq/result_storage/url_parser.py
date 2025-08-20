# -*- coding: utf-8 -*-
"""
URL-based 存储配置解析器

基于 Celery 的设计理念，通过 URL scheme 自动识别存储类型
"""

from __future__ import annotations

from urllib.parse import urlparse
from typing import Any

from .base import ResultStorageError


# URL scheme 到存储类型的映射
URL_SCHEME_MAPPING = {
    'redis': 'redis',
    'rediss': 'redis',           # Redis SSL
    'postgresql': 'database',
    'postgres': 'database',
    'mysql': 'database',
    'mongodb': 'mongodb',
    's3': 's3',
    'gs': 'gcs',                 # Google Cloud Storage
    'azure': 'azure',
    'elasticsearch': 'elasticsearch',
    'amqp': 'rabbitmq',          # RabbitMQ
    'kafka': 'kafka',
    'tiered': 'tiered',          # 分层存储
    'hybrid': 'hybrid',          # 混合存储
}


def parse_store_type_from_url(url: str) -> str:
    """从 URL 解析存储类型
    
    Args:
        url: 存储 URL
        
    Returns:
        存储类型字符串
        
    Raises:
        ValueError: 不支持的 URL scheme
        ResultStorageError: URL 格式错误
        
    Examples:
        >>> parse_store_type_from_url("redis://localhost:6379/0")
        'redis'
        >>> parse_store_type_from_url("postgresql://user:pass@host/db")
        'database'
        >>> parse_store_type_from_url("s3://bucket/path")
        's3'
    """
    if not url:
        raise ValueError("结果存储URL不能为空")
    
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        
        if not scheme:
            raise ValueError(f"URL 缺少有效的 scheme: {url}")
        
        if scheme in URL_SCHEME_MAPPING:
            return URL_SCHEME_MAPPING[scheme]
        else:
            available_schemes = ', '.join(sorted(URL_SCHEME_MAPPING.keys()))
            raise ValueError(
                f"不支持的存储URL scheme: {scheme}。"
                f"支持的 schemes: {available_schemes}"
            )
    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ResultStorageError(f"解析存储URL失败: {e}") from e


def parse_redis_url(url: str) -> dict[str, Any]:
    """解析 Redis URL 配置
    
    Args:
        url: Redis URL, 格式如 redis://[:password@]host:port/db
        
    Returns:
        Redis 连接配置字典
        
    Examples:
        >>> parse_redis_url("redis://localhost:6379/0")
        {'host': 'localhost', 'port': 6379, 'db': 0}
        >>> parse_redis_url("redis://:password@localhost:6379/1")
        {'host': 'localhost', 'port': 6379, 'db': 1, 'password': 'password'}
    """
    parsed = urlparse(url)
    
    config = {
        'host': parsed.hostname or 'localhost',
        'port': parsed.port or 6379,
        'db': 0
    }
    
    # 解析数据库编号
    if parsed.path and len(parsed.path) > 1:
        try:
            config['db'] = int(parsed.path[1:])  # 去掉开头的 /
        except ValueError:
            raise ValueError(f"无效的 Redis 数据库编号: {parsed.path}")
    
    # 解析密码
    if parsed.password:
        config['password'] = parsed.password
    
    # 处理 SSL
    if parsed.scheme == 'rediss':
        config['ssl'] = True
    
    return config


def parse_database_url(url: str) -> dict[str, Any]:
    """解析数据库 URL 配置
    
    Args:
        url: 数据库 URL
        
    Returns:
        数据库连接配置字典
    """
    parsed = urlparse(url)
    
    config = {
        'url': url,
        'scheme': parsed.scheme,
        'host': parsed.hostname,
        'port': parsed.port,
        'database': parsed.path[1:] if parsed.path else None,  # 去掉开头的 /
        'username': parsed.username,
        'password': parsed.password,
    }
    
    return config


def parse_mongodb_url(url: str) -> dict[str, Any]:
    """解析 MongoDB URL 配置"""
    parsed = urlparse(url)
    
    config = {
        'url': url,
        'host': parsed.hostname or 'localhost',
        'port': parsed.port or 27017,
        'database': parsed.path[1:] if parsed.path else 'default',
        'username': parsed.username,
        'password': parsed.password,
    }
    
    return config


def parse_s3_url(url: str) -> dict[str, Any]:
    """解析 S3 URL 配置"""
    parsed = urlparse(url)
    
    config = {
        'bucket': parsed.hostname,
        'prefix': parsed.path[1:] if parsed.path else '',  # 去掉开头的 /
    }
    
    return config


def parse_storage_config(url: str) -> dict[str, Any]:
    """根据 URL 解析存储配置
    
    Args:
        url: 存储 URL
        
    Returns:
        存储配置字典，包含 'type' 和其他配置参数
    """
    store_type = parse_store_type_from_url(url)
    
    # 分层配置策略：通用配置 + 存储特定配置
    config = {
        'type': store_type,
        'url': url,  # 通用URL字段
    }
    
    # 根据存储类型解析具体配置并添加存储特定的URL字段
    if store_type == 'redis':
        redis_config = parse_redis_url(url)
        config.update(redis_config)
        config['redis_url'] = url  # Redis特定URL字段
    elif store_type == 'database':
        db_config = parse_database_url(url)
        config.update(db_config)
        # database保持url字段（已在parse_database_url中设置）
    elif store_type == 'mongodb':
        mongo_config = parse_mongodb_url(url)
        config.update(mongo_config)
        # mongodb保持url字段（已在parse_mongodb_url中设置）
    elif store_type == 's3':
        s3_config = parse_s3_url(url)
        config.update(s3_config)
        config['s3_url'] = url  # S3特定URL字段
    # 其他存储类型可以继续扩展
    
    return config