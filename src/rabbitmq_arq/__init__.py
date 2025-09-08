# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 15:01
# @File           : __init__.py
# @IDE            : PyCharm
# @desc           : RabbitMQ-ARQ - 基于 RabbitMQ 的异步任务队列库

from .worker import Worker, WorkerSettings
from .client import RabbitMQClient
from .connections import RabbitMQSettings
from .exceptions import (
    Retry,
    JobNotFound,
    JobAlreadyExists,
    JobTimeout,
    JobAborted,
    MaxRetriesExceeded,
    SerializationError,
    ConfigurationError,
    RabbitMQConnectionError,
    RabbitMQArqException,
    JobException,
    ResultNotFound
)
from .job import Job
from .models import JobModel, JobContext, JobStatus, WorkerInfo
from .protocols import WorkerCoroutine, StartupShutdown
from .constants import default_queue_name

# 版本号读取：从包的元数据中读取，避免在源码中硬编码版本
# 构建与发布时以 pyproject.toml 中的 [project].version 为单一真相
try:
    from importlib.metadata import PackageNotFoundError, version as _pkg_version
except Exception:  # 兼容性兜底（极少数环境）
    _pkg_version = None
    class PackageNotFoundError(Exception):
        pass

try:
    __version__ = _pkg_version("rabbitmq_arq") if _pkg_version else "0.0.0"
except PackageNotFoundError:
    # 开发态（未安装/可编辑安装元数据缺失）时兜底
    __version__ = "0.0.0"

__all__ = [
    # Worker
    "Worker",
    "WorkerSettings",
    
    # Client
    "RabbitMQClient",

    # Job
    "Job",
    
    # Settings
    "RabbitMQSettings",
    
    # Models
    "JobModel",
    "JobContext", 
    "JobStatus",
    "WorkerInfo",
    
    # Exceptions
    "Retry",
    "JobNotFound",
    "JobAlreadyExists",
    "JobTimeout",
    "JobAborted",
    "MaxRetriesExceeded",
    "SerializationError",
    "ConfigurationError",
    "RabbitMQConnectionError",
    "RabbitMQArqException",
    "JobException",
    "ResultNotFound",
    
    # Types
    "WorkerCoroutine",
    "StartupShutdown",
    
    # Constants
    "default_queue_name"
]
