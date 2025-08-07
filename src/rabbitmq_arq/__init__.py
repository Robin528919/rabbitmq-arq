# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 15:01
# @File           : __init__.py
# @IDE            : PyCharm
# @desc           : RabbitMQ-ARQ - 基于 RabbitMQ 的异步任务队列库

from .worker import Worker, WorkerSettings
from .client import RabbitMQClient, create_client
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
    JobException
)
from .models import JobModel, JobContext, JobStatus, WorkerInfo
from .protocols import WorkerCoroutine, StartupShutdown
from .constants import default_queue_name

__version__ = "0.1.0"

__all__ = [
    # Worker
    "Worker",
    "WorkerSettings",
    
    # Client
    "RabbitMQClient",
    "create_client",
    
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
    
    # Types
    "WorkerCoroutine",
    "StartupShutdown",
    
    # Constants
    "default_queue_name"
]
