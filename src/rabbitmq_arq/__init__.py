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

__version__ = "0.2.0"

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
