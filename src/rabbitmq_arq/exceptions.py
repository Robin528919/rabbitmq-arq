# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 19:30
# @File           : exceptions
# @IDE            : PyCharm
# @desc           : 自定义异常类定义

from __future__ import annotations

from datetime import timedelta


class RabbitMQArqException(Exception):
    """rabbitmq-arq 基础异常类"""
    pass


class JobException(RabbitMQArqException):
    """任务相关异常基类"""
    pass


class Retry(JobException):
    """
    特殊异常，用于重试任务（如果还未达到最大重试次数）。
    
    这个异常用于在任务执行过程中请求重试，支持延迟重试。
    
    Args:
        defer: 延迟重试的时间，可以是秒数或 timedelta 对象
        
    Example:
        ```python
        # 立即重试
        raise Retry()
        
        # 延迟 10 秒重试
        raise Retry(defer=10)
        
        # 使用 timedelta 延迟重试
        raise Retry(defer=timedelta(minutes=5))
        ```
    """
    
    def __init__(self, defer: int | float | timedelta | None = None) -> None:
        self.defer = defer
        
        if defer is None:
            message = '任务需要重试'
        elif isinstance(defer, timedelta):
            message = f'任务需要重试，延迟 {defer.total_seconds()} 秒'
        else:
            message = f'任务需要重试，延迟 {defer} 秒'
            
        super().__init__(message)


class JobNotFound(JobException):
    """任务未找到异常"""
    
    def __init__(self, job_id: str | None = None) -> None:
        message = f"任务未找到: {job_id}" if job_id else "任务未找到"
        super().__init__(message)
        self.job_id = job_id


class JobAlreadyExists(JobException):
    """任务已存在异常"""
    
    def __init__(self, job_id: str | None = None) -> None:
        message = f"任务已存在: {job_id}" if job_id else "任务已存在"
        super().__init__(message)
        self.job_id = job_id


class JobTimeout(JobException):
    """任务执行超时异常"""
    
    def __init__(self, timeout: int | float | None = None, job_id: str | None = None) -> None:
        if timeout and job_id:
            message = f"任务 {job_id} 执行超时 ({timeout} 秒)"
        elif timeout:
            message = f"任务执行超时 ({timeout} 秒)"
        elif job_id:
            message = f"任务 {job_id} 执行超时"
        else:
            message = "任务执行超时"
        
        super().__init__(message)
        self.timeout = timeout
        self.job_id = job_id


class JobAborted(JobException):
    """任务被中止异常"""
    
    def __init__(self, reason: str | None = None, job_id: str | None = None) -> None:
        if reason and job_id:
            message = f"任务 {job_id} 被中止: {reason}"
        elif reason:
            message = f"任务被中止: {reason}"
        elif job_id:
            message = f"任务 {job_id} 被中止"
        else:
            message = "任务被中止"
        
        super().__init__(message)
        self.reason = reason
        self.job_id = job_id


class MaxRetriesExceeded(JobException):
    """超过最大重试次数异常"""
    
    def __init__(self, max_retries: int | None = None, job_id: str | None = None) -> None:
        if max_retries and job_id:
            message = f"任务 {job_id} 超过最大重试次数 {max_retries}"
        elif max_retries:
            message = f"超过最大重试次数 {max_retries}"
        elif job_id:
            message = f"任务 {job_id} 超过最大重试次数"
        else:
            message = "超过最大重试次数"
        
        super().__init__(message)
        self.max_retries = max_retries
        self.job_id = job_id


class SerializationError(RabbitMQArqException):
    """序列化/反序列化错误"""
    
    def __init__(self, message: str | None = None, original_error: Exception | None = None) -> None:
        if message:
            super().__init__(message)
        else:
            super().__init__("序列化/反序列化错误")
        self.original_error = original_error


class ConfigurationError(RabbitMQArqException):
    """配置错误"""
    
    def __init__(self, message: str | None = None, config_key: str | None = None) -> None:
        if message and config_key:
            full_message = f"配置错误 ({config_key}): {message}"
        elif message:
            full_message = f"配置错误: {message}"
        else:
            full_message = "配置错误"
        
        super().__init__(full_message)
        self.config_key = config_key


class RabbitMQConnectionError(RabbitMQArqException):
    """RabbitMQ 连接错误"""
    
    def __init__(self, message: str | None = None, url: str | None = None) -> None:
        if message and url:
            full_message = f"RabbitMQ 连接错误 ({url}): {message}"
        elif message:
            full_message = f"RabbitMQ 连接错误: {message}"
        else:
            full_message = "RabbitMQ 连接错误"
        
        super().__init__(full_message)
        self.url = url 