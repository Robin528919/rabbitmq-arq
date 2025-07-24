# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 20:00
# @File           : client
# @IDE            : PyCharm
# @desc           : RabbitMQ 客户端，用于提交任务

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from aio_pika import connect_robust, Message, RobustConnection

from .connections import RabbitMQSettings
from .models import JobModel, JobStatus
from .exceptions import JobAlreadyExists, SerializationError


class RabbitMQClient:
    """
    RabbitMQ 客户端，用于提交任务到队列
    
    支持单个和批量任务提交，延迟执行，以及任务生命周期管理。
    使用 Python 3.12 现代类型注解。
    """
    
    def __init__(self, rabbitmq_settings: RabbitMQSettings | None = None) -> None:
        """
        初始化客户端
        
        Args:
            rabbitmq_settings: RabbitMQ 连接配置，如果为 None 则使用默认配置
        """
        self.rabbitmq_settings = rabbitmq_settings or RabbitMQSettings()
        self.connection: RobustConnection | None = None
        self.channel = None
        
    async def connect(self):
        """
        连接到 RabbitMQ
        """
        if not self.connection or self.connection.is_closed:
            self.connection = await connect_robust(self.rabbitmq_settings.rabbitmq_url)
            self.channel = await self.connection.channel()
            
            # 声明队列
            await self.channel.declare_queue(self.rabbitmq_settings.rabbitmq_queue, durable=True)
    
    async def close(self):
        """
        关闭连接
        """
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
    
    async def enqueue_job(
        self,
        function: str,
        *args,
        _job_id: str | None = None,
        _queue_name: str | None = None,
        _defer_until: datetime | None = None,
        _defer_by: int | float | timedelta | None = None,
        _expires: int | float | timedelta | None = None,
        _job_try: int | None = None,
        **kwargs
    ) -> JobModel:
        """
        提交任务到队列
        
        Args:
            function: 要执行的函数名
            *args: 位置参数
            _job_id: 任务 ID，如果不提供则自动生成
            _queue_name: 队列名称，如果不提供则使用默认队列
            _defer_until: 延迟执行到指定时间
            _defer_by: 延迟执行的时间间隔
            _expires: 任务过期时间
            _job_try: 任务尝试次数
            **kwargs: 关键字参数
            
        Returns:
            JobModel: 任务对象
        """
        # 确保连接
        await self.connect()
        
        # 生成任务 ID
        job_id = _job_id or uuid.uuid4().hex
        
        # 队列名称
        queue_name = _queue_name or self.rabbitmq_settings.rabbitmq_queue
        
        # 计算延迟执行时间
        defer_until = None
        if _defer_until:
            defer_until = _defer_until
        elif _defer_by:
            if isinstance(_defer_by, timedelta):
                defer_until = datetime.now() + _defer_by
            else:
                defer_until = datetime.now() + timedelta(seconds=float(_defer_by))
        
        # 计算过期时间
        expires = None
        if _expires:
            if isinstance(_expires, (int, float)):
                expires = datetime.now() + timedelta(seconds=float(_expires))
            elif isinstance(_expires, timedelta):
                expires = datetime.now() + _expires
            else:
                expires = _expires
        else:
            # 默认 24 小时过期
            expires = datetime.now() + timedelta(hours=24)
        
        # 创建任务对象
        job = JobModel(
            job_id=job_id,
            function=function,
            args=list(args),
            kwargs=kwargs,
            job_try=_job_try or 1,
            queue_name=queue_name,
            defer_until=defer_until,
            expires=expires,
            status=JobStatus.QUEUED
        )
        
        # 序列化任务
        try:
            message_body = json.dumps(job.dict(), ensure_ascii=False, default=str).encode()
        except Exception as e:
            raise SerializationError(f"任务序列化失败: {e}")
        
        # 发送消息
        await self.channel.default_exchange.publish(
            Message(
                body=message_body,
                headers={"x-retry-count": 0}
            ),
            routing_key=queue_name
        )
        
        return job
    
    async def enqueue_jobs(
        self,
        jobs: list[dict[str, Any]]
    ) -> list[JobModel]:
        """
        批量提交任务
        
        Args:
            jobs: 任务列表，每个任务是一个字典，包含：
                - function: 函数名
                - args: 位置参数列表
                - kwargs: 关键字参数字典
                - 其他可选参数（_job_id, _queue_name 等）
                
        Returns:
            List[JobModel]: 任务对象列表
        """
        results = []
        for job_spec in jobs:
            function = job_spec.pop('function')
            args = job_spec.pop('args', [])
            kwargs = job_spec.pop('kwargs', {})
            
            # 提取特殊参数
            special_params = {}
            for key in list(job_spec.keys()):
                if key.startswith('_'):
                    special_params[key] = job_spec.pop(key)
            
            # 合并剩余参数到 kwargs
            kwargs.update(job_spec)
            
            # 提交任务
            job = await self.enqueue_job(function, *args, **special_params, **kwargs)
            results.append(job)
        
        return results

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()


async def create_client(
    rabbitmq_settings: RabbitMQSettings | None = None
) -> RabbitMQClient:
    """
    创建并连接客户端
    
    Args:
        rabbitmq_settings: RabbitMQ 连接配置
        
    Returns:
        RabbitMQClient: 已连接的客户端实例
    """
    client = RabbitMQClient(rabbitmq_settings)
    await client.connect()
    return client 