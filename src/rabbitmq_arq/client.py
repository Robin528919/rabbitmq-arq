# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 20:00
# @File           : client
# @IDE            : PyCharm
# @desc           : RabbitMQ 客户端，用于提交任务

from __future__ import annotations

import json
import uuid
import logging
from datetime import datetime, timedelta
from typing import Any

from aio_pika import connect_robust, Message, RobustConnection

from .connections import RabbitMQSettings
from .models import JobModel, JobStatus
from .exceptions import JobAlreadyExists, SerializationError

# 获取日志记录器
logger = logging.getLogger('rabbitmq-arq.client')


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
        
        # 延迟机制检测标志
        self._use_delayed_exchange = False
        self._delayed_exchange_name = f"delayed.{self.rabbitmq_settings.rabbitmq_queue}"
        self._delay_queue_name = f"{self.rabbitmq_settings.rabbitmq_queue}_delay"
        self._delay_mechanism_detected = False
        
    async def connect(self):
        """
        连接到 RabbitMQ 并检测延迟机制
        """
        if not self.connection or self.connection.is_closed:
            self.connection = await connect_robust(self.rabbitmq_settings.rabbitmq_url)
            self.channel = await self.connection.channel()
            
            # 声明主队列
            await self.channel.declare_queue(self.rabbitmq_settings.rabbitmq_queue, durable=True)
            
            # 检测延迟机制（只检测一次）
            if not self._delay_mechanism_detected:
                await self._detect_delay_mechanism()
                self._delay_mechanism_detected = True
    
    async def _detect_delay_mechanism(self):
        """
        检测并设置延迟机制：优先使用延迟插件，其次使用 TTL + DLX
        """
        try:
            # 尝试声明延迟交换机（需要 rabbitmq_delayed_message_exchange 插件）
            delayed_exchange = await self.channel.declare_exchange(
                self._delayed_exchange_name,
                type='x-delayed-message',  # 特殊的延迟消息类型
                durable=True,
                arguments={
                    'x-delayed-type': 'direct'  # 实际的路由类型
                }
            )
            
            # 绑定延迟交换机到主队列
            queue = await self.channel.get_queue(self.rabbitmq_settings.rabbitmq_queue)
            await queue.bind(delayed_exchange, routing_key=self.rabbitmq_settings.rabbitmq_queue)
            
            self._use_delayed_exchange = True
            logger.info("✅ 客户端检测到 RabbitMQ 延迟插件，使用延迟交换机模式")
            
        except Exception as e:
            # 插件未安装或声明失败，降级到 TTL + DLX 方案
            logger.warning(f"⚠️ 客户端未检测到 RabbitMQ 延迟插件: {e}")
            logger.info("📌 客户端降级使用 TTL + Dead Letter Exchange 方案")
            
            # 声明 TTL 延迟队列
            await self.channel.declare_queue(
                self._delay_queue_name,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': '',  # 默认交换机
                    'x-dead-letter-routing-key': self.rabbitmq_settings.rabbitmq_queue  # 路由到主队列
                }
            )
            
            self._use_delayed_exchange = False
    
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
            message_body = json.dumps(job.model_dump(), ensure_ascii=False, default=str).encode()
        except Exception as e:
            raise SerializationError(f"任务序列化失败: {e}")
        
        # 检查是否需要延迟执行
        if defer_until and defer_until > datetime.now():
            delay_seconds = (defer_until - datetime.now()).total_seconds()
            
            # 为延迟任务添加标记，避免 Worker 重复处理延迟
            headers = {"x-retry-count": 0, "x-client-delayed": "true"}
            
            # 清除延迟时间，避免 Worker 重复延迟
            job_copy = job.model_copy()
            job_copy.defer_until = None
            delayed_message_body = json.dumps(job_copy.model_dump(), ensure_ascii=False, default=str).encode()
            
            await self._send_delayed_job(delayed_message_body, queue_name, delay_seconds, headers)
            logger.info(f"📤 延迟任务已提交: {job.job_id} (延迟 {delay_seconds:.1f} 秒)")
        else:
            # 立即执行的任务，发送到普通队列
            await self.channel.default_exchange.publish(
                Message(
                    body=message_body,
                    headers={"x-retry-count": 0}
                ),
                routing_key=queue_name
            )
        
        return job
    
    async def _send_delayed_job(self, message_body: bytes, queue_name: str, delay_seconds: float, headers: dict | None = None):
        """
        发送延迟任务，自动选择最佳延迟机制
        """
        if headers is None:
            headers = {"x-retry-count": 0}
        
        if self._use_delayed_exchange:
            # 使用延迟插件（最优方案）
            delay_ms = int(delay_seconds * 1000)
            headers['x-delay'] = delay_ms
            
            # 获取延迟交换机并发送
            delayed_exchange = await self.channel.get_exchange(self._delayed_exchange_name)
            await delayed_exchange.publish(
                Message(body=message_body, headers=headers),
                routing_key=queue_name
            )
            logger.debug(f"🚀 使用延迟交换机发送任务 (延迟 {delay_seconds:.1f} 秒)")
            
        else:
            # 使用 TTL + DLX 方案（降级方案）
            expiration = timedelta(seconds=delay_seconds)
            
            # 发送到 TTL 延迟队列
            await self.channel.default_exchange.publish(
                Message(
                    body=message_body,
                    headers=headers,
                    expiration=expiration
                ),
                routing_key=self._delay_queue_name
            )
            logger.debug(f"⏱️ 使用 TTL 队列发送任务 (延迟 {delay_seconds:.1f} 秒)")
    
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