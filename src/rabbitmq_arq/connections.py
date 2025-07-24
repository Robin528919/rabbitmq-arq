# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 19:24
# @File           : connections
# @IDE            : PyCharm
# @desc           : RabbitMQ 连接配置

from __future__ import annotations

from . import constants


# RabbitMQ 配置类，用于存储连接和队列相关的配置信息
class RabbitMQSettings:
    """RabbitMQ 连接设置 - 使用 Python 3.12 现代类型注解"""
    
    def __init__(self,
                 rabbitmq_url: str | None = "amqp://guest:guest@localhost:5672/",  # RabbitMQ 服务器的连接地址
                 rabbitmq_queue: str | None = "",  # 主队列名称
                 rabbitmq_dlq: str | None = "",  # 死信队列名称
                 # 高级配置选项
                 max_retries: int = 3,  # 最大重试次数
                 retry_backoff: float = 5.0,  # 重试退避时间（秒）
                 job_timeout: int = 300,  # 任务超时时间（秒）
                 prefetch_count: int = 100,  # 预取消息数量
                 enable_compression: bool = False,  # 是否启用消息压缩
                 job_completion_wait: int = 0,  # 信号接收后等待任务完成的时间（秒）
                 health_check_interval: int = 60,  # 健康检查间隔（秒）
                 log_level: str = "INFO",  # 日志级别
                 enable_job_result_storage: bool = True,  # 是否存储任务结果
                 job_result_ttl: int = 86400,  # 任务结果保存时间（秒）
                 # Burst 模式配置
                 burst_mode: bool = False,  # 是否启用 burst 模式（处理完队列后自动退出）
                 burst_timeout: int = 300,  # burst 模式最大运行时间（秒）
                 burst_check_interval: float = 1.0,  # 队列状态检查间隔（秒）
                 burst_wait_for_tasks: bool = True,  # 退出前是否等待正在执行的任务完成
                 ) -> None:
        """
        初始化 RabbitMQ 连接设置
        
        Args:
            rabbitmq_url: RabbitMQ 服务器连接 URL
            rabbitmq_queue: 主队列名称
            rabbitmq_dlq: 死信队列名称
            max_retries: 最大重试次数
            retry_backoff: 重试退避时间（秒）
            job_timeout: 任务超时时间（秒）
            prefetch_count: 预取消息数量
            enable_compression: 是否启用消息压缩
            job_completion_wait: 信号接收后等待任务完成的时间（秒）
            health_check_interval: 健康检查间隔（秒）
            log_level: 日志级别
            enable_job_result_storage: 是否存储任务结果
            job_result_ttl: 任务结果保存时间（秒）
            burst_mode: 是否启用 burst 模式（处理完队列后自动退出）
            burst_timeout: burst 模式最大运行时间（秒）
            burst_check_interval: 队列状态检查间隔（秒）
            burst_wait_for_tasks: 退出前是否等待正在执行的任务完成
        """
        # RabbitMQ 服务器连接地址
        self.rabbitmq_url = rabbitmq_url
        
        # 主队列名称
        if rabbitmq_queue is None or rabbitmq_queue == "":
            rabbitmq_queue = constants.default_queue_name
        self.rabbitmq_queue = rabbitmq_queue
        
        # 死信队列名称
        if rabbitmq_dlq is None or rabbitmq_dlq == "":
            rabbitmq_dlq = rabbitmq_queue + "_dlq"
        self.rabbitmq_dlq = rabbitmq_dlq
        
        # 高级配置
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.job_timeout = job_timeout
        self.prefetch_count = prefetch_count
        self.enable_compression = enable_compression
        self.job_completion_wait = job_completion_wait
        self.health_check_interval = health_check_interval
        self.log_level = log_level
        self.enable_job_result_storage = enable_job_result_storage
        self.job_result_ttl = job_result_ttl
        
        # Burst 模式配置
        self.burst_mode = burst_mode
        self.burst_timeout = burst_timeout
        self.burst_check_interval = burst_check_interval
        self.burst_wait_for_tasks = burst_wait_for_tasks
        
        # 验证 burst 配置参数
        if self.burst_timeout <= 0:
            raise ValueError("burst_timeout 必须大于 0")
        if self.burst_check_interval <= 0:
            raise ValueError("burst_check_interval 必须大于 0")

    def __repr__(self) -> str:
        """返回配置的字符串表示"""
        return (f"RabbitMQSettings("
                f"url='{self.rabbitmq_url}', "
                f"queue='{self.rabbitmq_queue}', "
                f"dlq='{self.rabbitmq_dlq}', "
                f"max_retries={self.max_retries}, "
                f"burst_mode={self.burst_mode})")
