# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 19:24
# @File           : connections
# @IDE            : PyCharm
# @desc           : RabbitMQ 连接配置

from __future__ import annotations

from typing import Sequence, Callable

from .protocols import StartupShutdown, WorkerCoroutine
from .constants import default_queue_name


class RabbitMQSettings:
    """RabbitMQ 连接设置 - 纯连接配置，不包含业务逻辑配置"""

    def __init__(self,
                 # === 基础连接配置 ===
                 rabbitmq_url: str = "amqp://guest:guest@localhost:5672/",  # RabbitMQ 服务器连接地址
                 connection_timeout: int = 30,  # 连接超时时间（秒）
                 heartbeat: int = 60,  # 心跳间隔（秒）

                 # === 连接池配置 ===
                 connection_pool_size: int = 10,  # 连接池大小
                 channel_pool_size: int = 100,  # 通道池大小

                 # === 性能配置 ===
                 prefetch_count: int = 100,  # 预取消息数量（影响连接性能）
                 enable_compression: bool = False,  # 是否启用消息压缩

                 # === 安全配置 ===
                 ssl_enabled: bool = False,  # 是否启用SSL
                 ssl_cert_path: str | None = None,  # SSL证书路径
                 ssl_key_path: str | None = None,  # SSL私钥路径
                 ssl_ca_path: str | None = None,  # SSL CA证书路径
                 ssl_verify: bool = True,  # 是否验证SSL证书

                 # === 重连配置 ===
                 auto_reconnect: bool = True,  # 是否自动重连
                 reconnect_interval: float = 5.0,  # 重连间隔（秒）
                 max_reconnect_attempts: int = 10,  # 最大重连次数
                 ) -> None:
        """
        初始化 RabbitMQ 连接设置
        
        Args:
            rabbitmq_url: RabbitMQ 服务器连接 URL
            connection_timeout: 连接超时时间（秒）
            heartbeat: 心跳间隔（秒）
            connection_pool_size: 连接池大小
            channel_pool_size: 通道池大小
            prefetch_count: 预取消息数量
            enable_compression: 是否启用消息压缩
            ssl_enabled: 是否启用SSL
            ssl_cert_path: SSL证书路径
            ssl_key_path: SSL私钥路径
            ssl_ca_path: SSL CA证书路径
            ssl_verify: 是否验证SSL证书
            auto_reconnect: 是否自动重连
            reconnect_interval: 重连间隔（秒）
            max_reconnect_attempts: 最大重连次数
        """
        # 基础连接配置
        self.rabbitmq_url = rabbitmq_url
        self.connection_timeout = connection_timeout
        self.heartbeat = heartbeat

        # 连接池配置
        self.connection_pool_size = connection_pool_size
        self.channel_pool_size = channel_pool_size

        # 性能配置
        self.prefetch_count = prefetch_count
        self.enable_compression = enable_compression

        # 安全配置
        self.ssl_enabled = ssl_enabled
        self.ssl_cert_path = ssl_cert_path
        self.ssl_key_path = ssl_key_path
        self.ssl_ca_path = ssl_ca_path
        self.ssl_verify = ssl_verify

        # 重连配置
        self.auto_reconnect = auto_reconnect
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts

        # 配置验证
        self._validate_config()

    def _validate_config(self) -> None:
        """验证配置参数"""
        if self.connection_timeout <= 0:
            raise ValueError("connection_timeout 必须大于 0")
        if self.heartbeat <= 0:
            raise ValueError("heartbeat 必须大于 0")
        if self.prefetch_count <= 0:
            raise ValueError("prefetch_count 必须大于 0")
        if self.connection_pool_size <= 0:
            raise ValueError("connection_pool_size 必须大于 0")
        if self.channel_pool_size <= 0:
            raise ValueError("channel_pool_size 必须大于 0")
        if self.reconnect_interval <= 0:
            raise ValueError("reconnect_interval 必须大于 0")
        if self.max_reconnect_attempts < 0:
            raise ValueError("max_reconnect_attempts 必须大于等于 0")

        # SSL 配置验证
        if self.ssl_enabled:
            if not self.ssl_cert_path:
                raise ValueError("启用SSL时必须提供 ssl_cert_path")
            if not self.ssl_key_path:
                raise ValueError("启用SSL时必须提供 ssl_key_path")

    def __repr__(self) -> str:
        """返回配置的字符串表示"""
        return (f"RabbitMQSettings("
                f"url='{self.rabbitmq_url}', "
                f"prefetch_count={self.prefetch_count}, "
                f"ssl_enabled={self.ssl_enabled})")


class WorkerSettings:
    """Worker 配置类 - 包含所有 Worker 相关的配置项"""

    def __init__(self,
                 # === 基础配置 ===
                 rabbitmq_settings: RabbitMQSettings,
                 functions: Sequence[WorkerCoroutine | Callable] = (),
                 worker_name: str | None = None,

                 # === 队列配置 ===
                 queue_name: str = default_queue_name,
                 dlq_name: str | None = None,
                 queue_durable: bool = True,
                 queue_exclusive: bool = False,
                 queue_auto_delete: bool = False,

                 # === 任务处理配置 ===
                 max_retries: int = 3,
                 retry_backoff: float = 5.0,
                 job_timeout: int = 300,
                 max_concurrent_jobs: int = 10,

                 # === 任务结果配置 ===
                 enable_job_result_storage: bool = True,
                 job_result_ttl: int = 86400,

                 # === Worker 运行时配置 ===
                 health_check_interval: int = 30,
                 job_completion_wait: int = 10,
                 graceful_shutdown_timeout: int = 30,

                 # === Burst 模式配置 ===
                 burst_mode: bool = False,
                 burst_timeout: int = 300,
                 burst_check_interval: float = 1.5,
                 burst_wait_for_tasks: bool = True,
                 burst_exit_on_empty: bool = True,

                 # === 日志配置 ===
                 log_level: str = "INFO",
                 log_format: str | None = None,
                 log_file: str | None = None,

                 # === 信号处理配置 ===
                 handle_signals: bool = True,
                 signal_timeout: int = 30,

                 # === 生命周期钩子 ===
                 on_startup: StartupShutdown | None = None,
                 on_shutdown: StartupShutdown | None = None,
                 on_job_start: StartupShutdown | None = None,
                 on_job_end: StartupShutdown | None = None,
                 on_job_success: StartupShutdown | None = None,
                 on_job_failure: StartupShutdown | None = None,

                 # === 监控配置 ===
                 enable_metrics: bool = False,
                 metrics_interval: int = 60,
                 enable_health_endpoint: bool = False,
                 health_endpoint_port: int = 8080,

                 # === 延迟任务配置 ===
                 enable_delayed_jobs: bool = True,
                 delay_mechanism: str = "auto",  # auto, delayed_exchange, ttl_dlx

                 # === 调试配置 ===
                 debug_mode: bool = False,
                 trace_tasks: bool = False
                 ):
        """
        初始化 Worker 配置
        
        Args:
            rabbitmq_settings: RabbitMQ 连接配置
            functions: 任务函数列表
            worker_name: Worker 名称标识
            queue_name: 队列名称
            dlq_name: 死信队列名称（默认为 queue_name + "_dlq"）
            queue_durable: 队列持久化
            queue_exclusive: 队列独占
            queue_auto_delete: 自动删除队列
            max_retries: 最大重试次数
            retry_backoff: 重试退避时间（秒）
            job_timeout: 单任务超时时间（秒）
            max_concurrent_jobs: 最大并发任务数
            enable_job_result_storage: 是否存储任务结果
            job_result_ttl: 任务结果保存时间（秒）
            health_check_interval: 健康检查间隔（秒）
            job_completion_wait: 关闭时等待任务完成时间（秒）
            graceful_shutdown_timeout: 优雅关闭总超时（秒）
            burst_mode: 是否启用 Burst 模式
            burst_timeout: Burst 模式最大运行时间（秒）
            burst_check_interval: 队列检查间隔（秒）
            burst_wait_for_tasks: 退出前等待任务完成
            burst_exit_on_empty: 队列为空时是否退出
            log_level: 日志级别
            log_format: 日志格式
            log_file: 日志文件路径
            handle_signals: 是否处理系统信号
            signal_timeout: 信号处理超时（秒）
            on_startup: 启动时钩子函数
            on_shutdown: 关闭时钩子函数
            on_job_start: 任务开始钩子函数
            on_job_end: 任务结束钩子函数
            on_job_success: 任务成功钩子函数
            on_job_failure: 任务失败钩子函数
            enable_metrics: 启用指标收集
            metrics_interval: 指标收集间隔（秒）
            enable_health_endpoint: 启用健康检查端点
            health_endpoint_port: 健康检查端点端口
            enable_delayed_jobs: 启用延迟任务
            delay_mechanism: 延迟机制（auto/plugin/ttl）
            debug_mode: 调试模式
            trace_tasks: 追踪任务执行
        """
        # === 基础配置 ===
        self.rabbitmq_settings = rabbitmq_settings
        self.functions = functions
        self.worker_name = worker_name

        # === 队列配置 ===
        self.queue_name = queue_name
        self.dlq_name = dlq_name or f"{queue_name}.dlq"  # 默认使用队列名称 + .dlq
        self.queue_durable = queue_durable
        self.queue_exclusive = queue_exclusive
        self.queue_auto_delete = queue_auto_delete

        # === 任务处理配置 ===
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.job_timeout = job_timeout
        self.max_concurrent_jobs = max_concurrent_jobs

        # === 任务结果配置 ===
        self.enable_job_result_storage = enable_job_result_storage
        self.job_result_ttl = job_result_ttl

        # === Worker 运行时配置 ===
        self.health_check_interval = health_check_interval
        self.job_completion_wait = job_completion_wait
        self.graceful_shutdown_timeout = graceful_shutdown_timeout

        # === Burst 模式配置 ===
        self.burst_mode = burst_mode
        self.burst_timeout = burst_timeout
        self.burst_check_interval = burst_check_interval
        self.burst_wait_for_tasks = burst_wait_for_tasks
        self.burst_exit_on_empty = burst_exit_on_empty

        # === 日志配置 ===
        self.log_level = log_level
        self.log_format = log_format
        self.log_file = log_file

        # === 信号处理配置 ===
        self.handle_signals = handle_signals
        self.signal_timeout = signal_timeout

        # === 生命周期钩子 ===
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.on_job_start = on_job_start
        self.on_job_end = on_job_end
        self.on_job_success = on_job_success
        self.on_job_failure = on_job_failure

        # === 监控配置 ===
        self.enable_metrics = enable_metrics
        self.metrics_interval = metrics_interval
        self.enable_health_endpoint = enable_health_endpoint
        self.health_endpoint_port = health_endpoint_port

        # === 延迟任务配置 ===
        self.enable_delayed_jobs = enable_delayed_jobs
        self.delay_mechanism = delay_mechanism

        # === 调试配置 ===
        self.debug_mode = debug_mode
        self.trace_tasks = trace_tasks

        # 验证配置
        self._validate_config()

    def _validate_config(self) -> None:
        """验证配置参数"""
        if self.max_retries < 0:
            raise ValueError("max_retries 必须大于等于 0")
        if self.retry_backoff <= 0:
            raise ValueError("retry_backoff 必须大于 0")
        if self.job_timeout <= 0:
            raise ValueError("job_timeout 必须大于 0")
        if self.max_concurrent_jobs <= 0:
            raise ValueError("max_concurrent_jobs 必须大于 0")
        if self.health_check_interval <= 0:
            raise ValueError("health_check_interval 必须大于 0")
        if self.job_completion_wait < 0:
            raise ValueError("job_completion_wait 必须大于等于 0")
        if self.graceful_shutdown_timeout <= 0:
            raise ValueError("graceful_shutdown_timeout 必须大于 0")
        if self.burst_timeout <= 0:
            raise ValueError("burst_timeout 必须大于 0")
        if self.burst_check_interval <= 0:
            raise ValueError("burst_check_interval 必须大于 0")
        if self.signal_timeout <= 0:
            raise ValueError("signal_timeout 必须大于 0")
        if self.metrics_interval <= 0:
            raise ValueError("metrics_interval 必须大于 0")
        if not (1024 <= self.health_endpoint_port <= 65535):
            raise ValueError("health_endpoint_port 必须在 1024-65535 范围内")
        if self.delay_mechanism not in ("auto", "plugin", "ttl"):
            raise ValueError("delay_mechanism 必须是 'auto', 'plugin' 或 'ttl'")

    def __repr__(self) -> str:
        """返回配置的字符串表示"""
        return (f"WorkerSettings("
                f"worker_name='{self.worker_name}', "
                f"queue_name='{self.queue_name}', "
                f"burst_mode={self.burst_mode}, "
                f"max_concurrent_jobs={self.max_concurrent_jobs})")
