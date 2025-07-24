# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 15:02
# @File           : worker
# @IDE            : PyCharm
# @desc           : Worker 核心实现 - 使用 Python 3.12 现代语法

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import traceback
import uuid
from collections.abc import Callable, Coroutine, Sequence
from datetime import datetime, timedelta
from functools import partial
from signal import Signals
from typing import Any

from aio_pika import connect_robust, IncomingMessage, Message, Channel, RobustConnection

from .connections import RabbitMQSettings
from .protocols import StartupShutdown, WorkerCoroutine
from .models import JobModel, JobContext, JobStatus, WorkerInfo
from .exceptions import Retry, JobTimeout, MaxRetriesExceeded, SerializationError

logger = logging.getLogger('rabbitmq-arq.worker')


class WorkerUtils:
    """
    消费者工具类
    """

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.allow_pick_jobs = True
        self.tasks: dict[str, asyncio.Task] = {}
        self.main_task: asyncio.Task | None = None
        self.on_stop: Callable | None = None
        
        # 任务统计
        self.jobs_complete = 0
        self.jobs_failed = 0
        self.jobs_retried = 0
        
        # Worker 信息
        self.worker_id = uuid.uuid4().hex
        self.worker_info = WorkerInfo(
            worker_id=self.worker_id,
            start_time=datetime.now()
        )
        
        self._add_signal_handler(signal.SIGINT, self.handle_sig_wait_for_completion)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig_wait_for_completion)

    def handle_sig_wait_for_completion(self, signum: Signals) -> None:
        """
        允许任务在给定时间内完成后再关闭 worker 的信号处理器。
        可通过 `wait_for_job_completion_on_signal_second` 配置时间。
        收到信号后 worker 将停止获取新任务。
        """
        sig = Signals(signum)
        logger.info('正在优雅关闭，设置 allow_pick_jobs 为 False')
        self.allow_pick_jobs = False
        logger.info(
            '收到信号 %s ◆ %d 个任务完成 ◆ %d 个失败 ◆ %d 个重试 ◆ %d 个待完成',
            sig.name,
            self.jobs_complete,
            self.jobs_failed,
            self.jobs_retried,
            len(self.tasks),
        )
        self.loop.create_task(self._wait_for_tasks_to_complete(signum=sig))

    async def _wait_for_tasks_to_complete(self, signum: Signals) -> None:
        """
        等待任务完成，直到达到 `wait_for_job_completion_on_signal_second`。
        """
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self._sleep_until_tasks_complete(),
                self._job_completion_wait,
            )
        logger.info(
            '关闭信号 %s，等待完成 ◆ %d 个任务完成 ◆ %d 个失败 ◆ %d 个重试 ◆ %d 个正在取消',
            signum.name,
            self.jobs_complete,
            self.jobs_failed,
            self.jobs_retried,
            sum(not t.done() for t in self.tasks.values()),
        )
        for t in self.tasks.values():
            if not t.done():
                t.cancel()
        self.main_task and self.main_task.cancel()
        self.on_stop and self.on_stop(signum)

    async def _sleep_until_tasks_complete(self) -> None:
        """
        等待所有任务完成。与 asyncio.wait_for() 一起使用。
        """
        while len(self.tasks):
            await asyncio.sleep(0.1)

    def _add_signal_handler(self, signum: Signals, handler: Callable[[Signals], None]) -> None:
        try:
            self.loop.add_signal_handler(signum, partial(handler, signum))
        except NotImplementedError:  # pragma: no cover
            logger.debug('Windows 不支持向事件循环添加信号处理器')


class Worker(WorkerUtils):
    """
    消费者基类。
    该类用于实现 RabbitMQ 消费者的核心逻辑，支持自定义启动、关闭、任务开始和结束的钩子函数，
    并可通过 ctx 传递上下文信息。
    """

    def __init__(self,
                 functions: Sequence[WorkerCoroutine | Callable] = (),
                 *,
                 rabbitmq_settings: RabbitMQSettings | None = None,
                 on_startup: StartupShutdown | None = None,
                 on_shutdown: StartupShutdown | None = None,
                 on_job_start: StartupShutdown | None = None,
                 on_job_end: StartupShutdown | None = None,
                 after_job_end: StartupShutdown | None = None,
                 ctx: dict[Any, Any] | None = None,
                 ) -> None:
        """
        初始化 Worker 实例。
        :param functions: 需要注册的任务函数序列，可以是同步或异步的。
        :param rabbitmq_settings: RabbitMQ 连接配置。
        :param on_startup: 启动时执行的钩子函数。
        :param on_shutdown: 关闭时执行的钩子函数。
        :param on_job_start: 每个任务开始前执行的钩子函数。
        :param on_job_end: 每个任务结束后执行的钩子函数。
        :param after_job_end: 每个任务结束后额外执行的钩子函数。
        :param ctx: 运行时上下文信息（字典），可用于在各钩子间传递数据。
        """
        super().__init__()
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.on_job_start = on_job_start
        self.on_job_end = on_job_end
        self.after_job_end = after_job_end

        self.ctx = ctx or {}

        self.rabbitmq_settings = rabbitmq_settings or RabbitMQSettings()
        
        # 构建函数映射
        self.functions_map: dict[str, Callable] = {}
        for func in functions:
            if isinstance(func, str):
                # TODO: 支持字符串导入
                raise NotImplementedError("字符串函数导入尚未实现")
            else:
                func_name = getattr(func, '__name__', str(func))
                self.functions_map[func_name] = func
        
        # 设置日志级别
        logger.setLevel(self.rabbitmq_settings.log_level)
        
        # 任务完成等待时间
        self._job_completion_wait = self.rabbitmq_settings.job_completion_wait
        
        # 连接和通道
        self.connection: RobustConnection | None = None
        self.channel: Channel | None = None
        self.dlq_channel: Channel | None = None
        
        # 健康检查任务
        self._health_check_task: asyncio.Task | None = None

    async def _init(self):
        """
        初始化 RabbitMQ 连接和通道。
        """
        if not self.rabbitmq_settings:
            raise ValueError("RabbitMQ 配置未提供！")
            
        logger.info(f"正在连接到 RabbitMQ: {self.rabbitmq_settings.rabbitmq_url}")
        self.connection = await connect_robust(self.rabbitmq_settings.rabbitmq_url)
        self.channel = await self.connection.channel()
        self.dlq_channel = await self.connection.channel()
        
        # 设置预取数量
        await self.channel.set_qos(prefetch_count=self.rabbitmq_settings.prefetch_count)
        
        # 声明队列
        self.rabbitmq_queue = self.rabbitmq_settings.rabbitmq_queue
        self.rabbitmq_dlq = self.rabbitmq_settings.rabbitmq_dlq
        
        await self.channel.declare_queue(self.rabbitmq_queue, durable=True)
        await self.dlq_channel.declare_queue(self.rabbitmq_dlq, durable=True)
        
        logger.info(f"成功连接到 RabbitMQ，队列: {self.rabbitmq_queue}")

    async def on_message(self, message: IncomingMessage):
        """
        处理 RabbitMQ 消息的回调方法，包含重试和失败转死信队列逻辑。
        """
        job_id = None
        async with message.process():
            headers = message.headers or {}
            retry_count = headers.get("x-retry-count", 0)
            
            try:
                # 解析消息
                job_data = json.loads(message.body.decode())
                job = JobModel(**job_data)
                job_id = job.job_id
                
                # 检查是否允许接收新任务
                if not self.allow_pick_jobs:
                    logger.warning(f"Worker 正在关闭，拒绝任务: {job_id}")
                    await message.reject(requeue=True)
                    return
                
                # 创建任务并执行
                task = asyncio.create_task(self._execute_job(job))
                self.tasks[job_id] = task
                
                # 等待任务完成
                await task
                
            except json.JSONDecodeError as e:
                logger.error(f"消息解析失败: {e}\n{message.body}")
                # 无法解析的消息直接发送到死信队列
                await self._send_to_dlq(message.body, headers)
                
            except Exception as e:
                logger.error(f"处理消息时发生错误: {e}\n{traceback.format_exc()}")
                retry_count += 1
                
                if retry_count >= self.rabbitmq_settings.max_retries:
                    # 超过最大重试次数，发送到死信队列
                    await self._send_to_dlq(message.body, headers)
                    logger.error(f"任务 {job_id} 已重试 {retry_count} 次，发送到死信队列")
                else:
                    # 重新入队进行重试
                    await self._requeue_message(message.body, headers, retry_count)
                    logger.info(f"任务 {job_id} 第 {retry_count} 次重试")
            
            finally:
                # 从任务列表中移除
                if job_id and job_id in self.tasks:
                    del self.tasks[job_id]

    async def _execute_job(self, job: JobModel) -> Any:
        """
        执行单个任务
        """
        job.start_time = datetime.now()
        job.status = JobStatus.IN_PROGRESS
        self.worker_info.jobs_ongoing = len(self.tasks)
        
        # 构建任务上下文
        job_ctx = JobContext(
            job_id=job.job_id,
            job_try=job.job_try,
            enqueue_time=job.enqueue_time,
            start_time=job.start_time,
            queue_name=job.queue_name,
            worker_id=self.worker_id,
            extra=self.ctx
        )
        
        try:
            # 调用 on_job_start 钩子
            if self.on_job_start:
                # 传递任务上下文和 Worker 统计信息
                hook_ctx = job_ctx.dict()
                hook_ctx['worker_stats'] = {
                    'jobs_complete': self.jobs_complete,
                    'jobs_failed': self.jobs_failed,
                    'jobs_retried': self.jobs_retried,
                    'jobs_ongoing': len(self.tasks)
                }
                await self.on_job_start(hook_ctx)
            
            # 获取要执行的函数
            func = self.functions_map.get(job.function)
            if not func:
                raise ValueError(f"未找到函数: {job.function}")
            
            # 执行函数（带超时控制）
            logger.info(f"开始执行任务 {job.job_id} - {job.function}")
            
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(
                    func(job_ctx, *job.args, **job.kwargs),
                    timeout=self.rabbitmq_settings.job_timeout
                )
            else:
                # 同步函数在线程池中执行
                result = await asyncio.wait_for(
                    self.loop.run_in_executor(None, func, job_ctx, *job.args, **job.kwargs),
                    timeout=self.rabbitmq_settings.job_timeout
                )
            
            # 任务成功完成
            job.status = JobStatus.COMPLETED
            job.result = result
            job.end_time = datetime.now()
            self.jobs_complete += 1
            
            logger.info(f"任务 {job.job_id} 执行成功，耗时 {(job.end_time - job.start_time).total_seconds():.2f} 秒")
            
            # 无需更新全局统计，将通过钩子传递
            
        except asyncio.TimeoutError:
            job.status = JobStatus.FAILED
            job.error = f"任务执行超时 ({self.rabbitmq_settings.job_timeout}秒)"
            self.jobs_failed += 1
            logger.error(f"任务 {job.job_id} 执行超时")
            raise JobTimeout(job.error)
            
        except Retry as e:
            job.status = JobStatus.RETRYING
            job.error = str(e)
            self.jobs_retried += 1
            logger.warning(f"任务 {job.job_id} 请求重试: {e}")
            
            # 无需更新全局统计，将通过钩子传递
            
            # 计算重试延迟
            if e.defer:
                if isinstance(e.defer, timedelta):
                    defer_seconds = e.defer.total_seconds()
                else:
                    defer_seconds = float(e.defer)
            else:
                # 指数退避
                defer_seconds = self.rabbitmq_settings.retry_backoff * (2 ** (job.job_try - 1))
            
            # 重新入队
            job.job_try += 1
            await self._enqueue_job_retry(job, defer_seconds)
            
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error = f"{type(e).__name__}: {str(e)}"
            self.jobs_failed += 1
            logger.error(f"任务 {job.job_id} 执行失败: {job.error}\n{traceback.format_exc()}")
            
            # 无需更新全局统计，将通过钩子传递
            
            raise
            
        finally:
            job.end_time = datetime.now()
            
            # 调用 on_job_end 钩子
            if self.on_job_end:
                # 传递任务上下文和更新后的 Worker 统计信息
                hook_ctx = job_ctx.dict()
                hook_ctx['worker_stats'] = {
                    'jobs_complete': self.jobs_complete,
                    'jobs_failed': self.jobs_failed,
                    'jobs_retried': self.jobs_retried,
                    'jobs_ongoing': len(self.tasks)
                }
                await self.on_job_end(hook_ctx)
            
            # 调用 after_job_end 钩子
            if self.after_job_end:
                hook_ctx = job_ctx.dict()
                hook_ctx['worker_stats'] = {
                    'jobs_complete': self.jobs_complete,
                    'jobs_failed': self.jobs_failed,
                    'jobs_retried': self.jobs_retried,
                    'jobs_ongoing': len(self.tasks)
                }
                await self.after_job_end(hook_ctx)
            
            # 更新 Worker 信息
            self.worker_info.jobs_complete = self.jobs_complete
            self.worker_info.jobs_failed = self.jobs_failed
            self.worker_info.jobs_retried = self.jobs_retried
            self.worker_info.jobs_ongoing = len(self.tasks)

    async def _enqueue_job_retry(self, job: JobModel, defer_seconds: float):
        """
        重新入队任务进行重试
        """
        if job.job_try > self.rabbitmq_settings.max_retries:
            raise MaxRetriesExceeded(f"任务 {job.job_id} 已超过最大重试次数 {self.rabbitmq_settings.max_retries}")
        
        # 更新延迟执行时间
        job.defer_until = datetime.now() + timedelta(seconds=defer_seconds)
        
        # 序列化任务
        message_body = json.dumps(job.dict(), ensure_ascii=False, default=str).encode()
        
        # 发送消息
        await self.channel.default_exchange.publish(
            Message(
                body=message_body,
                headers={"x-retry-count": job.job_try - 1}
            ),
            routing_key=self.rabbitmq_queue
        )
        
        logger.info(f"任务 {job.job_id} 已重新入队，将在 {defer_seconds} 秒后执行")

    async def _send_to_dlq(self, body: bytes, headers: dict[str, Any]):
        """
        将消息发送到死信队列
        """
        await self.dlq_channel.default_exchange.publish(
            Message(body=body, headers=headers),
            routing_key=self.rabbitmq_dlq
        )

    async def _requeue_message(self, body: bytes, headers: dict[str, Any], retry_count: int):
        """
        重新将消息入队并增加重试次数
        """
        new_headers = dict(headers)
        new_headers["x-retry-count"] = retry_count
        
        # 计算延迟时间（指数退避）
        delay_seconds = self.rabbitmq_settings.retry_backoff * (2 ** (retry_count - 1))
        
        # 延迟发送
        await asyncio.sleep(delay_seconds)
        
        await self.channel.default_exchange.publish(
            Message(body=body, headers=new_headers),
            routing_key=self.rabbitmq_queue
        )

    async def _health_check_loop(self):
        """
        健康检查循环
        """
        while self.allow_pick_jobs:
            try:
                self.worker_info.last_health_check = datetime.now()
                # TODO: 可以在这里添加更多健康检查逻辑，比如写入 Redis
                logger.debug(f"健康检查 - Worker {self.worker_id} 正常运行")
                await asyncio.sleep(self.rabbitmq_settings.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"健康检查失败: {e}")

    async def consume(self):
        """
        开始消费消息
        """
        logger.info(f"[*] 等待队列 {self.rabbitmq_queue} 中的消息。按 CTRL+C 退出")
        queue = await self.channel.declare_queue(self.rabbitmq_queue, durable=True)
        
        # 开始健康检查
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        # 开始消费消息
        await queue.consume(lambda message: asyncio.create_task(self.on_message(message)))
        
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            logger.info("消费者被取消")
        finally:
            if self._health_check_task:
                self._health_check_task.cancel()

    async def main(self):
        """
        Worker 主函数
        """
        try:
            # 启动钩子
            if self.on_startup:
                logger.info("执行启动钩子")
                await self.on_startup(self.ctx)
            
            # 初始化连接
            await self._init()
            
            # 记录主任务
            self.main_task = asyncio.current_task()
            
            # 开始消费
            await self.consume()
            
        except KeyboardInterrupt:
            logger.info("收到键盘中断信号")
        except Exception as e:
            logger.error(f"Worker 运行出错: {e}\n{traceback.format_exc()}")
            raise
        finally:
            # 关闭钩子
            if self.on_shutdown:
                logger.info("执行关闭钩子")
                await self.on_shutdown(self.ctx)
            
            # 关闭连接
            if self.connection:
                await self.connection.close()
                logger.info("已关闭 RabbitMQ 连接")
    
    @classmethod
    def run(cls, worker_settings):
        """
        运行 Worker 的类方法
        
        Args:
            worker_settings: WorkerSettings 类或对象，包含 Worker 配置
        """
        # 从 settings 中提取配置
        settings_dict = {}
        
        # 定义 Worker.__init__ 接受的有效参数
        valid_params = {
            'functions', 'rabbitmq_settings', 'on_startup', 'on_shutdown', 
            'on_job_start', 'on_job_end', 'after_job_end', 'ctx'
        }
        
        # 获取配置属性
        if hasattr(worker_settings, '__dict__'):
            # 实例对象
            for attr, value in worker_settings.__dict__.items():
                if attr in valid_params:
                    settings_dict[attr] = value
        else:
            # 类
            for attr in dir(worker_settings):
                if attr in valid_params:
                    value = getattr(worker_settings, attr)
                    if not callable(value):
                        settings_dict[attr] = value
        
        # 创建 Worker 实例
        worker = cls(**settings_dict)
        
        # 运行
        asyncio.run(worker.main())
