# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 18:58
# @File           : protocols
# @IDE            : PyCharm
# @desc           : 协议定义 - 使用 Python 3.12 现代语法

from __future__ import annotations

from typing import Any, Protocol
from .models import JobContext

class WorkerCoroutine(Protocol):
    """
    Worker 任务函数协议
    
    定义了任务函数必须遵循的接口规范。任务函数必须是异步的，
    第一个参数是任务上下文，后续参数为任务的具体参数。
    """
    __qualname__: str

    async def __call__(self, ctx: JobContext, *args: Any, **kwargs: Any) -> Any:
        """
        执行任务
        
        Args:
            ctx: 任务上下文信息
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            任务执行结果
        """
        ...


class StartupShutdown(Protocol):
    """
    启动/关闭钩子函数协议
    
    定义了 Worker 启动和关闭时执行的钩子函数接口。
    """
    __qualname__: str

    async def __call__(self, ctx: dict[Any, Any]) -> Any:
        """
        执行启动/关闭钩子
        
        Args:
            ctx: 上下文信息，可以用于在不同阶段间传递数据
            
        Returns:
            钩子执行结果（通常为 None）
        """
        ... 