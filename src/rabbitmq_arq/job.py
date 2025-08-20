# -*- coding: utf-8 -*-
"""
Job对象 - 模仿ARQ库的优雅设计

提供统一的任务操作接口，包括结果查询、状态检查、任务中止等功能。
"""

from __future__ import annotations

import asyncio
from typing import Any, TYPE_CHECKING

from .exceptions import JobNotFound, ResultNotFound
from .models import JobStatus

if TYPE_CHECKING:
    from .result_storage.models import JobResult
    from .result_storage.base import ResultStore


class Job:
    """
    任务对象 - 提供优雅的任务操作接口
    
    模仿ARQ库的设计，将所有任务相关操作集中到Job对象中。
    """

    def __init__(self, job_id: str, result_store: ResultStore | None = None):
        """
        初始化Job对象
        
        Args:
            job_id: 任务ID
            result_store: 结果存储实例
        """
        self.job_id = job_id
        self._result_store = result_store

    @property
    def id(self) -> str:
        """任务ID属性（兼容ARQ API）"""
        return self.job_id

    async def info(self) -> JobResult:
        """
        获取任务详细信息
        
        Returns:
            JobResult对象，包含任务的完整信息
            
        Raises:
            JobNotFound: 任务不存在
            ResultNotFound: 结果存储未初始化
        """
        if not self._result_store:
            raise ResultNotFound("结果存储未初始化，无法获取任务信息")

        job_result = await self._result_store.get_result(self.job_id)
        if not job_result:
            raise JobNotFound(f"任务 {self.job_id} 不存在或已过期")

        return job_result

    async def result(self, timeout: int | None = None) -> Any:
        """
        获取任务执行结果 - ARQ风格API
        
        Args:
            timeout: 等待结果的超时时间（秒），None表示无限等待
            
        Returns:
            任务的执行结果
            
        Raises:
            JobNotFound: 任务不存在
            ResultNotFound: 任务未完成或结果不存在
            RuntimeError: 任务执行失败
        """

        if timeout is not None:
            # 有限等待模式
            for _ in range(timeout):
                try:
                    return await self._get_result()
                except ResultNotFound:
                    await asyncio.sleep(1)

            raise ResultNotFound(f"任务 {self.job_id} 在 {timeout} 秒内未完成")
        else:
            # 无限等待模式（ARQ风格：默认行为）
            while True:
                try:
                    return await self._get_result()
                except ResultNotFound:
                    await asyncio.sleep(1)  # 每秒检查一次

    async def _get_result(self) -> Any:
        """内部方法：获取任务结果"""
        job_info = await self.info()

        if job_info.status == JobStatus.COMPLETED:
            return job_info.result
        elif job_info.status == JobStatus.FAILED:
            raise RuntimeError(f"任务执行失败: {job_info.error}")
        elif job_info.status in (JobStatus.QUEUED, JobStatus.IN_PROGRESS):
            raise ResultNotFound(f"任务 {self.job_id} 尚未完成，当前状态: {job_info.status}")
        else:
            raise ResultNotFound(f"任务 {self.job_id} 状态异常: {job_info.status}")

    async def status(self) -> JobStatus:
        """
        获取任务状态
        
        Returns:
            JobStatus枚举值
            
        Raises:
            JobNotFound: 任务不存在
        """
        if not self._result_store:
            raise ResultNotFound("结果存储未初始化，无法获取任务状态")

        status = await self._result_store.get_status(self.job_id)
        if status is None:
            raise JobNotFound(f"任务 {self.job_id} 不存在或已过期")

        return status

    async def abort(self) -> bool:
        """
        中止任务执行
        
        Returns:
            成功中止返回True，任务已完成或不存在返回False
            
        Note:
            当前实现只是删除任务结果，真正的任务中止需要Worker配合
        """
        if not self._result_store:
            raise ResultNotFound("结果存储未初始化，无法中止任务")

        # 检查任务是否存在且可以中止
        try:
            current_status = await self.status()
            if current_status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.ABORTED):
                return False  # 已完成的任务无法中止
        except JobNotFound:
            return False

        # 删除任务结果（模拟中止）
        # TODO: 真正的实现需要通知Worker停止执行
        return await self._result_store.delete_result(self.job_id)

    async def delete(self) -> bool:
        """
        删除任务结果
        
        Returns:
            删除成功返回True，任务不存在返回False
        """
        if not self._result_store:
            raise ResultNotFound("结果存储未初始化，无法删除任务")

        return await self._result_store.delete_result(self.job_id)

    def __str__(self) -> str:
        """字符串表示"""
        return f"Job(id={self.job_id})"

    def __repr__(self) -> str:
        """调试表示"""
        return f"Job(job_id='{self.job_id}', result_store={self._result_store.__class__.__name__ if self._result_store else None})"
