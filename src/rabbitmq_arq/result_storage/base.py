# -*- coding: utf-8 -*-
"""
任务结果存储抽象基类和异常定义

定义了所有存储后端必须实现的接口规范
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from ..models import JobStatus
from .models import JobResult, ResultStorageStats

# 获取日志记录器
logger = logging.getLogger('rabbitmq-arq.result_storage')


class ResultStorageError(Exception):
    """结果存储基础异常"""
    
    def __init__(self, message: str, store_type: str | None = None):
        self.message = message
        self.store_type = store_type
        super().__init__(self.message)
    
    def __str__(self) -> str:
        if self.store_type:
            return f"[{self.store_type}] {self.message}"
        return self.message


class ResultStoreConnectionError(ResultStorageError):
    """存储连接异常"""
    pass


class ResultStoreTimeoutError(ResultStorageError):
    """存储操作超时"""
    pass


class ResultNotFoundError(ResultStorageError):
    """结果未找到异常"""
    
    def __init__(self, job_id: str, store_type: str | None = None):
        self.job_id = job_id
        message = f"任务结果未找到: {job_id}"
        super().__init__(message, store_type)


class ResultSerializationError(ResultStorageError):
    """结果序列化异常"""
    pass


class ResultStorageConfigError(ResultStorageError):
    """存储配置异常"""
    pass


class ResultStore(ABC):
    """任务结果存储抽象基类
    
    定义了所有存储后端必须实现的接口规范。
    所有具体实现都应该继承此类并实现所有抽象方法。
    """
    
    def __init__(self, config: dict[str, Any] | None = None):
        """初始化存储后端
        
        Args:
            config: 存储配置字典
        """
        self.config = config or {}
        self._stats = ResultStorageStats()
        self._logger = logging.getLogger(f'rabbitmq-arq.result_storage.{self.__class__.__name__.lower()}')
    
    @property
    def stats(self) -> ResultStorageStats:
        """获取存储统计信息"""
        return self._stats
    
    @abstractmethod
    async def store_result(self, job_result: JobResult) -> None:
        """存储任务结果
        
        Args:
            job_result: 要存储的任务结果
            
        Raises:
            ResultStorageError: 存储失败时抛出
        """
        pass
    
    @abstractmethod
    async def get_result(self, job_id: str) -> JobResult | None:
        """获取单个任务结果
        
        Args:
            job_id: 任务ID
            
        Returns:
            任务结果对象，如果不存在则返回None
            
        Raises:
            ResultStorageError: 查询失败时抛出
        """
        pass
    
    @abstractmethod
    async def get_results(self, job_ids: list[str]) -> dict[str, JobResult | None]:
        """批量获取任务结果
        
        Args:
            job_ids: 任务ID列表
            
        Returns:
            任务ID到结果的映射字典，不存在的任务返回None
            
        Raises:
            ResultStorageError: 查询失败时抛出
        """
        pass
    
    @abstractmethod
    async def get_status(self, job_id: str) -> JobStatus | None:
        """获取任务状态
        
        Args:
            job_id: 任务ID
            
        Returns:
            任务状态，如果不存在则返回None
            
        Raises:
            ResultStorageError: 查询失败时抛出
        """
        pass
    
    @abstractmethod
    async def delete_result(self, job_id: str) -> bool:
        """删除任务结果
        
        Args:
            job_id: 任务ID
            
        Returns:
            删除成功返回True，结果不存在返回False
            
        Raises:
            ResultStorageError: 删除失败时抛出
        """
        pass
    
    @abstractmethod
    async def cleanup_expired(self) -> int:
        """清理过期结果
        
        Returns:
            清理的结果数量
            
        Raises:
            ResultStorageError: 清理失败时抛出
        """
        pass
    
    async def get_stats(self) -> dict[str, Any]:
        """获取存储统计信息
        
        Returns:
            包含统计信息的字典
        """
        return {
            'store_type': self.__class__.__name__,
            'stats': self._stats.model_dump(),
            'config': self.config
        }
    
    async def health_check(self) -> bool:
        """健康检查
        
        Returns:
            健康状态，True表示正常
        """
        try:
            # 子类可以重写此方法提供更具体的健康检查
            await self.get_stats()
            return True
        except Exception as e:
            self._logger.warning(f"健康检查失败: {e}")
            return False
    
    @abstractmethod
    async def validate_connection(self) -> bool:
        """验证存储连接
        
        验证存储后端的连接是否可用，这是一个更轻量级的检查方法，
        主要用于启动时验证配置和连接状态。
        
        Returns:
            连接状态，True表示连接正常
            
        Raises:
            ResultStoreConnectionError: 连接验证失败时抛出
        """
        pass
    
    async def close(self) -> None:
        """关闭存储连接
        
        子类可以重写此方法来清理资源
        """
        self._logger.info(f"{self.__class__.__name__} 存储连接已关闭")
    
    def _update_stats_on_store(self) -> None:
        """更新存储统计"""
        self._stats.total_stored += 1
    
    def _update_stats_on_retrieve(self) -> None:
        """更新查询统计"""
        self._stats.total_retrieved += 1
    
    def _update_stats_on_delete(self) -> None:
        """更新删除统计"""
        self._stats.total_deleted += 1
    
    def _update_stats_on_expire(self, count: int = 1) -> None:
        """更新过期统计"""
        self._stats.total_expired += count
    
    def _update_stats_on_error(self, operation: str) -> None:
        """更新错误统计"""
        from datetime import datetime, timezone
        
        if operation in ('store', 'delete'):
            self._stats.storage_errors += 1
        elif operation in ('retrieve', 'get'):
            self._stats.retrieval_errors += 1
        
        self._stats.last_error_at = datetime.now(timezone.utc)
    
    def __str__(self) -> str:
        """存储后端的字符串表示"""
        return f"{self.__class__.__name__}(success_rate={self._stats.success_rate:.2%})"
    
    def __repr__(self) -> str:
        """存储后端的详细表示"""
        return f"{self.__class__.__name__}(config={self.config})"
