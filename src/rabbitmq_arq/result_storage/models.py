# -*- coding: utf-8 -*-
"""
任务结果存储数据模型定义

使用 Pydantic V2 和 Python 3.12 现代语法
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from ..models import JobStatus


class JobResult(BaseModel):
    """任务结果数据模型"""
    
    job_id: str = Field(..., description="任务ID")
    status: JobStatus = Field(..., description="任务状态")
    result: Any | None = Field(default=None, description="任务结果数据")
    error: str | None = Field(default=None, description="错误信息")
    start_time: datetime = Field(..., description="开始执行时间")
    end_time: datetime | None = Field(default=None, description="结束时间")
    duration: float | None = Field(default=None, description="执行时长(秒)")
    worker_id: str = Field(..., description="执行的 Worker ID")
    queue_name: str = Field(..., description="队列名称")
    retry_count: int = Field(default=0, description="重试次数")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="创建时间")
    expires_at: datetime | None = Field(default=None, description="过期时间")
    
    # 扩展元数据
    function_name: str = Field(..., description="函数名称")
    args: list[Any] = Field(default_factory=list, description="函数参数")
    kwargs: dict[str, Any] = Field(default_factory=dict, description="函数关键字参数")
    
    # Pydantic V2 配置
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=False,
        populate_by_name=True,
        extra='ignore',
        use_enum_values=True,
        # 添加 JSON schema 配置
        json_schema_extra={
            "examples": [
                {
                    "job_id": "abc123",
                    "status": "completed",
                    "result": {"processed": True, "count": 100},
                    "error": None,
                    "start_time": "2025-01-15T10:30:00",
                    "end_time": "2025-01-15T10:30:05",
                    "duration": 5.2,
                    "worker_id": "worker_001",
                    "queue_name": "default",
                    "retry_count": 0,
                    "created_at": "2025-01-15T10:30:00",
                    "expires_at": "2025-01-16T10:30:00",
                    "function_name": "process_data",
                    "args": [123, "test"],
                    "kwargs": {"flag": True}
                }
            ]
        }
    )
    
    @field_serializer('start_time', 'end_time', 'created_at', 'expires_at')
    def serialize_datetime(self, value: datetime | None) -> str | None:
        """序列化 datetime 字段为 ISO 格式字符串"""
        return value.astimezone(timezone.utc).isoformat() if value else None
    
    def __str__(self) -> str:
        """用户友好的字符串表示"""
        status_emoji = {
            JobStatus.QUEUED: "📥",
            JobStatus.IN_PROGRESS: "⏳", 
            JobStatus.COMPLETED: "✅",
            JobStatus.FAILED: "❌",
            JobStatus.RETRYING: "🔄",
            JobStatus.ABORTED: "🛑"
        }
        emoji = status_emoji.get(self.status, "❓")
        duration_str = f"{self.duration:.2f}s" if self.duration else "N/A"
        return f"{emoji} {self.job_id} [{self.function_name}] - {self.status} ({duration_str})"


class ResultStorageConfig(BaseModel):
    """结果存储配置基类"""
    
    enabled: bool = Field(default=True, description="是否启用结果存储")
    ttl: int = Field(default=86400, description="结果保存时间(秒)，默认24小时")
    max_retries: int = Field(default=3, description="存储操作最大重试次数")
    retry_delay: float = Field(default=1.0, description="重试延迟时间(秒)")
    batch_size: int = Field(default=100, description="批量操作大小")
    
    model_config = ConfigDict(
        extra='allow',  # 允许子类扩展配置
        from_attributes=True
    )



class RedisStorageConfig(ResultStorageConfig):
    """Redis存储配置"""
    
    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis连接URL")
    key_prefix: str = Field(default="rabbitmq_arq", description="键前缀")
    connection_pool_size: int = Field(default=20, description="连接池大小")
    socket_timeout: float = Field(default=5.0, description="套接字超时(秒)")
    max_connections: int = Field(default=100, description="最大连接数")
    retry_on_timeout: bool = Field(default=True, description="超时时是否重试")
    encoding: str = Field(default="utf-8", description="编码格式")
    decode_responses: bool = Field(default=True, description="是否解码响应")


class ResultStorageStats(BaseModel):
    """存储统计信息"""
    
    total_stored: int = Field(default=0, description="总存储数量")
    total_retrieved: int = Field(default=0, description="总查询数量")
    total_deleted: int = Field(default=0, description="总删除数量")
    total_expired: int = Field(default=0, description="总过期数量")
    storage_errors: int = Field(default=0, description="存储错误数量")
    retrieval_errors: int = Field(default=0, description="查询错误数量")
    last_cleanup_at: datetime | None = Field(default=None, description="最后清理时间")
    last_error_at: datetime | None = Field(default=None, description="最后错误时间")
    
    model_config = ConfigDict(
        from_attributes=True
    )
    
    @field_serializer('last_cleanup_at', 'last_error_at')
    def serialize_datetime(self, value: datetime | None) -> str | None:
        """序列化 datetime 字段"""
        return value.astimezone(timezone.utc).isoformat() if value else None
    
    @property
    def success_rate(self) -> float:
        """计算成功率"""
        total_ops = self.total_stored + self.total_retrieved + self.total_deleted
        if total_ops == 0:
            return 1.0
        total_errors = self.storage_errors + self.retrieval_errors
        return max(0.0, 1.0 - (total_errors / total_ops))
    
    def __str__(self) -> str:
        """统计信息的字符串表示"""
        return (f"ResultStorageStats(stored={self.total_stored}, "
                f"retrieved={self.total_retrieved}, "
                f"success_rate={self.success_rate:.2%})")
