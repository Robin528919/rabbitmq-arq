# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 19:35
# @File           : models
# @IDE            : PyCharm
# @desc           : 任务模型定义

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class JobStatus(str, Enum):
    """任务状态枚举"""
    QUEUED = "queued"  # 已入队
    IN_PROGRESS = "in_progress"  # 进行中
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"  # 失败
    RETRYING = "retrying"  # 重试中
    ABORTED = "aborted"  # 已中止


class JobModel(BaseModel):
    """任务模型 - 使用 Pydantic V2 和 Python 3.12 现代语法"""
    
    # 使用 Python 3.10+ 的联合类型语法
    job_id: str = Field(default_factory=lambda: uuid.uuid4().hex, description="任务ID")
    function: str = Field(..., description="要执行的函数名")
    args: list[Any] = Field(default_factory=list, description="位置参数")
    kwargs: dict[str, Any] = Field(default_factory=dict, description="关键字参数")
    job_try: int = Field(default=1, description="任务尝试次数")
    enqueue_time: datetime = Field(default_factory=datetime.now, description="入队时间")
    start_time: datetime | None = Field(default=None, description="开始执行时间")
    end_time: datetime | None = Field(default=None, description="结束时间")
    status: JobStatus = Field(default=JobStatus.QUEUED, description="任务状态")
    result: Any | None = Field(default=None, description="任务结果")
    error: str | None = Field(default=None, description="错误信息")
    queue_name: str = Field(..., description="队列名称")
    defer_until: datetime | None = Field(default=None, description="延迟执行时间")
    expires: datetime | None = Field(default=None, description="过期时间")
    
    # Pydantic V2 配置 - 使用 ConfigDict 替代 Config 类
    model_config = ConfigDict(
        # 启用从属性验证（替代 V1 的 from_orm）
        from_attributes=True,
        # 允许任意类型（如果需要）
        arbitrary_types_allowed=False,
        # 生成 JSON Schema 时使用字段别名
        populate_by_name=True,
        # 额外字段处理
        extra='ignore',
        # 使用枚举值而非名称
        use_enum_values=True,
    )
    
    # Pydantic V2 推荐使用 field_serializer 替代 json_encoders
    @field_serializer('enqueue_time', 'start_time', 'end_time', 'defer_until', 'expires')
    def serialize_datetime(self, value: datetime | None) -> str | None:
        """序列化 datetime 字段为 ISO 格式字符串"""
        return value.isoformat() if value else None


class JobContext(BaseModel):
    """任务上下文，传递给任务函数的第一个参数"""
    job_id: str = Field(..., description="任务ID")
    job_try: int = Field(..., description="任务尝试次数")
    enqueue_time: datetime = Field(..., description="入队时间")
    start_time: datetime = Field(..., description="开始执行时间")
    queue_name: str = Field(..., description="队列名称")
    worker_id: str = Field(..., description="Worker ID")
    # 用户自定义的上下文数据
    extra: dict[str, Any] = Field(default_factory=dict, description="额外的上下文数据")

    model_config = ConfigDict(
        from_attributes=True,
        extra='allow',  # 允许额外字段用于扩展上下文
    )


class WorkerInfo(BaseModel):
    """Worker 信息"""
    worker_id: str = Field(..., description="Worker ID")
    start_time: datetime = Field(..., description="启动时间")
    jobs_complete: int = Field(default=0, description="完成的任务数")
    jobs_failed: int = Field(default=0, description="失败的任务数")
    jobs_retried: int = Field(default=0, description="重试的任务数")
    jobs_ongoing: int = Field(default=0, description="正在执行的任务数")
    last_health_check: datetime = Field(default_factory=datetime.now, description="最后健康检查时间")

    model_config = ConfigDict(
        from_attributes=True,
        extra='ignore',
    )

    @field_serializer('start_time', 'last_health_check')
    def serialize_datetime(self, value: datetime) -> str:
        """序列化 datetime 字段为 ISO 格式字符串"""
        return value.isoformat() 