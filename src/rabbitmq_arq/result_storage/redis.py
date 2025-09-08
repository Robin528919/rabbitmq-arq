# -*- coding: utf-8 -*-
"""
基于 Redis 的任务结果存储实现

生产环境推荐的高性能存储方案，支持TTL自动过期和连接池
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

# 可选依赖检查
try:
    import redis.asyncio as redis
    from redis.asyncio import Redis
    from redis.exceptions import RedisError, ConnectionError, TimeoutError
except ImportError:
    raise Exception(
        "Redis依赖未安装，请运行: pip install redis>=4.5.0",
        "redis"
    )
from .base import (
    ResultStore,
    ResultStorageError,
    ResultStoreConnectionError,
    ResultStoreTimeoutError,
    ResultSerializationError,
)
from .models import JobResult, RedisStorageConfig
from ..models import JobStatus


class RedisResultStore(ResultStore):
    """基于 Redis 的结果存储实现
    
    特性:
    - 高性能读写操作 (>1000 ops/sec)
    - 自动TTL过期管理
    - 连接池支持，高并发友好
    - 原子操作和事务支持
    - 内存使用优化
    
    键命名策略:
    - 结果数据: {prefix}:result:{job_id}
    - 状态索引: {prefix}:status:{job_id}  
    - 队列分组: {prefix}:queue:{queue_name}:results
    - 过期管理: {prefix}:expiry:{timestamp}
    """

    def __init__(self, config: dict[str, Any] | None = None):
        """初始化 Redis 存储
        
        Args:
            config: Redis 配置，包含以下选项:
                - redis_url: Redis连接URL (默认: redis://localhost:6379/0)
                - key_prefix: 键前缀 (默认: rabbitmq_arq)
                - connection_pool_size: 连接池大小 (默认: 20)
                - socket_timeout: 套接字超时秒数 (默认: 5.0)
                - max_connections: 最大连接数 (默认: 100)
                - retry_on_timeout: 超时重试 (默认: True)
                - ttl: 结果保存时间秒数 (默认: 86400)
                
        Raises:
            ResultStorageConfigError: Redis依赖未安装或配置错误
        """
        super().__init__(config)

        # 解析配置
        self._config = RedisStorageConfig.model_validate(config or {})

        # Redis 连接
        self._redis: Redis | None = None
        self._connection_pool = None

        # 键名模板
        self._result_key_template = f"{self._config.key_prefix}:result:{{}}"
        self._status_key_template = f"{self._config.key_prefix}:status:{{}}"
        self._queue_key_template = f"{self._config.key_prefix}:queue:{{}}:results"
        self._expiry_key_template = f"{self._config.key_prefix}:expiry:{{}}"

        self._logger.info(f"Redis存储初始化 - URL: {self._config.redis_url}, 前缀: {self._config.key_prefix}")

    async def _get_redis(self) -> Redis:
        """获取Redis连接，支持延迟连接"""
        if self._redis is None:
            await self._connect()
        return self._redis

    async def _connect(self) -> None:
        """建立 Redis 连接"""
        try:
            # 直接创建 Redis 客户端（内部会自动创建连接池）
            self._redis = redis.from_url(
                self._config.redis_url,
                max_connections=self._config.max_connections,
                socket_timeout=self._config.socket_timeout,
                encoding=self._config.encoding,
                decode_responses=self._config.decode_responses,
                retry_on_timeout=self._config.retry_on_timeout,
            )

            # 测试连接
            await self._redis.ping()

            self._logger.info("Redis连接建立成功")

        except ConnectionError as e:
            raise ResultStoreConnectionError(f"Redis连接失败: {e}", "redis")
        except Exception as e:
            raise ResultStorageError(f"Redis初始化失败: {e}", "redis")

    @staticmethod
    def _serialize_result(job_result: JobResult) -> str:
        """序列化任务结果为JSON"""
        try:
            return job_result.model_dump_json(exclude_none=False)
        except Exception as e:
            raise ResultSerializationError(f"结果序列化失败: {e}", "redis")

    @staticmethod
    def _deserialize_result(data: str) -> JobResult:
        """反序列化JSON为任务结果"""
        try:
            result_dict = json.loads(data)
            return JobResult.model_validate(result_dict)
        except json.JSONDecodeError as e:
            raise ResultSerializationError(f"JSON解析失败: {e}", "redis")
        except Exception as e:
            raise ResultSerializationError(f"结果反序列化失败: {e}", "redis")

    def _get_result_key(self, job_id: str) -> str:
        """获取结果数据键名"""
        return self._result_key_template.format(job_id)

    def _get_status_key(self, job_id: str) -> str:
        """获取状态索引键名"""
        return self._status_key_template.format(job_id)

    def _get_queue_key(self, queue_name: str) -> str:
        """获取队列分组键名"""
        return self._queue_key_template.format(queue_name)

    async def store_result(self, job_result: JobResult) -> None:
        """存储任务结果"""
        try:
            redis_client = await self._get_redis()

            # 设置过期时间
            if not job_result.expires_at:
                job_result.expires_at = datetime.now(timezone.utc) + timedelta(seconds=self._config.ttl)

            # 序列化结果
            result_data = self._serialize_result(job_result)

            # 计算TTL（Redis EXPIRE命令需要秒数）
            ttl_seconds = int((job_result.expires_at - datetime.now(timezone.utc)).total_seconds())
            if ttl_seconds <= 0:
                ttl_seconds = self._config.ttl  # 使用默认TTL

            # 使用 Redis 事务确保原子性
            pipe = redis_client.pipeline()

            result_key = self._get_result_key(job_result.job_id)
            status_key = self._get_status_key(job_result.job_id)
            queue_key = self._get_queue_key(job_result.queue_name)

            # 存储结果数据（带TTL）
            # noinspection PyAsyncCall
            pipe.setex(result_key, ttl_seconds, result_data)

            # 存储状态索引（带TTL）
            # 确保状态值是字符串（兼容枚举和字符串两种情况）
            status_value = job_result.status.value if hasattr(job_result.status, 'value') else str(job_result.status)
            # noinspection PyAsyncCall
            pipe.setex(status_key, ttl_seconds, status_value)

            # 添加到队列分组集合（带TTL）
            # noinspection PyAsyncCall
            pipe.sadd(queue_key, job_result.job_id)
            # noinspection PyAsyncCall
            pipe.expire(queue_key, ttl_seconds)

            # 执行事务
            await pipe.execute()

            # 更新统计
            self._update_stats_on_store()

            self._logger.debug(f"Redis存储成功: {job_result.job_id} (TTL: {ttl_seconds}s)")

        except TimeoutError as e:
            self._update_stats_on_error('store')
            raise ResultStoreTimeoutError(f"Redis存储超时: {e}", "redis")
        except RedisError as e:
            self._update_stats_on_error('store')
            raise ResultStorageError(f"Redis存储失败: {e}", "redis")
        except Exception as e:
            self._update_stats_on_error('store')
            raise ResultStorageError(f"存储结果失败: {e}", "redis")

    async def get_result(self, job_id: str) -> JobResult | None:
        """获取单个任务结果"""
        try:
            redis_client = await self._get_redis()

            result_key = self._get_result_key(job_id)
            result_data = await redis_client.get(result_key)

            if result_data is None:
                self._update_stats_on_retrieve()
                return None

            # 反序列化结果
            job_result = self._deserialize_result(result_data)

            # 更新统计
            self._update_stats_on_retrieve()

            self._logger.debug(f"Redis查询成功: {job_id}")
            return job_result

        except TimeoutError as e:
            self._update_stats_on_error('retrieve')
            raise ResultStoreTimeoutError(f"Redis查询超时: {e}", "redis")
        except RedisError as e:
            self._update_stats_on_error('retrieve')
            raise ResultStorageError(f"Redis查询失败: {e}", "redis")
        except Exception as e:
            self._update_stats_on_error('retrieve')
            raise ResultStorageError(f"查询结果失败: {e}", "redis")

    async def get_results(self, job_ids: list[str]) -> dict[str, JobResult | None]:
        """批量获取任务结果"""
        if not job_ids:
            return {}

        try:
            redis_client = await self._get_redis()

            # 构建所有键名
            result_keys = [self._get_result_key(job_id) for job_id in job_ids]

            # 批量获取 (使用 MGET 命令)
            result_data_list = await redis_client.mget(result_keys)

            # 构建结果字典
            results = {}
            for job_id, result_data in zip(job_ids, result_data_list):
                if result_data is None:
                    results[job_id] = None
                else:
                    try:
                        results[job_id] = self._deserialize_result(result_data)
                    except Exception as e:
                        self._logger.warning(f"反序列化结果 {job_id} 失败: {e}")
                        results[job_id] = None

            # 更新统计
            self._stats.total_retrieved += len(job_ids)

            found_count = sum(1 for r in results.values() if r is not None)
            self._logger.debug(f"Redis批量查询: {found_count}/{len(job_ids)} 个结果找到")

            return results

        except TimeoutError as e:
            self._update_stats_on_error('retrieve')
            raise ResultStoreTimeoutError(f"Redis批量查询超时: {e}", "redis")
        except RedisError as e:
            self._update_stats_on_error('retrieve')
            raise ResultStorageError(f"Redis批量查询失败: {e}", "redis")
        except Exception as e:
            self._update_stats_on_error('retrieve')
            raise ResultStorageError(f"批量查询失败: {e}", "redis")

    async def get_status(self, job_id: str) -> JobStatus | None:
        """获取任务状态（优化版本，只查询状态索引）"""
        try:
            redis_client = await self._get_redis()

            status_key = self._get_status_key(job_id)
            status_str = await redis_client.get(status_key)

            if status_str is None:
                self._update_stats_on_retrieve()
                return None

            # 更新统计
            self._update_stats_on_retrieve()

            return JobStatus(status_str)

        except TimeoutError as e:
            self._update_stats_on_error('retrieve')
            raise ResultStoreTimeoutError(f"Redis状态查询超时: {e}", "redis")
        except RedisError as e:
            self._update_stats_on_error('retrieve')
            raise ResultStorageError(f"Redis状态查询失败: {e}", "redis")
        except Exception as e:
            self._update_stats_on_error('retrieve')
            raise ResultStorageError(f"状态查询失败: {e}", "redis")

    async def delete_result(self, job_id: str) -> bool:
        """删除任务结果"""
        try:
            redis_client = await self._get_redis()

            # 先获取结果以获得队列信息
            result = await self.get_result(job_id)

            # 使用事务删除所有相关键
            pipe = redis_client.pipeline()

            result_key = self._get_result_key(job_id)
            status_key = self._get_status_key(job_id)
            # noinspection PyAsyncCall
            pipe.delete(result_key)
            # noinspection PyAsyncCall
            pipe.delete(status_key)


            # 如果能获得队列信息，从队列集合中移除
            if result:
                queue_key = self._get_queue_key(result.queue_name)
                # noinspection PyAsyncCall
                pipe.srem(queue_key, job_id)

            # 执行删除
            delete_results = await pipe.execute()

            # 检查是否有键被删除 (至少结果键应该存在)
            deleted = any(delete_results[:2])  # 前两个是删除操作的结果

            if deleted:
                self._update_stats_on_delete()
                self._logger.debug(f"Redis删除成功: {job_id}")

            return deleted

        except TimeoutError as e:
            self._update_stats_on_error('delete')
            raise ResultStoreTimeoutError(f"Redis删除超时: {e}", "redis")
        except RedisError as e:
            self._update_stats_on_error('delete')
            raise ResultStorageError(f"Redis删除失败: {e}", "redis")
        except Exception as e:
            self._update_stats_on_error('delete')
            raise ResultStorageError(f"删除结果失败: {e}", "redis")

    async def cleanup_expired(self) -> int:
        """清理过期结果
        
        Redis会自动删除过期键，这里主要是统计和日志记录
        """
        try:
            # Redis的TTL机制会自动清理过期键
            # 这里我们可以检查一些队列集合的一致性

            redis_client = await self._get_redis()
            cleanup_count = 0

            # 获取所有队列键
            queue_pattern = self._queue_key_template.format("*")
            queue_keys = await redis_client.keys(queue_pattern)

            for queue_key in queue_keys:
                try:
                    # 获取队列中的所有任务ID
                    job_ids = await redis_client.smembers(queue_key)

                    # 检查对应的结果键是否还存在
                    if job_ids:
                        result_keys = [self._get_result_key(job_id) for job_id in job_ids]
                        exists_results = await redis_client.exists(*result_keys)

                        # 如果结果键不存在，从队列集合中移除
                        for job_id, exists in zip(job_ids, exists_results):
                            if not exists:
                                await redis_client.srem(queue_key, job_id)
                                cleanup_count += 1

                except Exception as e:
                    self._logger.warning(f"清理队列 {queue_key} 时出错: {e}")
                    continue

            if cleanup_count > 0:
                self._update_stats_on_expire(cleanup_count)
                self._stats.last_cleanup_at = datetime.now(timezone.utc)
                self._logger.debug(f"Redis清理完成: {cleanup_count} 个过期引用")

            return cleanup_count

        except Exception as e:
            self._logger.error(f"Redis清理过期结果失败: {e}")
            # 清理失败不抛出异常，因为Redis会自动处理过期
            return 0

    async def get_stats(self) -> dict[str, Any]:
        """获取存储统计信息"""
        base_stats = await super().get_stats()

        try:
            redis_client = await self._get_redis()

            # Redis服务器信息
            info = await redis_client.info()

            # 获取键数量统计
            result_pattern = self._result_key_template.format("*")
            result_keys_count = len(await redis_client.keys(result_pattern))

            redis_stats = {
                'redis_version': info.get('redis_version', 'unknown'),
                'used_memory': info.get('used_memory', 0),
                'used_memory_human': info.get('used_memory_human', '0B'),
                'connected_clients': info.get('connected_clients', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'current_result_keys': result_keys_count,
                'key_prefix': self._config.key_prefix,
                'pool_size': self._config.connection_pool_size,
                'max_connections': self._config.max_connections,
            }

            # 计算命中率
            hits = redis_stats['keyspace_hits']
            misses = redis_stats['keyspace_misses']
            if hits + misses > 0:
                redis_stats['hit_rate'] = hits / (hits + misses)
            else:
                redis_stats['hit_rate'] = 0.0

        except Exception as e:
            self._logger.warning(f"获取Redis统计信息失败: {e}")
            redis_stats = {'error': str(e)}

        base_stats['redis_specific'] = redis_stats
        return base_stats

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            redis_client = await self._get_redis()

            # 测试基本连接
            pong = await redis_client.ping()
            if not pong:
                return False

            # 测试读写操作
            test_key = f"{self._config.key_prefix}:health_check"
            test_value = f"health_check_{datetime.now(timezone.utc).timestamp()}"

            await redis_client.setex(test_key, 60, test_value)  # 60秒过期
            retrieved_value = await redis_client.get(test_key)
            await redis_client.delete(test_key)  # 清理测试数据

            return retrieved_value == test_value

        except Exception as e:
            self._logger.warning(f"Redis健康检查失败: {e}")
            return False

    async def validate_connection(self) -> bool:
        """验证Redis连接
        
        Returns:
            连接状态，True表示连接正常
            
        Raises:
            ResultStoreConnectionError: 连接验证失败时抛出
        """
        try:
            # 尝试建立连接（如果还没有建立）
            redis_client = await self._get_redis()
            
            # 执行PING命令验证连接
            pong = await redis_client.ping()
            if not pong:
                raise ResultStoreConnectionError("Redis PING 命令返回失败", "redis")
            
            self._logger.debug("Redis连接验证成功")
            return True
            
        except ConnectionError as e:
            raise ResultStoreConnectionError(f"Redis连接失败: {e}", "redis")
        except TimeoutError as e:
            raise ResultStoreConnectionError(f"Redis连接超时: {e}", "redis")
        except RedisError as e:
            raise ResultStoreConnectionError(f"Redis错误: {e}", "redis")
        except Exception as e:
            raise ResultStoreConnectionError(f"Redis连接验证异常: {e}", "redis")

    async def close(self) -> None:
        """关闭存储连接"""
        if self._redis:
            try:
                await self._redis.close()
                self._logger.info("Redis连接已关闭")
            except Exception as e:
                self._logger.warning(f"关闭Redis连接时出错: {e}")
            finally:
                self._redis = None

        if self._connection_pool:
            try:
                await self._connection_pool.disconnect()
                self._logger.debug("Redis连接池已断开")
            except Exception as e:
                self._logger.warning(f"断开Redis连接池时出错: {e}")
            finally:
                self._connection_pool = None

        await super().close()

    async def __aenter__(self):
        """异步上下文管理器支持"""
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器支持"""
        await self.close()
