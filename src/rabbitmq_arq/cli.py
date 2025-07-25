# -*- coding: utf-8 -*-
# @version        : 1.0
# @Create Time    : 2025/5/9 23:30
# @File           : cli
# @IDE            : PyCharm
# @desc           : RabbitMQ-ARQ 命令行工具

import asyncio
import sys
from pathlib import Path

import click

from .connections import RabbitMQSettings
from .worker import Worker, WorkerSettings


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """
    🚀 RabbitMQ-ARQ 命令行工具
    
    基于 RabbitMQ 的异步任务队列库，提供类似 arq 的简洁 API。
    """
    pass


@cli.command()
# === RabbitMQ 连接配置 ===
@click.option(
    '--rabbitmq-url', '-u',
    default="amqp://guest:guest@localhost:5672/",
    help='RabbitMQ 连接 URL'
)
@click.option(
    '--prefetch-count',
    default=100,
    type=int,
    help='预取消息数量'
)
@click.option(
    '--connection-timeout',
    default=30,
    type=int,
    help='连接超时时间（秒）'
)
# === Worker 配置 ===
@click.option(
    '--worker-module', '-m',
    required=True,
    help='Worker 模块路径，例如: myapp.workers:worker_settings'
)
@click.option(
    '--queue', '-q',
    default="default",
    help='队列名称'
)
@click.option(
    '--max-retries', '-r',
    default=3,
    type=int,
    help='最大重试次数'
)
@click.option(
    '--job-timeout', '-t',
    default=300,
    type=int,
    help='任务超时时间（秒）'
)
@click.option(
    '--max-concurrent-jobs',
    default=10,
    type=int,
    help='最大并发任务数'
)
# === Burst 模式配置 ===
@click.option(
    '--burst', '-b',
    is_flag=True,
    help='启用 Burst 模式（处理完队列后自动退出）'
)
@click.option(
    '--burst-timeout',
    default=300,
    type=int,
    help='Burst 模式最大运行时间（秒）'
)
@click.option(
    '--burst-check-interval',
    default=1.0,
    type=float,
    help='Burst 模式队列检查间隔（秒）'
)
@click.option(
    '--burst-no-wait',
    is_flag=True,
    help='Burst 模式下不等待正在执行的任务完成'
)
# === 日志配置 ===
@click.option(
    '--log-level', '-l',
    default="INFO",
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    help='日志级别'
)
def worker(
        # 连接配置
        rabbitmq_url: str,
        prefetch_count: int,
        connection_timeout: int,

        # Worker配置
        worker_module: str,
        queue: str,
        max_retries: int,
        job_timeout: int,
        max_concurrent_jobs: int,

        # Burst模式配置
        burst: bool,
        burst_timeout: int,
        burst_check_interval: float,
        burst_no_wait: bool,

        # 日志配置
        log_level: str
):
    """
    启动 Worker 处理任务
    
    示例:
    
    \b
    # 启动常规模式 Worker
    rabbitmq-arq worker -m myapp.workers:worker_settings
    
    \b
    # 启动 Burst 模式 Worker
    rabbitmq-arq worker -m myapp.workers:worker_settings --burst
    
    \b
    # 使用自定义配置
    rabbitmq-arq worker -m myapp.workers:worker_settings \\
        --rabbitmq-url amqp://user:pass@localhost:5672/ \\
        --queue my_queue \\
        --burst \\
        --burst-timeout 600
    """

    # 配置日志
    import logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger('rabbitmq-arq.cli')

    try:
        # 导入 WorkerSettings 或函数列表
        base_config = import_worker_config(worker_module)

        # 创建 RabbitMQ 连接配置
        rabbitmq_settings = RabbitMQSettings(
            rabbitmq_url=rabbitmq_url,
            prefetch_count=prefetch_count,
            connection_timeout=connection_timeout,
        )

        # 构建 WorkerSettings
        if isinstance(base_config, WorkerSettings):
            # 如果导入的是 WorkerSettings，则覆盖部分配置
            worker_settings = WorkerSettings(
                rabbitmq_settings=rabbitmq_settings,
                functions=base_config.functions,
                worker_name=base_config.worker_name,

                # 使用CLI参数覆盖队列和任务配置
                queue_name=queue,
                dlq_name=f"{queue}_dlq",
                max_retries=max_retries,
                job_timeout=job_timeout,
                max_concurrent_jobs=max_concurrent_jobs,

                # Burst模式配置
                burst_mode=burst,
                burst_timeout=burst_timeout,
                burst_check_interval=burst_check_interval,
                burst_wait_for_tasks=not burst_no_wait,

                # 日志配置
                log_level=log_level,

                # 保留原有的钩子函数
                on_startup=base_config.on_startup,
                on_shutdown=base_config.on_shutdown,
                on_job_start=base_config.on_job_start,
                on_job_end=base_config.on_job_end,
            )
        elif isinstance(base_config, list):
            # 如果导入的是函数列表，则创建新的 WorkerSettings
            worker_settings = WorkerSettings(
                rabbitmq_settings=rabbitmq_settings,
                functions=base_config,

                # Worker配置
                queue_name=queue,
                dlq_name=f"{queue}_dlq",
                max_retries=max_retries,
                job_timeout=job_timeout,
                max_concurrent_jobs=max_concurrent_jobs,

                # Burst模式配置
                burst_mode=burst,
                burst_timeout=burst_timeout,
                burst_check_interval=burst_check_interval,
                burst_wait_for_tasks=not burst_no_wait,

                # 日志配置
                log_level=log_level,
            )
        else:
            raise ValueError(f"不支持的配置类型: {type(base_config)}")

        # 显示启动信息
        if burst:
            logger.info(f"🚀 启动 Burst 模式 Worker")
            logger.info(f"   Worker: {worker_settings.worker_name}")
            logger.info(f"   队列: {queue}")
            logger.info(f"   超时: {burst_timeout}s")
            logger.info(f"   检查间隔: {burst_check_interval}s")
            logger.info(f"   等待任务完成: {'是' if not burst_no_wait else '否'}")
        else:
            logger.info(f"🚀 启动常规模式 Worker")
            logger.info(f"   Worker: {worker_settings.worker_name}")
            logger.info(f"   队列: {queue}")
            logger.info(f"   预取数量: {prefetch_count}")
            logger.info(f"   最大并发: {max_concurrent_jobs}")

        # 创建并运行 Worker
        worker_instance = Worker(worker_settings)
        asyncio.run(worker_instance.main())

    except Exception as e:
        logger.error(f"❌ Worker 启动失败: {e}")
        sys.exit(1)


@cli.command()
@click.option(
    '--rabbitmq-url', '-u',
    default="amqp://guest:guest@localhost:5672/",
    help='RabbitMQ 连接 URL'
)
@click.option(
    '--queue', '-q',
    default="default",
    help='队列名称'
)
def queue_info(rabbitmq_url: str, queue: str):
    """
    查看队列信息
    """

    async def get_queue_info():
        from aio_pika import connect_robust

        connection = await connect_robust(rabbitmq_url)
        channel = await connection.channel()

        try:
            queue_obj = await channel.declare_queue(queue, durable=True, passive=True)
            message_count = queue_obj.declaration_result.message_count
            consumer_count = queue_obj.declaration_result.consumer_count

            click.echo(f"📊 队列信息: {queue}")
            click.echo(f"   消息数量: {message_count}")
            click.echo(f"   消费者数量: {consumer_count}")
            click.echo(f"   队列状态: {'空闲' if message_count == 0 else '有消息'}")

        except Exception as e:
            click.echo(f"❌ 获取队列信息失败: {e}")
        finally:
            await connection.close()

    asyncio.run(get_queue_info())


@cli.command()
@click.option(
    '--rabbitmq-url', '-u',
    default="amqp://guest:guest@localhost:5672/",
    help='RabbitMQ 连接 URL'
)
@click.option(
    '--queue', '-q',
    default="default",
    help='队列名称'
)
@click.confirmation_option(prompt='确认要清空队列吗？这将删除所有未处理的消息')
def purge_queue(rabbitmq_url: str, queue: str):
    """
    清空队列中的所有消息
    """

    async def purge():
        from aio_pika import connect_robust

        connection = await connect_robust(rabbitmq_url)
        channel = await connection.channel()

        try:
            queue_obj = await channel.declare_queue(queue, durable=True)
            purged_count = await queue_obj.purge()

            click.echo(f"✅ 已从队列 {queue} 中清空 {purged_count} 条消息")

        except Exception as e:
            click.echo(f"❌ 清空队列失败: {e}")
        finally:
            await connection.close()

    asyncio.run(purge())


@cli.command()
@click.option(
    '--worker-module', '-m',
    required=True,
    help='Worker 模块路径'
)
def validate_config(worker_module: str):
    """
    验证 Worker 配置
    """
    try:
        config = import_worker_config(worker_module)

        if isinstance(config, WorkerSettings):
            click.echo(f"✅ WorkerSettings 配置验证通过")
            click.echo(f"   Worker: {config.worker_name}")
            click.echo(f"   函数数量: {len(config.functions)}")
            click.echo(f"   队列: {config.queue_name}")
        elif isinstance(config, list):
            click.echo(f"✅ 函数列表配置验证通过")
            click.echo(f"   函数数量: {len(config)}")
            for func in config:
                click.echo(f"   - {getattr(func, '__name__', str(func))}")
        else:
            click.echo(f"❌ 不支持的配置类型: {type(config)}")

    except Exception as e:
        click.echo(f"❌ 配置验证失败: {e}")


def import_worker_config(module_path: str):
    """
    导入 Worker 配置（WorkerSettings 对象或函数列表）
    
    Args:
        module_path: 模块路径，格式为 'module.path:attribute'
        
    Returns:
        WorkerSettings 对象或函数列表
    """
    if ':' not in module_path:
        raise ValueError("模块路径格式错误，应为 'module.path:attribute'")

    module_name, attr_name = module_path.rsplit(':', 1)

    # 添加当前目录到 Python 路径
    current_dir = Path.cwd()
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))

    try:
        import importlib
        module = importlib.import_module(module_name)
        config = getattr(module, attr_name)

        # 验证配置类型
        if isinstance(config, WorkerSettings):
            return config
        elif isinstance(config, list):
            # 验证是否为函数列表
            for item in config:
                if not callable(item):
                    raise ValueError(f"列表中的项目必须是可调用对象: {item}")
            return config
        else:
            raise ValueError(f"配置必须是 WorkerSettings 对象或函数列表，实际类型: {type(config)}")

    except ImportError as e:
        raise ImportError(f"无法导入模块 {module_name}: {e}")
    except AttributeError as e:
        raise AttributeError(f"模块 {module_name} 中没有属性 {attr_name}: {e}")


def main():
    """CLI 入口点"""
    try:
        cli()
    except KeyboardInterrupt:
        click.echo("\n👋 用户中断，退出程序")
        sys.exit(0)
    except Exception as e:
        click.echo(f"❌ 程序异常: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
