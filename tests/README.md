# RabbitMQ-ARQ 测试文档

## 错误处理测试

`test_error_handling.py` 提供了全面的错误处理测试，用于验证框架的错误分类和重试逻辑。

### 🧪 测试场景

#### 1. 发送测试任务
```bash
# 发送所有类型的错误测试任务
python tests/test_error_handling.py send
```

#### 2. 启动测试 Worker
```bash
# 启动专用测试 Worker 处理任务
python tests/test_error_handling.py worker

# 或者使用主 Worker（需要先运行 send 命令）
python examples/example.py worker
```

#### 3. Burst 模式测试
```bash
# 自动发送任务 + 处理 + 退出
python tests/test_error_handling.py burst
```

### 📊 预期结果

运行测试后，应该看到以下日志模式：

#### 🔴 不可重试错误（立即失败）
```
任务 xxx 遇到不可重试错误 (non_retriable): TypeError: ...
任务 xxx 发送到死信队列: TypeError: ...
```

#### 🟡 可重试错误（重试后失败）  
```
任务 xxx 第 1 次重试，延迟 2.0 秒 (错误类型: ConnectionError, 分类: retriable)
任务 xxx 第 2 次重试，延迟 4.0 秒 (错误类型: ConnectionError, 分类: retriable)
任务 xxx 第 3 次重试，延迟 8.0 秒 (错误类型: ConnectionError, 分类: retriable)
任务 xxx 已达到最大重试次数 3
任务 xxx 发送到死信队列: ConnectionError: ...
```

#### 🟠 业务异常（重试后失败）
```
任务 xxx 第 1 次重试，延迟 2.0 秒 (错误类型: Exception, 分类: business_retriable)
任务 xxx 第 2 次重试，延迟 4.0 秒 (错误类型: Exception, 分类: business_retriable)
任务 xxx 第 3 次重试，延迟 8.0 秒 (错误类型: Exception, 分类: business_retriable)
任务 xxx 业务异常已达到最大重试次数 3: Exception: ...
任务 xxx 发送到死信队列: Exception: ...
```

#### 🔄 显式重试（成功）
```
任务 xxx 请求重试: 任务需要重试，延迟 4 秒
任务 xxx 已通过延迟交换机发送，将在 4.0 秒后处理 (重试次数: 1)
...
重试任务成功完成（第 3 次尝试）
任务 xxx 执行成功，耗时 0.10 秒
```

#### ✅ 成功任务
```
任务 xxx 执行成功，耗时 0.10 秒
```

### 🔍 验证要点

运行测试时，请重点关注：

1. **✅ 无无限循环**：任务在达到最大重试次数后必须停止
2. **✅ 无重复消费**：每个任务 ID 只应该被处理一次  
3. **✅ 正确分类**：每种错误应该按预期分类处理
4. **✅ 准确计数**：重试次数应该正确递增 (1, 2, 3...)
5. **✅ 及时停止**：不可重试错误应该立即停止

### 🛠️ 故障排除

如果测试结果不符合预期：

1. **检查 RabbitMQ 连接**：确保 RabbitMQ 服务正在运行
2. **检查队列状态**：使用 RabbitMQ 管理界面查看队列和死信队列
3. **查看完整日志**：注意错误分类和重试计数是否正确
4. **清空队列**：测试前可以清空队列避免历史任务干扰

### 📝 自定义测试

可以修改 `test_error_handling.py` 中的参数：

```python
# 修改重试参数
worker_settings = WorkerSettings(
    max_retries=5,      # 增加重试次数
    retry_backoff=1,    # 缩短退避时间 
    job_timeout=60,     # 调整任务超时
)

# 添加自定义错误任务
async def my_custom_error_task(ctx, param):
    raise MyCustomError("自定义错误")
```

### 🎯 高级测试

对于更复杂的测试场景：

1. **并发测试**：启动多个 Worker 实例
2. **负载测试**：发送大量任务
3. **延迟测试**：测试延迟任务执行
4. **故障恢复**：测试 RabbitMQ 重启后的恢复

## 其他测试

更多测试场景正在开发中... 