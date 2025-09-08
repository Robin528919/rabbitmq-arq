# Repository Guidelines

## 项目结构与模块
- `src/rabbitmq_arq/`：库源码（核心：`worker.py`、`client.py`、`connections.py`；结果存储在 `result_storage/`）。
- `tests/`：pytest 测试（含异步与集成测试，依赖 Redis/RabbitMQ）。
- `examples/`、`docs/`：示例与文档；`scripts/`：工具脚本（如 `build.sh`）。
- 根目录配置：`pyproject.toml`（构建、格式、mypy、pytest），`README.md`，`requirements-*.txt`。

## 构建、测试与开发命令
- 安装开发依赖：`pip install -e .[dev]`
- 仅单元测试：`pytest -q -m "not integration"`
- 全量含覆盖率：`pytest --cov --cov-report=term-missing`
- 格式与静态检查：`black src tests && isort src tests && flake8 src tests`
- 类型检查：`mypy src`
- 构建发布物：`python -m build` 或 `./scripts/build.sh`

## 代码风格与命名
- Python 3.8+，四空格缩进；文件/函数名使用小写加下划线。
- 格式化：Black（行宽 88）+ isort（profile=black）。
- Lint：flake8；提交前消除告警。
- 类型：mypy 严格规则（见 `tool.mypy`）；`src/` 内避免无类型定义。
- 测试命名：文件 `test_*.py`，函数 `test_*`，类 `Test*`。

## 测试规范
- 使用 `pytest`、`pytest-asyncio`（`asyncio_mode=auto`）。
- 需要外部服务的用例标记 `@pytest.mark.integration`；本地可用 `-m "not integration"` 跳过。
- Redis 测试建议使用 DB `15` 与唯一 `key_prefix`（参考现有用例）。
- 维护或提升覆盖率；为新特性与失败路径补充测试。

## 提交与 Pull Request
- 提交遵循 Conventional Commits：`feat(scope): 简述`、`fix(scope): ...`、`refactor`、`docs`、`build`、`ci`（参见 `git log`）。主题≤72字符，必要时补充正文说明。
- PR 需：清晰描述、关联 Issue、复现步骤/示例、相应测试与文档更新；变更 CLI 行为时附日志/截图；确保格式、mypy、测试全部通过。

## 安全与配置提示
- 勿提交密钥；使用环境变量（如 `RABBITMQ_URL`、`ARQ_RESULT_STORE_URL`）。
- 集成测试指向本地服务：`amqp://localhost:5672`、`redis://localhost:6379/15`。

## 贡献者/代理注意
- 变更应最小化且聚焦；严格遵循 `pyproject.toml` 中工具链。
- 代码放于 `src/rabbitmq_arq/`，测试置于 `tests/` 并镜像模块路径。

## 强制规则与约定
- 分支命名：`feat/<scope>-<desc>`、`fix/<scope>-<desc>`、`chore/...`，避免直接在 `main` 上提交。
- 日志与异步：`src/` 禁止 `print`，统一使用 `logging`；异步代码不得阻塞（避免 `time.sleep`，使用 `asyncio.sleep`）。
- 单元测试禁止网络/外部依赖：如需 Redis/RabbitMQ，标记为 `integration`；默认 CI 可跳过。
- 结果存储：测试使用 Redis DB `15` 与唯一 `key_prefix`；生产必须配置 `ttl` 与鉴权，避免使用 DB `15`。
- 兼容性：保持 Python 3.8+；公共 API 变更需在 `README.md` 与示例同步更新，并在提交信息中标注 `BREAKING CHANGE`（如有）。
- CLI 变更：更新 `README.md` 中命令示例，并在 PR 描述附运行截图/日志。
- 类型与异常：公共函数需显式类型标注；抛出库内自定义异常（见 `src/rabbitmq_arq/exceptions.py`），避免裸 `Exception`。
- 语言要求：日志内容与代码注释均须使用中文；用户可见的 CLI 输出与错误信息优先中文（保留必要的专有名词与协议关键字英文）。
