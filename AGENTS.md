# AGENTS.md

本文件面向 AI Agent（Kiro、Copilot、Cursor 等），描述本项目的开发规范和协作约定。

## 语言偏好

- 代码、变量名、commit message、分支名使用英文
- 注释、文档、日志消息、CLI 输出使用中文
- Prompt 模板中的指令使用中文
- YAML/JSON 配置中的描述字段使用中文

## 技术栈

- 后端: Python 3.11+, FastAPI, LangGraph, Pydantic 2.8+, PyODPS, httpx
- 前端: React 19, TypeScript, Vite, Tailwind CSS
- 数据: MaxCompute (ODPS), SQLite, JSONL
- 测试: pytest, hypothesis (property-based)
- 包管理: uv + pyproject.toml (后端), npm (前端/monorepo 脚本)

## 项目结构约定

```
backend/src/job_freshness/   # 后端源码根
backend/tests/unit/                    # 单元测试
backend/tests/integration/             # 集成测试
backend/sql/                           # SQL 模板
backend/scripts/                       # 运维脚本
frontend/src/                          # 前端源码
.kiro/specs/                           # 功能规格文档
```

## 命名规范

| 场景 | 风格 | 示例 |
|------|------|------|
| Python 类 | PascalCase | `StaticProfileService`, `GraphState` |
| Python 函数/变量 | snake_case | `build_cache_key`, `run_once` |
| Python 私有 | 前缀下划线 | `_execute_with_retry`, `_csv_row_to_wide_row` |
| Python 模块级常量 | UPPER_SNAKE_CASE | `_WIDE_TABLE`, `_COLUMNS` |
| React 组件 | PascalCase | `DateSelector`, `DateRangeView` |
| TypeScript 类型 | PascalCase | `RunDetail`, `StatsResponse` |
| API 路径 | /api/resource | `/api/runs`, `/api/stats` |
| 数据库表/列 | snake_case | `pipeline_runs`, `entity_key` |
| 测试文件 | test_{模块名}.py | `test_graph_routing.py` |

## Pydantic 模型规范

- 所有模型使用 `ConfigDict(extra="forbid")` 禁止多余字段
- 使用 `Field` 约束数值范围: `Field(ge=0)`, `Field(ge=1, le=32)`
- 使用 `list[X]` 而非 `List[X]`，`str | None` 而非 `Optional[str]`
- 默认值用 `Field(default_factory=list)` 而非 `= []`
- 模型更新使用 `state.model_copy(update={...})` 保持不可变

## 模块组织模式

### 节点模块 (nodes/)

每个 LangGraph 节点是一个子包，包含三个文件:

```
nodes/{node_name}/
├── service.py          # 主逻辑: 构建 prompt → 调用 LLM → 解析 → 更新 state
├── prompt_builder.py   # Prompt 构建: 加载模板 + 填充数据
├── parser.py           # 响应解析: normalize_llm_json → Pydantic 校验
```

### API 模块 (api/)

- `server.py` — FastAPI app 工厂，依赖注入，CORS 配置
- `routes.py` — 路由定义，无业务逻辑，只做参数校验和服务调用
- `services.py` — 业务逻辑层，操作 store 和 SQLite
- `schemas.py` — 请求/响应 Pydantic 模型
- `auth.py` — 认证服务（飞书 OAuth）

### Writers 模块 (writers/)

- 每个 writer 接收 `GraphState`，写入对应 store
- 同时写入 JSONL store 和 SQLite store（双写）
- publish_key 格式: `entity_key::version1::version2::...`

## 错误处理约定

- 校验错误: `ValueError`（参数、schema 不合法）
- 运行时错误: `RuntimeError`（ODPS 查询失败、LLM 超时）
- API 错误: `HTTPException`（400/404/422/503）
- 错误类型字符串: snake_case，如 `"parse_error"`, `"schema_validation_error"`
- 重试策略: ODPS 3 次 / 5s 间隔，LLM 2 次（可配置）

## 配置管理

- 所有配置通过 `.env` 文件 + 环境变量加载
- 使用 `python-dotenv` 的 `dotenv_values()` 读取
- 配置类使用 Pydantic `BaseModel` + `ConfigDict(extra="forbid")`
- 使用 `@lru_cache(maxsize=1)` 缓存配置加载结果
- 敏感信息（API Key、Secret）绝不硬编码或提交

## 测试规范

- 单元测试: `backend/tests/unit/test_{module}.py`
- 集成测试: `backend/tests/integration/test_{feature}.py`
- 测试数据: `backend/tests/integration/fixtures/`
- 使用 `tmp_path` fixture 处理临时文件
- Mock 外部依赖（ODPS、LLM），不在单元测试中发起真实网络请求
- 测试函数名: `test_{行为描述}`，如 `test_low_confidence_routes_to_fallback_branch`

## 常用命令

```bash
# 后端依赖管理 (使用 uv)
uv sync                      # 安装/同步依赖
uv add <package>             # 添加依赖
uv add --dev <package>       # 添加开发依赖
uv run pytest                # 通过 uv 运行命令

# monorepo 统一入口
npm run dev:backend          # 启动后端 API (uvicorn --reload)
npm run dev:frontend         # 启动前端开发服务
npm run test:backend         # 运行全部后端测试
npm run test:backend:unit    # 仅单元测试
npm run dry-run:backend      # 使用 mock 数据验证流水线
npm run lint:frontend        # 前端类型检查 (tsc --noEmit)
```

## 版本化设计

项目中多个维度有独立版本号，缓存键包含所有版本维度:

- `feature_schema_version` — 数据 schema 版本
- `taxonomy_version` — 行业分类体系版本
- `graph_version` — 流水线图版本
- `prompt_version_{node}` — 各节点 Prompt 版本
- `model_version_{node}` — 各节点 LLM 模型版本

版本变更会自动使缓存失效，无需手动清理。

## 输出目录结构

按业务日期分区:

```
output/{pt}/
├── formal_output.jsonl        # 高置信结果
├── fallback_output.jsonl      # 低置信/错误结果（待人工审核）
├── pipeline_results.sqlite3   # 完整审计记录
└── run_summary.json           # 运行摘要
```

## 代码风格偏好

- 使用 `from __future__ import annotations` 延迟类型求值
- 类型注解使用现代语法: `str | None`, `list[str]`, `dict[str, Any]`
- 日志使用 `logging.getLogger(__name__)`
- JSON 序列化: `ensure_ascii=False, sort_keys=True`
- 文件编码统一 `utf-8`
- 线程安全: 共享资源使用 `threading.Lock`
- dataclass 用于配置/结果，Pydantic 用于需要校验的模型
- Enum 继承 `(str, Enum)` 以便 JSON 序列化

## Git 提交规范

```
type(scope): description

# type: feat | fix | docs | refactor | test | chore
# scope: 模块名，如 nodes, api, graph, scheduler
# description: 祈使语气，50 字符内
```

## Agent 协作注意事项

- 修改 Pydantic 模型字段时，同步检查所有 `model_dump()` 和 `model_validate()` 调用
- 新增 API 端点时，在 `schemas.py` 定义请求/响应模型，在 `routes.py` 添加路由
- 新增 LangGraph 节点时，遵循 `service.py` / `prompt_builder.py` / `parser.py` 三文件结构
- SQL 模板使用 `${bizdate}` 占位符，通过 `sql_template.py` 渲染
- 不要自动添加测试，除非明确要求
- 前端修改后运行 `npm run lint:frontend` 检查类型
