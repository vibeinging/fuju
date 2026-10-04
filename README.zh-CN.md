# yiTrace

## 评测改进，还是排查执行过程？

- [Fuju RSI](https://github.com/vibeinging/fuju-rsi)：通过编码 Agent Skill 评测和改进已有程序，先从智能问数开始。可以离线试玩，或连接真实程序生成原版报告。
- [Fuju Trace](https://github.com/vibeinging/fuju-trace)：记录、检索和排查 Agent 的执行过程。

本仓库保留原 yiTrace 引擎与兼容包。RSI 是独立的开发工具，业务程序可继续按原有方式运行。


**给 Agent 工程用的本地 TraceDB。**

Agent 工程，也就是 agentic engineering，需要的不只是 prompt、日志和聊天记忆。你需要看到
Agent 真实走过的路径：prompt、工具调用、span、错误、token、模型输出，以及最后真正有效的那一步。
yiTrace 把这些路径存进本地 TraceDB，让你可以搜索、回放、沉淀 eval 数据，也能把有用的执行历史
交给 Agent Memory。

中文 · [English](README.md)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange?logo=rust)](https://www.rust-lang.org/)
[![npm](https://img.shields.io/npm/v/%40yitrace%2Fdb?logo=npm)](https://www.npmjs.com/package/@yitrace/db)
[![PyPI](https://img.shields.io/pypi/v/yitrace-db?logo=pypi)](https://pypi.org/project/yitrace-db/)

![yiTrace 控制台](docs/images/console-overview.png)

## 为什么做

做 Agent 不是只写 prompt。它更像在做一个会调用工具、会失败、需要比较多次运行结果、还要持续改进的系统。

这里缺的一层是执行记忆：不只是用户说过什么，而是 Agent 实际做过什么。yiTrace 在本地给你补上这一层：

- 回放多轮对话、工具调用、复杂 agent run
- 搜索 prompt、日志、工具调用、错误和模型输出
- 按 tenant、agent、状态、时间、自定义 attrs 精确过滤
- 按 trace、session、agent 汇总 token 和成本
- 把失败或有价值的 span 收进 eval dataset
- 把成功路径作为 Agent Memory 的证据

## 快速开始

按你的项目技术栈选安装方式。

### Python

Python / FastAPI 项目如果想在本地写入和搜索 trace，不需要单独起 yiTrace 服务：

```bash
python -m pip install yitrace yitrace-db
```

```python
from yitrace import DbExporter, Tracer, connect

with connect(path="./yitrace-data", tenant_id=1) as db:
    tracer = Tracer(exporter=DbExporter(db, tenant_id=1), node_id=1)

    with tracer.trace("风控复核", tenant_id=1) as trace:
        with trace.span("LLM 判断") as span:
            span.log("疑似盗刷")
            span.set_tokens(input_tokens=900, output_tokens=120)

    tracer.close()

    hits = db.search({"text": "盗刷", "k": 10})
    print(hits)
```

上面的 `with connect(...)` 适合脚本和最小示例。长期运行的 FastAPI、ARQ、Celery 服务不要按请求或任务反复打开 DB。正确做法是每个进程启动时调用一次 `init_yitrace(...)`，进程内复用 `runtime.tracer` 和查询 handle，退出时调用一次 `shutdown_yitrace()`。同机多个 worker 可以共享一个本地 data dir；多台机器应改用 HTTP。

完整的生命周期、FastAPI/ARQ 示例、buffered/spool/http 选择和查询写法见 [Python 服务端接入指南](docs/design/2026-07-14_python-service-integration.md)。

如果只想把 trace 发到一个运行中的 yiTrace server，只装轻量 SDK：

```bash
python -m pip install yitrace
```

```python
from yitrace import HttpExporter, Tracer

tracer = Tracer(
    exporter=HttpExporter("http://127.0.0.1:7878/v1/ingest", tenant_id=1),
    node_id=1,
)

with tracer.trace("风控复核", tenant_id=1) as trace:
    with trace.span("工具调用") as span:
        span.log("查询风控数据库")

tracer.close()
```

### Node / Electron

Node 后端或 Electron 主进程如果要本地写入和搜索 trace，用 `@yitrace/db`。它通过
Node-API 嵌入 Rust engine，不是在 JS 里直接解析数据库文件。

```bash
npm install @yitrace/db
```

```ts
import { YiTraceDB, createSpanEventBuilder } from "@yitrace/db";

const db = await YiTraceDB.open({ dataDir: "./yitrace-data", tenantId: 1 });

const events = createSpanEventBuilder({
  traceId: "run-uuid",
  sessionId: "session-uuid",
  attrs: {
    project_id: "agentic-data",
    skill: "review",
    mode: "auto",
    call_site: "worker.ts:10",
  },
});

events.startSpan({ spanId: "span-uuid", name: "风控复核", agentName: "risk-agent" });
events.log({ spanId: "span-uuid", message: "possible card fraud" });
events.endSpan({ spanId: "span-uuid", status: 0, durationNs: 12_000_000 });

await events.ingest(db);

const hits = await db.search({
  text: "fraud",
  k: 10,
  filter: { attrs: { project_id: "agentic-data", skill: "review" } },
});

console.log(hits);
await db.close();
```

如果 TypeScript 应用只打点上报到 server，用更轻的 SDK：

```bash
npm install @yitrace/trace-sdk
```

```ts
import { HttpExporter, Tracer } from "@yitrace/trace-sdk";

const tracer = new Tracer(
  new HttpExporter({ url: "http://127.0.0.1:7878/v1/ingest", tenantId: 1 }),
  1,
);

tracer.trace("风控复核", (trace) => {
  trace.span("工具调用", (span) => {
    span.log("查询风控数据库");
  });
}, undefined, 1);

await tracer.close();
```

### 起一个 yiTrace server

多台机器、跨主机容器或不能共享本地磁盘时，用 server 模式。同一台机器上的多个进程也可以直接使用 embedded 模式，不需要仅因为 worker 多就改成 server。

```bash
python -m pip install "yitrace-db[server]"
yitrace-db serve --data-dir ./yitrace-data --bind 127.0.0.1:7878
```

然后通过 HTTP 搜索：

```bash
curl -XPOST http://127.0.0.1:7878/v1/search \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-Id: 1' \
  -d '{"text":"盗刷","k":10}'
```

完整 HTTP 契约见 [API Reference](docs/API_REFERENCE.md)。

## 应该安装哪个包

| 应用形态 | 安装 | 做什么 |
|---|---|---|
| Python 应用本地写入和搜索 trace | `pip install yitrace yitrace-db` | Python 进程内嵌入 DB |
| Python 应用只发 trace 到 server | `pip install yitrace` | 轻量打点 SDK |
| FastAPI 想挂 yiTrace route / server | `pip install "yitrace-db[server]"` | embedded DB 加可选 FastAPI/Uvicorn server |
| Node 后端或 Electron 本地写入和搜索 trace | `npm install @yitrace/db` | 通过 Node-API 嵌入 DB |
| TypeScript 应用只发 trace 到 server | `npm install @yitrace/trace-sdk` | 轻量打点 SDK |
| 已经有 OTel/OpenInference | 发 `POST /v1/traces` | 兼容 OTLP/OpenInference 摄入 |
| Rust 应用 | 源码依赖 | Rust SDK 和 embedded wrapper 都在本仓库 |

已经发布的包：

- npm：[`@yitrace/db`](https://www.npmjs.com/package/@yitrace/db)、[`@yitrace/trace-sdk`](https://www.npmjs.com/package/@yitrace/trace-sdk)
- PyPI：[`yitrace`](https://pypi.org/project/yitrace/)、[`yitrace-db`](https://pypi.org/project/yitrace-db/)

## embedded 模式和 server 模式

**embedded 模式**是在你的应用进程里打开一个本地目录：

```python
db = connect(path="./yitrace-data", tenant_id=1)
```

```ts
const db = await YiTraceDB.open({ dataDir: "./yitrace-data", tenantId: 1 });
```

适合一个机器拥有这个 data dir 的场景。同一台机器上的多个 worker 进程可以同时打开；
yiTrace 会用 data-dir 锁串行化 open/write，并用 reader pin 保护快照清理。

服务端应让每个 worker 进程启动时打开一次、退出时关闭一次。不要在每个请求、任务或查询中反复 open/close。buffered 模式下刚写完就要查询时，先等待 exporter flush，不要重新打开 DB。

**server 模式**是跑一个 yiTrace 进程，其他客户端通过 HTTP 写入：

```bash
yitrace-db serve --data-dir ./yitrace-data --bind 0.0.0.0:7878
```

适合多机器、多容器、网络文件系统不可靠的场景。不要跨机器共享同一个 embedded data dir。

## 控制台

yiTrace 自带 trace 回放控制台。使用包安装时，启动 Python server 后打开：

```text
http://127.0.0.1:7878/
```

从源码运行时，先构建 React 控制台并拷到 engine crate：

```bash
cd yitrace-console
npm install
VITE_API=http npm run build
rm -rf ../yitrace-engine/crates/yt-engine/console_dist
cp -r dist ../yitrace-engine/crates/yt-engine/console_dist
```

控制台调用的也是普通 `/v1/*` JSON API，和你自己写 UI 用的是同一套接口。

## 从源码构建

改 engine、console 或包封装时走源码路径。

```bash
git clone https://github.com/vibeinging/fuju.git
cd fuju

cd yitrace-engine
cargo test --offline
cargo run -p yt-engine --example server
```

Rust engine 主体刻意保持 std-only。较重的集成放在独立 crate 或包目录里。

包级检查：

```bash
# Python SDK
cd yitrace-sdk/python
python -m pytest

# TypeScript SDK
cd yitrace-sdk/typescript
npm install
npm test
npm run build

# Node embedded DB
cd yitrace-node
npm install
npm run build
npm test

# Python embedded DB
cd yitrace-db-python
python -m pytest
```

构建发版产物：

```bash
python scripts/check_release_versions.py
./scripts/package_release_artifacts.sh
./tests/upgrade_012_to_current.sh
./tests/sidecar_rebuild_kill9.sh
```

GitHub Actions 只在推送 `v*` tag 时打包。可以用 `vX.Y.Z-only-python-sdk`
这类 tag 先跑单个包；用 `vX.Y.Z` 跑完整打包矩阵。每个 native job 都会先在
干净 consumer 中安装最终 wheel 或 tarball，验证通过后才上传产物。

## 它怎么工作

```text
SDKs / OTLP / embedded package
    |
    v
EngineJsonApi or HTTP ingest
    |
    v
WAL + memtable --flush--> immutable segments
    |                         |
    v                         v
BM25 / vector / attrs indexes read-time fold
    |                         |
    +---------- search / replay / cost / eval
```

核心思路很简单：yiTrace 存的是事件，不是可变 span。一个 span 会被写成 start、log、end
和晚到属性事件。读取时再把这些事件折叠成完整 span。事件 ID 是确定性的：

```text
event_id = hash(ext_span_id, seq, event_type)
```

所以重试和崩溃重放不会重复计算同一个事件。

## 现在能用什么

yiTrace 面向正在做 Agent 工程的团队：排查真实 run、构建 eval 闭环、查看工具行为，
并把执行历史保存在本地、保持可搜索。

| 能力 | 你能得到什么 |
|---|---|
| 本地 TraceDB | WAL、重启恢复、快照读取 |
| Python | SDK 和 embedded DB 已发布到 PyPI |
| Node / Electron | embedded DB 已发布到 npm |
| TypeScript | 轻量打点 SDK 已发布到 npm |
| Rust | SDK 和 embedded wrapper 在本仓库 |
| 搜索 | 中文 BM25、向量召回、混合检索 |
| 过滤 | tenant、agent、状态、时间、attrs |
| 回放 | session、trace、span、log event 视图 |
| eval 数据 | annotation 和 dataset hooks |
| Agent Memory | 可搜索的执行历史，记录什么有效、什么失败 |
| 控制台 | 本地回放 UI |

实现边界见 [Current State](docs/CURRENT_STATE.md)。

## 仓库结构

```text
yitrace-engine/              # Rust engine workspace
yitrace-console/             # React 回放控制台
yitrace-sdk/python/          # Python 打点 SDK: yitrace
yitrace-sdk/typescript/      # TypeScript 打点 SDK: @yitrace/trace-sdk
yitrace-sdk/rust/            # Rust 打点 SDK
yitrace-node/                # Node/Electron embedded DB: @yitrace/db
yitrace-db-python/           # Python embedded DB: yitrace-db
yitrace-db-rs/               # Rust embedded DB wrapper
yitrace-segstore-vortex/     # 可选 Vortex 段存储适配
yitrace-tokenizer-jieba/     # 可选分词适配
yitrace-vecindex-graph/      # 可选图索引适配
docs/                        # 公开 API 文档、当前态索引、截图
```

## License

MIT
