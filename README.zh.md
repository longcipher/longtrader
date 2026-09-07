# LongTrader

> [English](README.md) | **中文**

基于契约优先的 ConnectRPC API 构建的本地策略宿主。策略仅依赖生成的 protobuf 端口；daemon/terminal 后端可插拔，同一策略可在 Rust、Python、TypeScript 和 Go 中运行。

> **状态：** 开源预览版。`proto/` 下的契约为唯一事实来源（由 `buf` 管理）；所有语言的 SDK 均为生成桩代码的轻量封装。本项目为 LongTrader 团队从头原创实现。

## 特性

- **契约优先** — `proto/longtrader/**` 定义了交易、行情、worker、数据与回测服务。CI 中通过 `buf breaking` 检测破坏性变更。
- **可移植策略** — `bin/longtrader-worker/src/strategies/` 下 20+ 原生策略仅依赖 `TradingGateway`/`MarketDataSource` 抽象（`ports.rs`），无隐藏的领域耦合。
- **会话控制面** — 确定性的 `ATTACHED → SYNCING → ACTIVE → KILL_SWITCH_TRIPPED` 生命周期、原子化 `ReconcileState` 快照、3 倍心跳租约、按会话的 `KillSwitchPolicy`（session/all/none）。
- **轻量 SDK** — `sdks/{python,typescript,go}` 遵循相同的六目录规范布局（`contract/session/ports/adapters/strategies/examples`），生成代码永不手工修改。
- **Worker 加固** — 仅使用 `tracing`、`eyre`/`thiserror` 错误分层、`hpx`+`rustls`、`tokio` 异步、严格的 `clippy::pedantic`/`nursery` 检查。

## 架构

```
proto/                        # buf v2，唯一事实来源 (longtrader.*.v1)
crates/longtrader-contract/   # 生成类型 + ConnectRPC 特征 (build.rs → buffa/connectrpc)
bin/longtrader-worker/        # 策略宿主：ports、session、adapters、envelope、strategies
bin/longtrader-cli/           # Terminal API CLI 客户端（二进制：longtrader）
sdks/{python,typescript,go}/  # 生成桩之上的轻量封装 (contract/session/ports/...)
```

各语言规范布局：

| 规范目录 | Rust | Python | TypeScript | Go |
|---|---|---|---|---|
| `contract/` | `crates/longtrader-contract` | `sdks/python/longtrader_sdk/proto/` | `sdks/typescript/src/gen/` | `sdks/go/gen/` |
| `session/` | `bin/longtrader-worker/src/session/` | `longtrader_sdk/session.py` | `src/session.ts` | `session/` |
| `ports/` | `src/ports.rs` | `longtrader_sdk/ports.py` | `src/ports.ts` | `ports/` |
| `adapters/` | `src/adapters/{remote,mock}.rs` | Connect-over-httpx 于 `session.py` | Connect-over-undici 于 `session.ts` | Connect-over-http 于 `session/` |
| `strategies/` | `src/strategies/` | `examples/grid_strategy.py` | `examples/grid_strategy.ts` | `strategies/` |
| `examples/` | `examples/` / `docs/` | `sdks/python/examples/` | `sdks/typescript/examples/` | `sdks/go/examples/` |

## CLI（`longtrader-cli`）

`longtrader-cli` 二进制提供 LongTrader Terminal API 的命令行接口。二进制名称：`longtrader`。

```bash
# 构建
cargo build -p longtrader-cli

# 全局标志
--endpoint <url>    # Terminal API 端点（默认：http://127.0.0.1:8810）
--token <token>     # Bearer token 认证
--venue <name>      # 交易所名称（默认：mock）
--format <format>   # 输出格式：table, json（默认：table）

# 命令
longtrader health                                    # 健康检查
longtrader venues                                    # 列出已连接交易所
longtrader symbols --venue mock                      # 列出可交易品种
longtrader candles <SYMBOL> --timeframe M1 --limit 100 # K线数据
longtrader book <SYMBOL> --depth 10                  # 订单簿快照
longtrader search <QUERY> --limit 50                 # 搜索品种
longtrader account                                   # 账户余额
longtrader positions                                 # 当前持仓
longtrader orders --symbol <SYMBOL>                  # 当前挂单
longtrader history --limit 100                       # 历史订单
longtrader buy <SYMBOL> <QTY> --price <PRICE>        # 下限买单
longtrader sell <SYMBOL> <QTY> --price <PRICE>       # 下限卖单
longtrader cancel <ORDER_ID>                         # 取消订单
longtrader close <POSITION_ID>                       # 平仓
longtrader stream --topics MARKET_LITE,TRADING       # 订阅实时更新
longtrader strategies                                # 列出策略
```

时间周期：`M1`、`M5`、`M15`、`M30`、`H1`、`H4`、`D1`、`W1`。
主题：`MARKET_LITE`、`MARKET_HEAVY`、`TRADING`、`RUNTIME`、`FUNDING`、`DERIVATIVES`。

## 快速开始

前置要求：Rust stable + nightly（`rustfmt`/`clippy`）、`just`、`buf`（用于契约检查与生成）。

```bash
just setup          # 安装 cargo-mutants、cargo-shear、cargo-sort、typos、rumdl
just check          # cargo check --all-targets --all-features
just lint           # typos + rumdl + cargo sort/fmt + clippy -D warnings + shear
just test           # cargo test --all-features（单元测试 + proptest）
just build          # cargo build --workspace

# 契约
just proto-lint     # buf lint 于 proto/
just proto-breaking # 相对 main 分支的破坏性变更检查
just sdk-generate   # 本地桩生成（通过 scripts/gen-proto.sh 离线安全）

# 运行 worker（示例）
cargo run -p longtrader-worker -- --config config.toml

# 构建并运行 CLI
cargo run -p longtrader-cli -- --help
longtrader health                    # 健康检查
longtrader symbols --venue mock      # 列出品种
longtrader candles BTCUSDT --timeframe M1 --limit 100
longtrader buy BTCUSDT 0.001 --price 95000
```

最小 `config.toml`：

```toml
daemon_endpoint = "http://127.0.0.1:8080"
api_endpoint = "http://127.0.0.1:7888"      # 设置后 backend = "terminal"
api_token_file = "/run/secrets/longtrader_token"
listen_endpoint = "127.0.0.1:9000"          # WorkerSessionService + 代理

[strategy]
type = "simple_grid"
[strategy.params]
symbol = "BTCUSDT"
exchange_id = "mock"
lower_price = "90000"
upper_price = "110000"
num_levels = 10
qty_per_level = "0.001"
```

Token 仅从 `api_token_file` 加载（自动 trim，永不落日志）；通过 `hpx` + `rustls` 以 `Authorization: Bearer <token>` 发送。

## SDK

```bash
# Python
just sdk-generate
pip install -e sdks/python
python sdks/python/examples/grid_strategy.py --help

# TypeScript
just sdk-generate
cd sdks/typescript && npm install && npm run build
npx tsx examples/grid_strategy.ts --help

# Go（通过 buf 生成桩，paths=source_relative）
just sdk-generate
go run ./sdks/go/examples/grid_strategy --help
```

最小使用示例（所有语言生命周期一致 — Attach → 心跳 → ReconcileState 门控至 ACTIVE）：

```python
from longtrader_sdk import Session
s = Session.attach("http://127.0.0.1:8080", token="YOUR_TERMINAL_TOKEN")
s.start_heartbeat()
snap = s.reconcile_state()
print(s.session_id, s.state, snap.snapshot_sequence)
s.close()
```

```ts
import { Session } from "@longtrader/sdk";
const s = await Session.attach("http://127.0.0.1:8080", "YOUR_TERMINAL_TOKEN");
s.startHeartbeat();
const snap = await s.reconcileState();
console.log(s.sessionId, s.state, snap.snapshotSequence);
s.stop();
```

## 策略

`bin/longtrader-worker/src/strategies/<name>/` 下的每个策略均配有 `README.md`，通过 `strategy.type` 选择：

| 分类 | 策略 |
|---|---|
| 网格 / 做市 | `simple_grid`、`boll_grid`、`hedge_grid`、`fixed_maker`、`cross_maker`、`cross_depth_maker`、`cross_fixed_maker` |
| 趋势跟踪 | `ema_cross`、`supertrend` |
| 组合与再平衡 | `rebalance`、`market_cap`、`dca_scheduler` |
| 风控与运维 | `sentinel`、`autoborrow`、`balance_align`、`convert`、`deposit_transfer` |
| 信号与分析 | `irr`、`nav_recorder`、`premium_monitor`、`random_entry`、`xfunding_lite` |

所有策略均为 LongTrader 原生实现，仅使用 `ports::{TradingGateway, MarketDataSource}`；传输层选择位于 `adapters/`。

## 开发

```bash
just format     # rumdl fmt + cargo sort + cargo +nightly fmt
just fix        # rumdl --fix + clippy --fix
just mutation   # cargo mutants（聚焦 library crates）
just ci         # lint + test + build（与 CI 一致）
```

工作区规则：根 `[workspace.dependencies]` 仅承载数字版本（无默认特性）；子 crate 使用 `workspace = true`。优先使用 `hpx` 而非 `reqwest`、`tokio` 处理异步、`tracing` 负责可观测性。

## 安全

- 不提交任何密钥。Token 仅通过文件加载（`api_token_file`）并以常量时间 `subtle` 比较。
- `proto/longtrader/exchange/v1/exchange_daemon.proto` 已在此开源版本中移除（遗留私有 daemon API），请使用 `longtrader.market/trading/stream/ops.v1`。
- 请通过 GitHub Security Advisories 报告漏洞；敏感问题请勿公开提 issue。

## 许可证

Apache-2.0 — 见 `LICENSE`。SDK 包（`sdks/python`、`sdks/typescript`）除非另有说明，同样为 Apache-2.0。

## 致谢

基于 `buffa`/`connectrpc`、`hpx`、`tokio`、`rust_decimal`、`buf` 构建。
