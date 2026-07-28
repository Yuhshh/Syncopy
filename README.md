# Syncopy

Syncopy is an asynchronous, reliability-first copy-trading service for Polymarket. It
polls authoritative target positions, maintains local CLOB books, sizes a proportional
portfolio, and submits bounded marketable limit orders. Paper mode is the default.

> Trading is financially risky and may be restricted by jurisdiction. This software is
> not financial advice. Keep `DRY_RUN=true` until you have reviewed paper results,
> allowances, wallet type, geographic eligibility, and kill-switch behavior.

## What is implemented

- Current Data API target position/trade polling with pagination and retry/backoff.
- Market WebSocket snapshots and deltas, dynamic token subscriptions, heartbeat,
  reconnect backoff, and strict `STALE` gating.
- REST-over-stream reconciliation for target positions, own live positions, and books.
- Proportional multi-target sizing with target weights, liquidity caps, and per-market
  capital caps.
- Adverse-selection, resolution-proximity, blocklist, order-size, portfolio exposure,
  daily-loss, and environment kill switches.
- Paper fills against recorded order-book depth and a live adapter for Polymarket's
  supported unified Python SDK.
- Durable idempotency reservation before submission, TTL cancellation, structured JSON
  logs, batched raw-event persistence, and replay backtesting.
- PostgreSQL schema/Alembic migration, FastAPI health endpoints, Docker Compose, and
  systemd unit.

## Current API compatibility

This repository was built against the July 2026 Polymarket API contracts:

- Public APIs: Gamma `https://gamma-api.polymarket.com`, Data
  `https://data-api.polymarket.com`, and CLOB `https://clob.polymarket.com`.
- Market WebSocket:
  `wss://ws-subscriptions-clob.polymarket.com/ws/market`, subscribing with
  `{"assets_ids": [...], "type": "market"}`.
- Target positions use `GET /positions?user=...`; target price hints use
  `GET /trades?user=...`.
- Live execution uses `polymarket-client` and `AsyncSecureClient`. The old
  `py-clob-client` repository is archived and says it is no longer functional.

The execution SDK is isolated in `src/syncopy/execution.py`; future SDK changes should
only require an adapter update.

Official references:

- [API overview](https://docs.polymarket.com/api-reference/predictions/overview)
- [Market WebSocket](https://docs.polymarket.com/api-reference/wss/market)
- [Current positions](https://docs.polymarket.com/api-reference/core/get-current-positions-for-a-user)
- [User trades](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets)
- [Rate limits](https://docs.polymarket.com/api-reference/rate-limits)
- [Unified Python SDK](https://github.com/Polymarket/py-sdk)

## Architecture

```text
Gamma/Data/CLOB REST ──> typed API clients ──┐
                                             ├─> RuntimeState ─> sizing/risk ─> execution
Market WebSocket ──────> MarketIngestor ─────┘          ▲
                            │                           │
                            └─ raw event log     Reconciler (REST authority)

PostgreSQL: markets, targets, snapshots, own positions, orders, fills,
            raw events, reconciliation divergences, PnL snapshots
```

The in-memory book is never silently trusted after a disconnect. A connection worker
marks every affected token stale, applies a CLOB REST snapshot, then resumes trading.
Periodic reconciliation repeats the authoritative overwrite and records every
divergence.

## Quick start with Docker

Requirements: Docker with Compose.

```bash
cd syncopy
cp .env.example .env
```

Edit `.env` and set at least:

```dotenv
DRY_RUN=true
TARGETS=0xYourTargetAddress:1.0
TARGET_CAPITALS=0xYourTargetAddress:25000
OWN_CAPITAL_USD=1000
```

Then:

```bash
docker compose up --build
```

Inspect:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
docker compose logs -f syncopy
```

The first target snapshot is treated as a desired portfolio, so paper mode can mirror
positions that already exist when the service starts.

## Local development

Python 3.11+ and PostgreSQL 15+ are required.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,live,backtest]"
cp .env.example .env
alembic upgrade head
syncopy doctor
syncopy run
```

Useful commands:

```bash
syncopy discover --limit 10
syncopy init-db
pytest
ruff check .
```

`AUTO_CREATE_SCHEMA=true` lets the service create the v1 tables during development.
Production deployments should run `alembic upgrade head` and may set it to `false`.

## Configuration

Percent settings are entered as percentages (`5` means 5%). Decimal arithmetic is used
throughout strategy, risk, price, size, and accounting code.

| Variable | Default | Meaning |
|---|---:|---|
| `DRY_RUN` | `true` | Paper execution; no signing or exchange submissions |
| `KILL_SWITCH` | `false` | Reject all new orders |
| `TARGETS` | empty | Comma-separated `address:weight` entries |
| `TARGET_CAPITALS` | empty | Comma-separated `address:estimated_usd` entries |
| `OWN_CAPITAL_USD` | `1000` | Strategy capital denominator |
| `POLL_INTERVAL_S` | `4` | Authoritative target polling cadence |
| `RECONCILE_INTERVAL_S` | `20` | Full authoritative reconciliation cadence |
| `MAX_MARKET_EXPOSURE_PCT` | `5` | Per-token market value cap |
| `MAX_TOTAL_EXPOSURE_PCT` | `50` | Portfolio market value cap |
| `MAX_DEPTH_FRACTION` | `0.10` | Fraction of in-tolerance book depth usable |
| `SLIPPAGE_TOLERANCE_PCT` | `2` | Depth and execution tolerance |
| `MAX_TARGET_PRICE_MOVE_PCT` | `3` | Skip after too much movement from target hint |
| `ORDER_TTL_S` | `15` | Cancel old live orders |
| `DAILY_LOSS_HALT_USD` | `50` | Cancel all and halt at daily loss threshold |
| `MIN_DAYS_TO_RESOLUTION` | `1` | Avoid near-resolution markets |
| `BLOCKED_TOKEN_IDS` | empty | Comma-separated outcome token IDs |

See `.env.example` for the complete list.

### Multiple targets

The desired raw position is:

```text
sum(target_shares × own_capital / target_capital × target_weight)
```

Liquidity and market-risk caps are applied after aggregation. Set an explicit
`TARGET_CAPITALS` entry for every target when possible; otherwise
`DEFAULT_TARGET_CAPITAL_USD=10000` is used.

## Live-mode checklist

Do not enable live mode until all of these are true:

1. Paper mode has run long enough to capture representative delayed-copy behavior.
2. The funded Polymarket Deposit Wallet is set as `POLYMARKET_DEPOSIT_WALLET`.
3. `POLYMARKET_PRIVATE_KEY` (the legacy `POLYGON_PRIVATE_KEY` name is also accepted)
   belongs to its authorized signer and is stored outside source control.
4. Trading approvals/allowances are enabled for that wallet.
5. `syncopy doctor` passes, `/readyz` is ready, and stale-state behavior is tested.
6. `KILL_SWITCH=true` has been exercised in a disposable/paper environment.
7. Exposure, order-size, loss, blocklist, and resolution windows are conservative.

Then set `DRY_RUN=false`. Startup derives/validates API credentials through the official
SDK. Syncopy never prints the private key.

Durable idempotency is deliberately fail-safe: an intent is inserted before submission.
If the process loses the exchange response, that same state transition is not submitted
again. Inspect/reconcile the uncertain order instead of assuming it failed.

## Backtesting

Replay the PostgreSQL event log:

```bash
syncopy backtest --after 2026-07-01T00:00:00+00:00 --fee-bps 0 \
  --output backtest-output/report.json
```

Or replay exported JSONL, one event object per line:

```bash
syncopy backtest --jsonl events.jsonl --fee-bps 0
```

The replay engine uses the same `PositionSizer`, book-depth logic, and marketable-limit
calculation as live mode. It fills only against displayed opposing depth at or better
than the bounded limit. Reports include equity/PnL, realized/unrealized PnL, fees,
trade details, skips, and peak exposure.

## Operations

- `GET /healthz`: liveness, database state, loop timestamps, stale-book count, halt.
- `GET /readyz`: `503` if PostgreSQL is unavailable, any subscribed book is stale, or
  initial live reconciliation is incomplete.
- `GET /state`: non-secret runtime counts.
- Equity/PnL is snapshotted on `PNL_SNAPSHOT_INTERVAL_S`; live equity uses the
  authenticated collateral balance plus marked outcome tokens.
- JSON logs go to stdout; replayable messages go to `events`.
- Divergences go to `reconciliation_events`.

The systemd template is in `deploy/syncopy.service`. Adjust its paths and environment
file before installing it.

## Remaining production decisions

Before your first deployment, supply the target address(es), estimated target capital,
operator capital, and desired paper-test window. Before live mode, also select the
funded Deposit Wallet/signing setup and confirm local legal eligibility. The optional
Redis cache and C++ hot path are intentionally omitted from v1; profile the Python
ingestor first, since target detection remains REST-poll-bound.
