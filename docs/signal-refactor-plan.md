# Signal-source refactor plan

**Context:** Polymarket's offshore venue (polymarket.com) is geoblocked for US traders. US users
are routed to polymarket.us, a separately CFTC-regulated venue that does not expose other users'
positions through any public or institutional API. Copy trading is not implementable on the US
venue by design. This document maps what must change to pivot the signal source while keeping the
reconciliation, sizing, risk, and backtesting layers intact.

Leading replacement strategy: cross-venue dislocation between polymarket.us and Kalshi (both
CFTC-regulated, US-legal), trading the spread when the same outcome is priced differently.

`DRY_RUN` stays true throughout this work. Nothing executes against real money.

---

## 1. Module map

### Coupled to "copy a specific trader"

| Module | What ties it to copy trading |
|---|---|
| `config.py` | `TARGETS` and `TARGET_CAPITALS` keyed by EVM address; `ADDRESS_RE` validator; `polymarket_private_key` / `polymarket_deposit_wallet` credential block; `validate_targets` enforces wallet address format on both dicts |
| `tracker.py` | `TargetTracker` — the copy-trading loop. Polls `DataApi.positions(address)` for each target wallet, diffs snapshots, fires `TargetPositionChange` |
| `reconcile.py:reconcile_target` | Re-fetches authoritative positions per target wallet and re-fires `TargetPositionChange` on drift. This entire method is copy-trading signal logic living inside what should be a pure state-correctness layer |
| `reconcile.py:reconcile_own` | Fetches own positions by on-chain wallet address via `DataApi`; survives a venue change but the client call changes |
| `clients.py:DataApi` | `positions(address)` and `trades(address)` — entire class is a Polymarket data indexer that serves other users' on-chain positions |
| `messages.py:DataPosition` | `proxy_wallet` field; venue-specific wire format |
| `messages.py:DataTrade` | `transaction_hash` — an on-chain Polygon tx hash used to derive price hints |
| `domain.py:TargetPositionChange` | `address` field is a wallet address; this is the signal event type and it is copy-trading-specific in shape |
| `service.py:_on_target_change` | The central copy-trading logic: receives a `TargetPositionChange`, aggregates across all target wallets, computes desired position, calls execution |
| `db.py:TargetRow` | `address: String(42)` — column sized and typed for an EVM address |
| `backtest.py:ReplayEngine._target_change` | Handles `target_position_change` event type with `address` / `after_size` fields |

### Signal-source agnostic

These modules have no structural dependence on copy trading or wallets:

| Module | Why it's clean |
|---|---|
| `state.py` | `OrderBook`, `RuntimeState` — generic price and position state; no wallet concept |
| `strategy.py` | `PositionSizer`, `marketable_limit` — pure sizing math with no signal assumptions |
| `risk.py` | `RiskManager` — portfolio-level checks; references `Settings` only for numeric limits |
| `execution.py:ExecutionEngine` | Order lifecycle (paper and live); `PolymarketExecutor` is venue-specific but the engine itself receives a `(token_id, desired_size)` call and is otherwise generic |
| `markets.py` | `MarketRegistry` — market metadata cache; uses `ClobApi.market_info()` so it is Polymarket-specific, but the interface is clean |
| `ingest.py` | `MarketIngestor` — WS order book feed; Polymarket-specific protocol, but the data it produces (`OrderBook` updates) is venue-agnostic |
| `events.py`, `db.py:Repository` (most of it), `health.py`, `domain.py` (most) | Pure infrastructure |

**Rough count of lines to change:** tracker (~155 lines), `reconcile_target` (~40 lines), copy-trading config validators (~40 lines), `service._on_target_change` (~70 lines), `DataApi` (~45 lines). That is roughly 350 lines of signal-coupled code out of ~2,200 total. The execution, sizing, risk, and ingest machinery is intact.

---

## 2. Every place that assumes a wallet address, on-chain data, or another user's positions

**Config / validation:**
- `config.py:9` — `ADDRESS_RE` compiled for `0x` + 40 hex chars
- `config.py:48-49` — `targets` and `target_capitals` dict keys are EVM addresses
- `config.py:91-109` — `validate_targets` / `validate_target_capitals` both enforce `ADDRESS_RE`
- `config.py:111-124` — `validate_live_credentials` requires `polymarket_deposit_wallet` to pass `ADDRESS_RE` (execution wallet)
- `config.py:127` — `target_capital(address)` looks up by address

**Domain types:**
- `domain.py:69` — `TargetPositionChange.address` — carries a wallet address as the signal source identity
- `messages.py:14` — `DataPosition.proxy_wallet` — Polymarket's proxy wallet in the wire format
- `messages.py:36` — `DataTrade.transaction_hash` — on-chain Polygon tx hash; used in `tracker.py:86` to derive `price_hint`

**Clients:**
- `clients.py:62-107` — all of `DataApi`: `positions(address)` and `trades(address)` query a public Polymarket indexer that exposes any wallet's on-chain state; neither endpoint exists on polymarket.us

**Tracker (entirely):**
- `tracker.py:40` — stores `address.lower()` as EVM convention
- `tracker.py:78-155` — `poll_target(address)` fetches another user's positions; the whole method is inapplicable on a centralized venue

**Reconciler (partially):**
- `reconcile.py:140-157` — `reconcile_own()` calls `data_api.positions(self.own_wallet)`; the two-tier model survives but the client call changes (authenticated REST endpoint, not a public wallet lookup)
- `reconcile.py:159-201` — `reconcile_target(address)` — fetches another user's positions; entirely copy-trading specific and drops out with the tracker

**Service:**
- `service.py:94` — `own_wallet = settings.polymarket_deposit_wallet if not dry_run else None`
- `service.py:248-253` — iterates `settings.targets` by wallet address to aggregate position across multiple targets

**Database schema:**
- `db.py:65-72` — `TargetRow.address: String(42)` sized for EVM addresses
- `db.py:75-78` — `TargetPositionRow.address` FK back to `TargetRow.address`
- `db.py:250-270` — `upsert_target(address, ...)` stores by EVM address

**Backtest:**
- `backtest.py:152-153` — `_target_change` reads `payload["address"]` and uses it as a dict key

---

## 3. Signal-source interface

There is no interface today. The boundary between "signal" and "execution" is buried inside
`service._on_target_change()`, which does three things at once: resolves market info, computes
desired size across all targets, and calls `execution.process()`. The `Reconciler` also doubles
as a signal source by re-firing `TargetPositionChange` events on position drift.

### Proposed seam

A `SignalEvent` value type and a `SignalSource` Protocol:

```python
# domain.py addition
@dataclass(frozen=True, slots=True)
class SignalEvent:
    token_id: str
    desired_size: Decimal          # absolute desired position in shares
    price_hint: Decimal | None     # expected fill price (adverse-selection guard)
    reason: str                    # human-readable label for logging/audit
    observed_at: datetime = field(default_factory=utcnow)

# signal.py (new file)
class SignalSource(Protocol):
    async def run(self) -> None: ...
    async def stop(self) -> None: ...
    # Implementation calls the registered callback whenever the desired
    # position in a token should be re-evaluated.
    # Callback signature: (SignalEvent) -> Awaitable[None]
```

`SignalEvent.desired_size` is an **absolute target**, not a delta. The execution layer already
works with absolute desired positions (`execution.process(token_id, desired_position, ...)`).
The copy-trading sizing math (scaling by target capital, aggregating weights) moves fully inside
`CopyTradingSignalSource`. The dislocation strategy's sizing math lives inside
`DislocaitonSignalSource` — it computes `desired_size` from the spread magnitude rather than
mirroring a target.

`service._on_target_change` becomes a thin adapter that calls
`execution.process(event.token_id, event.desired_size, reason=event.reason, price_hint=event.price_hint)`.
The sizing logic moves out of the service orchestrator and into the signal source.

The `Reconciler` gets a corresponding change: its `on_target_change` callback (currently a
`ChangeCallback` firing `TargetPositionChange`) becomes `on_signal: SignalCallback | None` firing
`SignalEvent`. In the copy-trading setup the reconciler still runs the same sizing math currently
in `reconcile_target`; in the dislocation setup `on_signal` is wired to the dislocation source's
callback instead and `reconcile_target` is not called.

---

## 4. Migration plan

### Phase 0 (now, no code): This document

Confirm the seam, lock `DRY_RUN=True`, do not touch execution or sizing math.

---

### Phase 1: Extract `SignalEvent` and a `SignalSource` protocol (pure refactor, no behavior change)

Goal: establish the interface without breaking anything. The copy-trading machinery still runs.

1. Add `SignalEvent` to `domain.py`. It carries `token_id`, `desired_size`, `price_hint`, `reason`.
2. Create `signal.py` with the `SignalSource` Protocol and a `CopyTradingSignalSource` wrapper
   class that owns `TargetTracker` and the sizing logic from `service._on_target_change`.
3. Move the sizing computation out of `service._on_target_change` into `CopyTradingSignalSource`.
   The service's `_on_target_change` becomes a one-liner that calls `execution.process(...)`.
4. Wire `Reconciler.on_target_change` to produce `SignalEvent` through the same path (it
   currently calls back into the same `_on_target_change`).
5. Tests pass unchanged because the math moved, not changed.

DB and config are untouched. This phase is reversible in a single commit.

---

### Phase 2: Decouple config from EVM addresses

Goal: `targets` keys become generic string identifiers; EVM validation is confined to a
copy-trading sub-config.

1. Introduce `source_type: Literal["copy_trader", "dislocation"] = "copy_trader"` in `Settings`.
2. Move `ADDRESS_RE`, `validate_targets`, and `validate_target_capitals` behind a
   `source_type == "copy_trader"` conditional validator, or into `CopyTradingSignalSource.__init__`.
3. `polymarket_deposit_wallet` validation (`ADDRESS_RE.match`) stays — it is execution, not signal.
4. `targets` becomes `dict[str, Decimal]` with no format constraint at the `Settings` level.
   Copy-trading source validates its own inputs.
5. `target_capital(address)` method stays as-is; it is just a dict lookup.

No DB migration needed yet. Schema still has `String(42)` address columns; they will not be
validated at parse time for a non-copy-trading source.

---

### Phase 3: Implement `DislocaitonSignalSource`

Goal: ingest price from two venues, emit `SignalEvent` when spread threshold is breached.

1. Add `KalshiApi` client (REST + WS) analogous to `ClobApi` / `MarketIngestor`. The book data
   maps directly into `OrderBook`.
2. Add a market mapping config: `dislocation_pairs` — a list of
   `{polymarket_token_id, kalshi_market_id, spread_threshold_pct}`.
3. `DislocaitonSignalSource.run()` subscribes to both venues' order books, computes mid-price
   spread on each tick, and emits `SignalEvent(desired_size=X)` when spread ≥ threshold. The
   sizing is based on own capital and spread magnitude, not on mirroring a target — but it still
   flows through the same `PositionSizer` with a synthetic `SizingInput`.
4. Wire it into `service.start()` under `source_type == "dislocation"`. `TargetTracker` and
   `reconcile_target` are simply not started.

`reconcile_own()` continues to work: for polymarket.us it calls an authenticated REST endpoint
scoped to your account — just a different `DataApi`-equivalent client.

---

### Phase 4: Schema migration (lowest urgency, do last)

The `targets` / `target_positions` tables are useful audit history regardless of signal source.
Two options:

- **Rename** `TargetRow.address` → `TargetRow.source_id` (`String(200)`, drop the 42-char cap),
  add `source_type` column, write an Alembic migration.
- **Add a new table** `signal_sources` for the dislocation strategy and leave the existing
  copy-trading tables as historical record.

The second option is safer: it preserves all historical copy-trading snapshots for the existing
backtest harness and avoids a data migration on live data.

---

## Flagged: on-chain finality semantics

**`DataTrade.transaction_hash` (`messages.py:36`):** Used in `tracker.py:86` to derive
`price_hint` for a `TargetPositionChange`. On Polymarket offshore, each trade is a confirmed
Polygon transaction; the position snapshot only updates after on-chain finality, so the hint is
guaranteed to be from a settled fill. On a centralized venue, fills arrive via WS and the REST
position snapshot may lag by one poll cycle. The hint derivation logic is fine; the field name
and client call change, and the REST authoritative snapshot may trail WS by milliseconds rather
than being strictly ahead of it as on-chain finality guarantees.

**`reconcile_own()` trust model (`reconcile.py:140-157`):** The two-tier pattern (WS
fast-but-untrusted, REST authoritative) still holds on a centralized venue. On Polymarket
offshore the REST source is a blockchain indexer, so a WS update that has not been mined cannot
appear in REST. On polymarket.us / Kalshi, REST lags WS by the exchange's settlement cycle
(typically milliseconds to seconds, not a block). The reconciler's `_position_json` diff will
therefore fire more often on rapid fills — not incorrect, just noisier. No logic change is
needed, but `reconcile_interval_s` may warrant tuning upward to reduce churn.

---

## Flagged: backtest harness coupling

The `ReplayEngine` handles one signal event type, `target_position_change`, at `backtest.py:149`.
The replay is inherently copy-trading-specific. Two implications:

1. **Historical copy-trading backtests remain valid.** The existing event log can still be
   replayed through `ReplayEngine` to evaluate the copy-trading strategy's historical PnL.
   Nothing is invalidated.

2. **The dislocation strategy needs a new event type and a new replay path.** A
   `dislocation_signal` event has a different shape (two venue prices, a spread, a desired size
   derived from own capital). A `_dislocation_signal` handler would be added to `ReplayEngine`
   with a different sizing path. The two strategies produce separate backtest reports from
   separate event logs and are not comparable in a single replay run.

The correct approach is to emit `dislocation_signal` events with a `signal_type` field so the
replay engine can dispatch on it, rather than mixing the two schemas into `target_position_change`.
