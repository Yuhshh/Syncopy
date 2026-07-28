from __future__ import annotations

import asyncio
from decimal import Decimal

import aiohttp
import structlog
import uvicorn

from syncopy.clients import ClobApi, DataApi, GammaApi
from syncopy.config import Settings
from syncopy.db import Database, Repository
from syncopy.domain import ZERO, Freshness, SizingInput, TargetPositionChange
from syncopy.events import EventWriter
from syncopy.execution import ExecutionEngine, PolymarketExecutor
from syncopy.health import create_app
from syncopy.ingest import MarketIngestor
from syncopy.markets import MarketRegistry
from syncopy.reconcile import Reconciler
from syncopy.risk import RiskManager
from syncopy.state import RuntimeState
from syncopy.strategy import PositionSizer
from syncopy.tls import default_ssl_context
from syncopy.tracker import TargetTracker

log = structlog.get_logger()


class EmbeddedUvicornServer(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        """The parent process owns SIGINT/SIGTERM handling."""


class SyncopyService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.state = RuntimeState()
        self.database = Database(settings.database_url)
        self.repository = Repository(self.database)
        self.http: aiohttp.ClientSession | None = None
        self.events: EventWriter | None = None
        self.clob: ClobApi | None = None
        self.data_api: DataApi | None = None
        self.gamma: GammaApi | None = None
        self.markets: MarketRegistry | None = None
        self.risk: RiskManager | None = None
        self.execution: ExecutionEngine | None = None
        self.reconciler: Reconciler | None = None
        self.tracker: TargetTracker | None = None
        self.ingestor: MarketIngestor | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()
        self._stopped = asyncio.Event()
        self._shutdown_lock = asyncio.Lock()
        self._token_locks: dict[str, asyncio.Lock] = {}
        self._server: uvicorn.Server | None = None

    async def start(self) -> None:
        if self.settings.auto_create_schema:
            await self.database.create_schema()
        timeout = aiohttp.ClientTimeout(total=self.settings.http_timeout_s)
        connector = aiohttp.TCPConnector(ssl=default_ssl_context())
        self.http = aiohttp.ClientSession(timeout=timeout, connector=connector)
        self.events = EventWriter(self.repository)
        await self.events.start()
        self.clob = ClobApi(self.settings.clob_api_url, self.http)
        self.data_api = DataApi(self.settings.data_api_url, self.http)
        self.gamma = GammaApi(self.settings.gamma_api_url, self.http)
        self.markets = MarketRegistry(self.clob, self.repository)
        self.risk = RiskManager(self.settings, self.state)
        live_executor = None
        if not self.settings.dry_run:
            assert self.settings.polymarket_private_key is not None
            assert self.settings.polymarket_deposit_wallet is not None
            live_executor = PolymarketExecutor(
                self.settings.polymarket_private_key.get_secret_value(),
                self.settings.polymarket_deposit_wallet,
            )
        self.execution = ExecutionEngine(
            self.settings,
            self.state,
            self.repository,
            self.events,
            self.markets,
            self.risk,
            live_executor,
        )
        if self.settings.dry_run:
            persisted_positions = await self.repository.load_own_positions()
            await self.state.set_own_positions(
                {position.token_id: position for position in persisted_positions}
            )
        await self.execution.start()
        own_wallet = self.settings.polymarket_deposit_wallet if not self.settings.dry_run else None
        self.reconciler = Reconciler(
            self.clob,
            self.data_api,
            self.repository,
            self.events,
            self.state,
            own_wallet=own_wallet,
            targets=self.settings.targets,
            interval_s=self.settings.reconcile_interval_s,
            daily_loss_halt_usd=self.settings.daily_loss_halt_usd,
            on_target_change=self._on_target_change,
            on_halt=self.execution.cancel_all_and_halt,
        )
        self.ingestor = MarketIngestor(
            self.settings.market_ws_url,
            self.clob,
            self.state,
            self.events,
            batch_size=self.settings.ws_batch_size,
            on_reconnect=self.reconciler.reconcile_all,
        )
        self.ingestor.set_assets(self.settings.market_token_ids)
        self.tracker = TargetTracker(
            self.data_api,
            self.repository,
            self.events,
            self.state,
            self.settings.targets,
            self.settings.target_capitals,
            default_capital=self.settings.default_target_capital_usd,
            interval_s=self.settings.poll_interval_s,
            on_change=self._on_target_change,
            on_assets=self.ingestor.add_assets,
        )
        await self.tracker.initialize()
        if not self.settings.dry_run:
            await self.reconciler.reconcile_own()
        app = create_app(self.settings, self.state, self.database)
        config = uvicorn.Config(
            app,
            host=self.settings.health_host,
            port=self.settings.health_port,
            log_config=None,
            access_log=False,
        )
        self._server = EmbeddedUvicornServer(config)
        self._tasks = [
            asyncio.create_task(self.ingestor.run(), name="market-ingestor"),
            asyncio.create_task(self.tracker.run(), name="target-tracker"),
            asyncio.create_task(self.reconciler.run(), name="reconciler"),
            asyncio.create_task(self.execution.ttl_loop(), name="order-ttl"),
            asyncio.create_task(self.execution.user_event_loop(), name="user-ingestor"),
            asyncio.create_task(self.execution.pnl_loop(), name="pnl-snapshots"),
            asyncio.create_task(self._server.serve(), name="health-api"),
        ]
        for task in self._tasks:
            task.add_done_callback(self._task_done)
        await self.events.emit(
            "service",
            "started",
            {
                "dry_run": self.settings.dry_run,
                "targets": list(self.settings.targets),
                "seed_assets": self.settings.market_token_ids,
            },
        )
        log.info(
            "syncopy_started",
            dry_run=self.settings.dry_run,
            targets=len(self.settings.targets),
            health_port=self.settings.health_port,
        )

    async def run(self) -> None:
        await self.start()
        await self._stopping.wait()

    def request_stop(self) -> None:
        self._stopping.set()

    async def stop(self) -> None:
        if self._stopped.is_set():
            return
        async with self._shutdown_lock:
            if self._stopped.is_set():
                return
            self._stopping.set()
            if self.ingestor is not None:
                await self.ingestor.stop()
            if self.tracker is not None:
                await self.tracker.stop()
            if self.reconciler is not None:
                await self.reconciler.stop()
            if self.execution is not None:
                await self.execution.stop()
            if self._server is not None:
                self._server.should_exit = True
            current = asyncio.current_task()
            tasks = [task for task in self._tasks if task is not current]
            if tasks:
                try:
                    async with asyncio.timeout(10):
                        await asyncio.gather(*tasks, return_exceptions=True)
                except TimeoutError:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            if self.events is not None:
                await self.events.emit("service", "stopped", {})
                await self.events.close()
            if self.http is not None:
                await self.http.close()
            await self.database.close()
            self._stopped.set()
            log.info("syncopy_stopped")

    def _task_done(self, task: asyncio.Task[None]) -> None:
        if self._stopping.is_set() or task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self.state.health.last_error = f"{task.get_name()}: {error}"
            log.error("service_task_failed", task=task.get_name(), error=str(error))
            self.request_stop()

    async def _on_target_change(self, change: TargetPositionChange) -> None:
        assert self.ingestor is not None
        assert self.reconciler is not None
        assert self.markets is not None
        assert self.execution is not None
        assert self.events is not None
        self.ingestor.add_assets([change.token_id])
        lock = self._token_locks.setdefault(change.token_id, asyncio.Lock())
        async with lock:
            reference = next(
                (
                    positions[change.token_id]
                    for positions in self.state.target_positions.values()
                    if change.token_id in positions
                ),
                None,
            )
            await self.markets.ensure(change.token_id, position=reference)
            book = self.state.books.get(change.token_id)
            if book is None or book.freshness is Freshness.STALE:
                if not await self.reconciler.refresh_book(change.token_id):
                    return
                book = self.state.books[change.token_id]
            price = book.mid or book.best_ask or book.best_bid
            if price is None or price <= ZERO:
                return
            raw = ZERO
            target_entries: list[tuple[Decimal, Decimal, Decimal]] = []
            for address, weight in self.settings.targets.items():
                position = self.state.target_positions.get(address, {}).get(change.token_id)
                target_size = position.size if position else ZERO
                capital = self.settings.target_capital(address)
                raw += target_size * (self.settings.own_capital_usd / capital) * weight
                target_entries.append((target_size, capital, weight))
            current = self.state.own_positions.get(change.token_id)
            current_size = current.size if current else ZERO
            from syncopy.domain import Side

            side = Side.BUY if raw >= current_size else Side.SELL
            depth = book.depth_within(side, self.settings.slippage_tolerance_pct)
            sizer = PositionSizer(
                max_depth_fraction=self.settings.max_depth_fraction,
                max_market_exposure_pct=self.settings.max_market_exposure_pct,
            )
            result = sizer.size_aggregate(
                SizingInput(
                    target_size=target_size,
                    target_capital=capital,
                    own_capital=self.settings.own_capital_usd,
                    market_price=price,
                    depth_shares=depth,
                    target_weight=weight,
                )
                for target_size, capital, weight in target_entries
            )
            await self.events.emit(
                "strategy",
                "sizing_decision",
                {
                    "token_id": change.token_id,
                    "desired_size": str(result.desired_size),
                    "raw_mirror": str(result.raw_mirror),
                    "liquidity_cap": str(result.liquidity_cap),
                    "risk_cap_shares": str(result.risk_cap_shares),
                    "limiting_factor": result.limiting_factor,
                },
            )
            await self.execution.process(
                change.token_id,
                result.desired_size,
                reason=f"target change {change.address} delta={change.size_delta}",
                price_hint=change.price_hint,
            )
