from __future__ import annotations

import asyncio

import aiohttp
import structlog
import uvicorn

from syncopy.clients import ClobApi, DataApi, GammaApi
from syncopy.config import Settings
from syncopy.db import Database, Repository
from syncopy.domain import SignalEvent, TargetPositionChange
from syncopy.events import EventWriter
from syncopy.execution import ExecutionEngine, PolymarketExecutor
from syncopy.health import create_app
from syncopy.ingest import MarketIngestor
from syncopy.markets import MarketRegistry
from syncopy.reconcile import Reconciler
from syncopy.risk import RiskManager
from syncopy.signal import CopyTradingSignalSource
from syncopy.state import RuntimeState
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
        self.copy_trading_source = CopyTradingSignalSource(
            state=self.state,
            settings=self.settings,
            token_locks=self._token_locks,
            get_ingestor=lambda: self.ingestor,
            get_markets=lambda: self.markets,
            get_reconciler=lambda: self.reconciler,
            get_events=lambda: self.events,
            on_signal=self._on_signal_event,
        )
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
        await self.copy_trading_source.on_target_change(change)

    async def _on_signal_event(self, event: SignalEvent) -> None:
        assert self.execution is not None
        await self.execution.process(
            event.token_id,
            event.desired_size,
            reason=event.reason,
            price_hint=event.price_hint,
        )
