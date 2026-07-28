from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp

from syncopy.backtest import ReplayEngine, load_jsonl
from syncopy.clients import ClobApi, DataApi, GammaApi
from syncopy.config import Settings
from syncopy.db import Database, Repository
from syncopy.logging import configure_logging
from syncopy.service import SyncopyService
from syncopy.tls import default_ssl_context


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="syncopy", description="Polymarket copy trader")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("run", help="Run the trading service")
    subcommands.add_parser("init-db", help="Create the database schema")
    discover = subcommands.add_parser("discover", help="List active liquid market tokens")
    discover.add_argument("--limit", type=int, default=20)
    backtest = subcommands.add_parser("backtest", help="Replay events from JSONL or PostgreSQL")
    backtest.add_argument("--jsonl", type=Path)
    backtest.add_argument("--after", help="ISO-8601 lower time bound for database replay")
    backtest.add_argument("--limit", type=int)
    backtest.add_argument("--fee-bps", type=Decimal, default=Decimal("0"))
    backtest.add_argument("--output", type=Path)
    subcommands.add_parser("doctor", help="Check config, database, and public APIs")
    return parser


async def run_service(settings: Settings) -> None:
    service = SyncopyService(settings)
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(name, service.request_stop)
    try:
        await service.run()
    finally:
        await service.stop()


async def init_db(settings: Settings) -> None:
    database = Database(settings.database_url)
    try:
        await database.create_schema()
    finally:
        await database.close()
    print("Syncopy database schema is ready.")


async def discover_markets(settings: Settings, limit: int) -> None:
    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_s)
    connector = aiohttp.TCPConnector(ssl=default_ssl_context())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        gamma = GammaApi(settings.gamma_api_url, session)
        markets = await gamma.discover(limit=limit)
    for market in markets[: limit * 2]:
        print(
            json.dumps(
                {
                    "token_id": market.token_id,
                    "condition_id": market.condition_id,
                    "question": market.question,
                    "tick_size": str(market.tick_size),
                    "min_size": str(market.min_size),
                    "end_date": market.end_date.isoformat() if market.end_date else None,
                }
            )
        )


def make_replay(settings: Settings, fee_bps: Decimal) -> ReplayEngine:
    return ReplayEngine(
        own_capital=settings.own_capital_usd,
        target_capitals=settings.target_capitals,
        target_weights=settings.targets,
        default_target_capital=settings.default_target_capital_usd,
        max_depth_fraction=settings.max_depth_fraction,
        max_market_exposure_pct=settings.max_market_exposure_pct,
        slippage_tolerance_pct=settings.slippage_tolerance_pct,
        fee_bps=fee_bps,
    )


async def backtest_events(
    settings: Settings,
    *,
    jsonl: Path | None,
    after: str | None,
    limit: int | None,
    fee_bps: Decimal,
    output: Path | None,
) -> None:
    if jsonl is not None:
        records = load_jsonl(jsonl)
    else:
        database = Database(settings.database_url)
        try:
            repository = Repository(database)
            parsed_after = datetime.fromisoformat(after) if after else None
            rows = await repository.iter_events(after=parsed_after, limit=limit)
            records = [
                {
                    "ts": row.ts.isoformat(),
                    "source": row.source,
                    "type": row.type,
                    "payload": row.payload,
                }
                for row in rows
            ]
        finally:
            await database.close()
    report = make_replay(settings, fee_bps).replay(records).to_json()
    rendered = json.dumps(report, indent=2)
    if output is not None:
        await asyncio.to_thread(_write_report, output, rendered)
    print(rendered)


def _write_report(output: Path, rendered: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered + "\n", encoding="utf-8")


async def doctor(settings: Settings) -> None:
    checks: dict[str, Any] = {
        "mode": "paper" if settings.dry_run else "live",
        "targets": len(settings.targets),
    }
    database = Database(settings.database_url)
    try:
        await database.ping()
        checks["database"] = "ok"
    except Exception as error:
        checks["database"] = f"error: {error}"
    finally:
        await database.close()
    timeout = aiohttp.ClientTimeout(total=settings.http_timeout_s)
    connector = aiohttp.TCPConnector(ssl=default_ssl_context())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        clob = ClobApi(settings.clob_api_url, session)
        gamma = GammaApi(settings.gamma_api_url, session)
        data = DataApi(settings.data_api_url, session)
        try:
            await clob.get_json("/ok")
            checks["clob"] = "ok"
        except Exception as error:
            checks["clob"] = f"error: {error}"
        try:
            markets = await gamma.discover(limit=1)
            checks["gamma"] = "ok"
            if markets:
                await clob.book(markets[0].token_id)
                checks["clob_book"] = "ok"
        except Exception as error:
            checks["gamma"] = f"error: {error}"
        try:
            await data.trades(None, limit=1)
            checks["data"] = "ok"
        except Exception as error:
            checks["data"] = f"error: {error}"
    print(json.dumps(checks, indent=2))


async def dispatch(args: argparse.Namespace, settings: Settings) -> None:
    if args.command == "run":
        await run_service(settings)
    elif args.command == "init-db":
        await init_db(settings)
    elif args.command == "discover":
        await discover_markets(settings, args.limit)
    elif args.command == "backtest":
        await backtest_events(
            settings,
            jsonl=args.jsonl,
            after=args.after,
            limit=args.limit,
            fee_bps=args.fee_bps,
            output=args.output,
        )
    elif args.command == "doctor":
        await doctor(settings)


def main() -> None:
    args = build_parser().parse_args()
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(dispatch(args, settings))


if __name__ == "__main__":
    main()
