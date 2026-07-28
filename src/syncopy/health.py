from __future__ import annotations

from fastapi import FastAPI, HTTPException

from syncopy.config import Settings
from syncopy.db import Database
from syncopy.state import RuntimeState


def create_app(settings: Settings, state: RuntimeState, database: Database) -> FastAPI:
    app = FastAPI(title="Syncopy Health API", version="0.1.0", docs_url="/docs")

    @app.get("/healthz")
    async def health() -> dict[str, object]:
        try:
            await database.ping()
            database_ok = True
        except Exception:
            database_ok = False
        snapshot = await state.snapshot()
        return {
            "status": "ok" if database_ok and not state.health.last_error else "degraded",
            "database": database_ok,
            "dry_run": settings.dry_run,
            "trading_halted": state.health.trading_halted,
            "halt_reason": state.health.halt_reason,
            "last_error": state.health.last_error,
            "started_at": state.health.started_at,
            "last_target_poll": state.health.last_target_poll,
            "last_reconciliation": state.health.last_reconciliation,
            "last_ws_message": state.health.last_ws_message,
            **snapshot,
        }

    @app.get("/readyz")
    async def ready() -> dict[str, str]:
        try:
            await database.ping()
        except Exception as error:
            raise HTTPException(status_code=503, detail="database unavailable") from error
        if state.books and any(book.freshness.value == "STALE" for book in state.books.values()):
            raise HTTPException(status_code=503, detail="one or more order books are stale")
        if not settings.dry_run and state.health.last_reconciliation is None:
            raise HTTPException(status_code=503, detail="initial reconciliation incomplete")
        return {"status": "ready"}

    @app.get("/state")
    async def runtime_state() -> dict[str, object]:
        return await state.snapshot()

    return app
