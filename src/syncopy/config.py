from __future__ import annotations

import re
from decimal import Decimal

from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _csv(value: str | list[str]) -> list[str]:
    if isinstance(value, list):
        return value
    return [part.strip() for part in value.split(",") if part.strip()]


def _mapping(value: str | dict[str, Decimal]) -> dict[str, Decimal]:
    if isinstance(value, dict):
        return {key.lower(): Decimal(str(val)) for key, val in value.items()}
    result: dict[str, Decimal] = {}
    for item in _csv(value):
        key, separator, raw = item.partition(":")
        if not separator:
            raise ValueError(f"Expected key:value entry, got {item!r}")
        result[key.lower()] = Decimal(raw)
    return result


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        populate_by_name=True,
        extra="ignore",
    )

    dry_run: bool = True
    kill_switch: bool = False
    database_url: str = "postgresql+asyncpg://syncopy:syncopy@localhost:5432/syncopy"
    auto_create_schema: bool = True
    clob_api_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    data_api_url: str = "https://data-api.polymarket.com"
    market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    targets: dict[str, Decimal] = Field(default_factory=dict)
    target_capitals: dict[str, Decimal] = Field(default_factory=dict)
    market_token_ids: list[str] = Field(default_factory=list)

    poll_interval_s: float = Field(default=4, ge=1)
    reconcile_interval_s: float = Field(default=20, ge=2)
    ws_batch_size: int = Field(default=50, ge=1, le=500)
    http_timeout_s: float = Field(default=10, gt=0)

    own_capital_usd: Decimal = Field(default=Decimal("1000"), gt=0)
    default_target_capital_usd: Decimal = Field(default=Decimal("10000"), gt=0)
    max_market_exposure_pct: Decimal = Field(default=Decimal("5"), gt=0, le=100)
    max_total_exposure_pct: Decimal = Field(default=Decimal("50"), gt=0, le=100)
    max_order_size_usd: Decimal = Field(default=Decimal("100"), gt=0)
    max_depth_fraction: Decimal = Field(default=Decimal("0.10"), gt=0, le=1)
    slippage_tolerance_pct: Decimal = Field(default=Decimal("2"), ge=0, le=100)
    price_buffer_ticks: int = Field(default=1, ge=0, le=100)
    max_target_price_move_pct: Decimal = Field(default=Decimal("3"), ge=0)
    order_ttl_s: float = Field(default=15, gt=0)
    pnl_snapshot_interval_s: float = Field(default=10, ge=1)
    daily_loss_halt_usd: Decimal = Field(default=Decimal("50"), gt=0)
    min_days_to_resolution: Decimal = Field(default=Decimal("1"), ge=0)
    blocked_token_ids: list[str] = Field(default_factory=list)

    polymarket_private_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("POLYMARKET_PRIVATE_KEY", "POLYGON_PRIVATE_KEY"),
    )
    polymarket_deposit_wallet: str | None = None
    health_host: str = "0.0.0.0"
    health_port: int = Field(default=8080, ge=1, le=65535)
    log_level: str = "INFO"

    @field_validator("targets", "target_capitals", mode="before")
    @classmethod
    def parse_mapping(cls, value: object) -> object:
        return _mapping(value) if isinstance(value, (str, dict)) else value

    @field_validator("market_token_ids", "blocked_token_ids", mode="before")
    @classmethod
    def parse_csv(cls, value: object) -> object:
        return _csv(value) if isinstance(value, (str, list)) else value

    @field_validator("targets")
    @classmethod
    def validate_targets(cls, value: dict[str, Decimal]) -> dict[str, Decimal]:
        for address, weight in value.items():
            if not ADDRESS_RE.match(address):
                raise ValueError(f"Invalid target address: {address}")
            if weight <= 0:
                raise ValueError(f"Target weight must be positive: {address}")
        return value

    @field_validator("target_capitals")
    @classmethod
    def validate_target_capitals(cls, value: dict[str, Decimal]) -> dict[str, Decimal]:
        for address, capital in value.items():
            if not ADDRESS_RE.match(address):
                raise ValueError(f"Invalid target capital address: {address}")
            if capital <= 0:
                raise ValueError(f"Target capital must be positive: {address}")
        return value

    @model_validator(mode="after")
    def validate_live_credentials(self) -> Settings:
        if not self.dry_run:
            if (
                self.polymarket_private_key is None
                or not self.polymarket_private_key.get_secret_value()
                or not self.polymarket_deposit_wallet
            ):
                raise ValueError(
                    "Live mode requires POLYMARKET_PRIVATE_KEY and POLYMARKET_DEPOSIT_WALLET"
                )
            if not ADDRESS_RE.match(self.polymarket_deposit_wallet):
                raise ValueError("POLYMARKET_DEPOSIT_WALLET is not a valid EVM address")
        return self

    def target_capital(self, address: str) -> Decimal:
        return self.target_capitals.get(address.lower(), self.default_target_capital_usd)
