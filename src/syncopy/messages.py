from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ApiModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")


class DataPosition(ApiModel):
    proxy_wallet: str = Field(alias="proxyWallet")
    asset: str
    condition_id: str = Field(alias="conditionId")
    size: Decimal
    avg_price: Decimal = Field(alias="avgPrice")
    current_price: Decimal | None = Field(default=None, alias="curPrice")
    current_value: Decimal | None = Field(default=None, alias="currentValue")
    title: str = ""
    slug: str = ""
    outcome: str = ""
    outcome_index: int | None = Field(default=None, alias="outcomeIndex")
    end_date: datetime | None = Field(default=None, alias="endDate")
    negative_risk: bool = Field(default=False, alias="negativeRisk")


class DataTrade(ApiModel):
    proxy_wallet: str = Field(alias="proxyWallet")
    side: str
    asset: str
    condition_id: str = Field(alias="conditionId")
    size: Decimal
    price: Decimal
    timestamp: int
    transaction_hash: str = Field(default="", alias="transactionHash")


class WsLevel(ApiModel):
    price: Decimal
    size: Decimal


class WsBook(ApiModel):
    event_type: str
    asset_id: str
    market: str
    bids: list[WsLevel]
    asks: list[WsLevel]
    timestamp: str | int
    hash: str | None = None


class WsPriceChangeItem(ApiModel):
    asset_id: str
    price: Decimal
    size: Decimal
    side: str
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None


class WsPriceChange(ApiModel):
    event_type: str
    market: str
    price_changes: list[WsPriceChangeItem]
    timestamp: str | int


class RestBook(ApiModel):
    market: str
    asset_id: str
    bids: list[WsLevel]
    asks: list[WsLevel]
    hash: str | None = None
    timestamp: str | int | None = None

    @field_validator("asset_id", mode="before")
    @classmethod
    def asset_alias_fallback(cls, value: object) -> object:
        return value
