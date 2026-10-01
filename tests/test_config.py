from decimal import Decimal

import pytest
from pydantic import ValidationError

from syncopy.config import Settings

ADDRESS = "0x1111111111111111111111111111111111111111"


def test_parses_target_mappings_and_csv() -> None:
    settings = Settings(
        _env_file=None,
        targets=f"{ADDRESS}:0.5",
        target_capitals=f"{ADDRESS}:25000",
        market_token_ids="1,2",
    )
    assert settings.targets[ADDRESS] == Decimal("0.5")
    assert settings.target_capital(ADDRESS) == Decimal("25000")
    assert settings.market_token_ids == ["1", "2"]


def test_live_mode_requires_credentials() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, dry_run=False)


def test_target_capital_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, target_capitals=f"{ADDRESS}:0")


def test_copy_trader_rejects_malformed_address() -> None:
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            source_type="copy_trader",
            targets="not-an-address:0.5",
        )


def test_dislocation_accepts_empty_targets() -> None:
    settings = Settings(_env_file=None, source_type="dislocation", targets={})
    assert settings.source_type == "dislocation"
    assert settings.targets == {}
