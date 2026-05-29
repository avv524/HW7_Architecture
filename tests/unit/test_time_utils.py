"""Unit tests for app.time_utils."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from app.time_utils import ensure_utc, parse_event_timestamp, utc_now, utc_now_iso


class TestUtcNow:
    def test_returns_timezone_aware_datetime(self) -> None:
        result = utc_now()
        assert result.tzinfo is not None
        assert result.utcoffset() == timedelta(0)

    def test_close_to_real_now(self) -> None:
        result = utc_now()
        delta = abs((datetime.now(timezone.utc) - result).total_seconds())
        assert delta < 1.0


class TestUtcNowIso:
    def test_ends_with_z(self) -> None:
        result = utc_now_iso()
        assert result.endswith("Z")
        assert "+00:00" not in result

    def test_round_trips_through_parse(self) -> None:
        iso = utc_now_iso()
        parsed = parse_event_timestamp(iso)
        assert parsed.tzinfo == timezone.utc


class TestEnsureUtc:
    def test_naive_datetime_is_assumed_utc(self) -> None:
        naive = datetime(2026, 1, 1, 12, 0, 0)
        result = ensure_utc(naive)
        assert result.tzinfo == timezone.utc
        assert result.hour == 12

    def test_aware_datetime_is_converted_to_utc(self) -> None:
        moscow = timezone(timedelta(hours=3))
        msk = datetime(2026, 1, 1, 15, 0, 0, tzinfo=moscow)
        result = ensure_utc(msk)
        assert result.tzinfo == timezone.utc
        assert result.hour == 12


class TestParseEventTimestamp:
    def test_z_suffix(self) -> None:
        result = parse_event_timestamp("2026-04-01T12:00:00Z")
        assert result == datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_offset_suffix(self) -> None:
        result = parse_event_timestamp("2026-04-01T15:00:00+03:00")
        assert result == datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_datetime_input_passes_through(self) -> None:
        dt = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert parse_event_timestamp(dt) == dt

    @pytest.mark.parametrize("bad", ["", "   ", None, 123, []])
    def test_invalid_input_raises(self, bad: object) -> None:
        with pytest.raises(ValueError):
            parse_event_timestamp(bad)
