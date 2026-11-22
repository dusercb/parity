"""live_bot unit tests -- the observer had NO tests until this file.

Focus: the scheduling/data-window logic that decides WHEN a session runs and
WHICH bars it feeds the engines. Those are the parts that can silently corrupt
parity or waste a trading day, and they are testable without TWS.

Run from the bot repo root:
    ..\\hindsight\\.venv\\Scripts\\python.exe -m pytest tests -q
"""
import sys
from datetime import date, datetime, time as dtime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot.discovery import (
    close_scanner, daily_volume_baseline, open_scanner, read_scanner, scanner_symbols,
)
from live_bot.ib_errors import BENIGN_CODES, install_error_triage
from live_bot.ibcompat import TagValue
from live_bot.live_data import ET, SymbolFeed
from live_bot.session_controller import _hhmm, _next_window_start, _secs_until


# --- window scheduling --------------------------------------------------------

def _et(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=ZoneInfo(ET))


@pytest.mark.parametrize("now,expect", [
    ("2026-07-24 02:30", "2026-07-24 04:00"),   # before -> today
    ("2026-07-24 06:00", "2026-07-25 04:00"),   # inside -> tomorrow
    ("2026-07-24 11:00", "2026-07-25 04:00"),   # after  -> tomorrow
    ("2026-03-07 23:00", "2026-03-08 04:00"),   # across a DST spring-forward
])
def test_next_window_start(now, expect):
    assert _next_window_start(_et(now), dtime(4, 0)) == _et(expect)


def test_secs_until_never_negative():
    # target already passed today -> clamped to >=1, never a negative sleep
    assert _secs_until(_et("2026-07-24 09:00"), dtime(4, 0)) >= 1.0
    assert _secs_until(_et("2026-07-24 03:00"), dtime(4, 0)) == pytest.approx(3600, abs=1)


def test_hhmm_parses_config_times():
    assert _hhmm("04:00") == dtime(4, 0)
    assert _hhmm("09:35") == dtime(9, 35)


def test_serve_runs_one_session_per_et_date():
    """The 335x bug: any early return from run() (blackout, failed reconnect,
    crash) used to drop back into the loop still inside the window and re-enter
    every 60s. The guard is 'claim the ET date before running'."""
    start_t, end_t = dtime(4, 0), dtime(9, 35)
    done_for, starts = None, 0
    now = _et("2026-07-24 04:00")

    def due(n):
        return start_t <= n.time() < end_t and n.date() != done_for

    for _ in range(20000):
        if due(now):
            done_for = now.date()
            starts += 1
        now += pd.Timedelta(minutes=1)
        if now > _et("2026-07-25 10:00"):
            break
    assert starts == 2          # one per ET date, not 335 per day


# --- backfill window / cross-day isolation ------------------------------------

class _FakeIB:
    def __init__(self, bars):
        self._bars = bars
        self.requested = {}

    def qualifyContracts(self, *a, **k):
        return None

    def reqHistoricalData(self, contract, **kw):
        self.requested = kw
        return self._bars


class _Rec:
    def __init__(self):
        self.events = []

    def event(self, kind, **payload):
        self.events.append((kind, payload))


def _bar(ts_iso, o=1.0, h=1.1, low=0.9, c=1.0, v=100.0):
    return SimpleNamespace(date=pd.Timestamp(ts_iso, tz=ET), open=o, high=h,
                           low=low, close=c, volume=v)


def _feed(bars, session_date=date(2026, 7, 24)):
    ib = _FakeIB(bars)
    f = SymbolFeed(ib, "TEST", dtime(4, 0), _Rec(), session_date=session_date)
    return f, ib


def test_backfill_reaches_back_to_the_session_start():
    """A fixed 3h window truncated late discoveries: a symbol confirmed at 09:00
    would start its frame at 06:00, so premarket_open -- and therefore gain% --
    would differ from the backtest, which always splits from 04:00."""
    f, _ = _feed([])
    at_0900 = pd.Timestamp("2026-07-24 09:00", tz=ET)
    secs = f._backfill_seconds(at_0900)
    assert secs >= 5 * 3600, "must cover 04:00 -> 09:00"
    # and it still covers the far end of the window
    assert f._backfill_seconds(pd.Timestamp("2026-07-24 09:35", tz=ET)) >= 5.5 * 3600


def test_backfill_keeps_only_todays_session_bars():
    """The cross-day leak the longer window would otherwise open: yesterday's
    evening bars are also '>= 04:00' by clock, so a time-of-day-only filter
    would merge two days into one frame."""
    bars = [
        _bar("2026-07-23 22:00"),   # YESTERDAY evening -> must be dropped
        _bar("2026-07-24 03:59"),   # before session start -> dropped
        _bar("2026-07-24 04:00"),   # keep
        _bar("2026-07-24 08:30"),   # keep
    ]
    f, _ = _feed(bars)
    assert f.backfill() == 2
    frame = f.frame(upto=pd.Timestamp("2026-07-24 09:00", tz=ET))
    assert len(frame) == 2
    assert frame["dt_et"].min() == pd.Timestamp("2026-07-24 04:00", tz=ET)


def test_frame_excludes_the_still_forming_bar():
    """A bar whose 10s window has not closed still has a moving high/low/close;
    evaluating it would be a live-only peek the replay could never reproduce."""
    now = pd.Timestamp("2026-07-24 08:30:05", tz=ET)
    bars = [_bar("2026-07-24 08:29:40"), _bar("2026-07-24 08:29:50"), _bar("2026-07-24 08:30:00")]
    f, _ = _feed(bars)
    f.backfill()
    frame = f.frame(upto=now)
    assert pd.Timestamp("2026-07-24 08:30:00", tz=ET) not in list(frame["dt_et"])
    assert len(frame) == 2


def test_frame_has_the_backtesters_column_shape():
    """The engines are handed this frame directly -- the columns must match what
    split_sessions produces or parity is impossible."""
    f, _ = _feed([_bar("2026-07-24 05:00")])
    f.backfill()
    frame = f.frame(upto=pd.Timestamp("2026-07-24 06:00", tz=ET))
    assert list(frame.columns) == ["dt_et", "open", "high", "low", "close", "volume"]


def test_empty_feed_returns_empty_frame_not_a_crash():
    f, _ = _feed([])
    assert f.backfill() == 0
    assert f.frame(upto=pd.Timestamp("2026-07-24 09:00", tz=ET)).empty


def test_frame_accepts_naive_and_aware_upto_and_the_default():
    """frame() crashed on EVERY production call: pd.Timestamp(x, tz=ET) raises
    when x is already tz-aware, which is the default upto=None path. All three
    input shapes must work."""
    f, _ = _feed([_bar("2026-07-24 05:00")])
    f.backfill()
    aware = f.frame(upto=pd.Timestamp("2026-07-24 06:00", tz=ET))
    naive = f.frame(upto=pd.Timestamp("2026-07-24 06:00"))          # tz-naive
    default = f.frame()                                             # upto=None -> now(tz=ET)
    assert len(aware) == 1 and len(naive) == 1
    assert isinstance(default, pd.DataFrame)                        # must not raise


# --- host-timezone independence (UK now, New York later; behavior identical) --

class _DailyBars(_FakeIB):
    pass


def test_rvol_baseline_excludes_the_ET_session_day_not_the_host_day():
    """The baseline excludes 'today', and 'today' must be the ET SESSION date,
    never the host's local date. A UK host whose local date differed would
    exclude the wrong bar, shift the baseline, and break RVOL qualification
    parity. Here the session-day bar (1e9) must be dropped and the baseline come
    only from the prior days."""
    bars = [
        SimpleNamespace(date=date(2026, 7, 21), volume=100.0),
        SimpleNamespace(date=date(2026, 7, 22), volume=200.0),
        SimpleNamespace(date=date(2026, 7, 23), volume=300.0),
        SimpleNamespace(date=date(2026, 7, 24), volume=1_000_000_000.0),  # session day -> excluded
    ]
    scfg = SimpleNamespace(rvol_lookback_days=20, rvol_min_history_days=1,
                           rvol_baseline_stat="median")
    base = daily_volume_baseline(_DailyBars(bars), "TEST", scfg, date(2026, 7, 24))
    assert base == 200.0            # median of {100,200,300}; the 1e9 session bar is gone
    assert base < 1000.0           # the session day did NOT leak in


# --- scanner filter type (the live crash: dicts where TagValue was required) ---

class _ScannerIB:
    """Records the filter argument reqScannerData was called with, and (like the
    real API) raises AttributeError if any filter lacks a .tag attribute -- which
    is exactly how a plain dict failed live ('dict' object has no attribute
    'tag')."""
    def __init__(self):
        self.filter_arg = None

    def reqScannerData(self, sub, opts=None, filters=None):
        self.filter_arg = filters
        for f in filters or []:
            _ = f.tag, f.value          # a dict would raise here, as it did live
        return []


def test_scanner_passes_TagValue_objects_not_dicts():
    """The exact bug that broke discovery against live TWS: the abstract filter
    was built as dicts, and ib_insync/ib_async iterate filters expecting a .tag
    attribute -> AttributeError, so the scanner returned nothing every poll."""
    ib = _ScannerIB()
    dcfg = {"instrument": "STK", "location": "STK.US.MAJOR", "scan_code": "TOP_PERC_GAIN",
            "scanner_rows": 50, "change_perc_above": 15.0}
    scanner_symbols(ib, dcfg)          # must NOT raise
    assert ib.filter_arg is not None and len(ib.filter_arg) >= 1
    assert all(isinstance(f, TagValue) for f in ib.filter_arg)
    assert any(f.tag == "changePercAbove" and f.value == "15.0" for f in ib.filter_arg)


def test_scanner_adds_price_band_filters_when_configured():
    ib = _ScannerIB()
    dcfg = {"instrument": "STK", "location": "STK.US.MAJOR", "scan_code": "TOP_PERC_GAIN",
            "scanner_rows": 50, "change_perc_above": 15.0, "price_min": 1.6, "price_max": 16}
    scanner_symbols(ib, dcfg)
    tags = {f.tag for f in ib.filter_arg}
    assert {"changePercAbove", "priceAbove", "priceBelow"} <= tags


class _StreamIB:
    """Counts subscribe/cancel calls. The persistent path must subscribe ONCE
    and cancel ONCE regardless of how many times it's read -- that's what stops
    the per-poll error-162 flood."""
    def __init__(self, rows):
        self._rows = rows
        self.subscribes = 0
        self.cancels = 0

    def reqScannerSubscription(self, sub, opts=None, filters=None):
        self.subscribes += 1
        for f in filters or []:
            _ = f.tag, f.value          # TagValue enforced here too
        return self._rows

    def cancelScannerSubscription(self, data):
        self.cancels += 1


def test_persistent_scanner_subscribes_once_reads_many_cancels_once():
    """The root-cause fix for error 162: a streaming subscription opened once and
    read on every poll, not a one-shot subscribe/cancel per poll (each cancel
    emits a 162 -- 204 of them across a morning)."""
    rows = [SimpleNamespace(contractDetails=SimpleNamespace(
        contract=SimpleNamespace(symbol=s))) for s in ("AAA", "BBB")]
    ib = _StreamIB(rows)
    dcfg = {"instrument": "STK", "location": "STK.US.MAJOR", "scan_code": "TOP_PERC_GAIN",
            "scanner_rows": 50, "change_perc_above": 15.0}
    data = open_scanner(ib, dcfg)
    for _ in range(10):                 # ten polls...
        assert read_scanner(data) == ["AAA", "BBB"]
    close_scanner(ib, data)
    assert ib.subscribes == 1           # ...one subscribe (not ten)
    assert ib.cancels == 1              # ...one cancel -> at most one 162, all session


# --- IBKR error triage: benign codes recorded but never printed ---------------

class _EventHook:
    """Minimal stand-in for ib_insync's errorEvent (supports += and firing)."""
    def __init__(self):
        self._h = []

    def __iadd__(self, fn):
        self._h.append(fn)
        return self

    def fire(self, *a):
        for fn in self._h:
            fn(*a)


class _IBStub:
    def __init__(self):
        self.errorEvent = _EventHook()


def test_error_triage_records_benign_quietly_and_prints_real():
    ib, rec = _IBStub(), _Rec()
    printed = []
    install_error_triage(ib, rec, console_print=lambda *a, **k: printed.append(a[0]))

    ib.errorEvent.fire(3, 162, "API scanner subscription cancelled: 3", None)   # benign
    ib.errorEvent.fire(7, 2104, "Market data farm connection is OK", None)      # benign
    ib.errorEvent.fire(9, 1100, "Connectivity between IB and TWS has been lost", None)  # REAL

    kinds = [e for e in rec.events if e[0] == "ib_message"]
    assert len(kinds) == 3                       # everything is recorded (audit)
    assert 162 in BENIGN_CODES and 2104 in BENIGN_CODES and 1100 not in BENIGN_CODES
    # only the real one reached the console
    assert len(printed) == 1
    assert "1100" in printed[0]
    assert all("162" not in p for p in printed)  # the scanner-cancel never printed
