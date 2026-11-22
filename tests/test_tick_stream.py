"""Tick-by-tick path: clock-aligned candle construction, first-touch cross
resolution, and that a slotted position exits TICK-precise (on the crossing
print) rather than waiting for a 5s bar -- the whole point of the tiered zoom-in.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot.slot_manager import SlotManager
from live_bot.tick_stream import TickStream, bar_start

ET = "America/New_York"


def _t(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz=ET)


# --- clock alignment: candles on the market grid, not on arrival --------------

def test_bars_align_to_the_wall_clock_grid():
    ts = TickStream(None, None, "T", bar_seconds=10)
    # ticks starting mid-bar at :04:53
    for dt, px in [("2026-07-27 08:04:53", 3.00), ("2026-07-27 08:04:56", 3.05),
                   ("2026-07-27 08:04:59", 2.98),      # all in the :04:50 bar
                   ("2026-07-27 08:05:01", 3.10)]:     # rolls into :05:00 (forming, excluded)
        ts.on_tick(_t(dt), px, 100)
    f = ts.frame()
    assert len(f) == 1                                  # only the completed :50 bar
    row = f.iloc[0]
    assert str(row["dt_et"].time()) == "08:04:50"       # aligned to :50, not :53
    assert row["open"] == 3.00 and row["high"] == 3.05 and row["low"] == 2.98 and row["close"] == 2.98


def test_bar_start_snaps_10s_and_1m():
    assert bar_start(_t("2026-07-27 08:04:57"), 10) == _t("2026-07-27 08:04:50")
    assert bar_start(_t("2026-07-27 08:04:57"), 60) == _t("2026-07-27 08:04:00")


def test_forming_bar_is_excluded():
    ts = TickStream(None, None, "T", 10)
    ts.on_tick(_t("2026-07-27 08:04:51"), 3.0, 10)      # only a forming bar exists
    assert ts.frame().empty


# --- first-touch: the live mirror of the backtest's tick resolution -----------

def test_first_touch_long_stop_before_target_in_tick_order():
    ts = TickStream(None, None, "T", 10)
    for dt, px in [("2026-07-27 08:10:00", 3.00), ("2026-07-27 08:10:01", 2.95),
                   ("2026-07-27 08:10:02", 2.89),       # <- stop 2.90 crossed here
                   ("2026-07-27 08:10:03", 3.20)]:      # target 3.10 later, must not win
        ts.on_tick(_t(dt), px, 10)
    kind, t_time, price, cur = ts.first_touch(0, "long", stop=2.90, target=3.10)
    assert kind == "stop" and price == 2.89


def test_first_touch_resumes_from_cursor():
    ts = TickStream(None, None, "T", 10)
    ts.on_tick(_t("2026-07-27 08:10:00"), 3.00, 10)
    kind, _, _, cur = ts.first_touch(0, "long", 2.90, 3.10)
    assert kind is None                                  # nothing crossed yet
    ts.on_tick(_t("2026-07-27 08:10:01"), 3.15, 10)      # target crosses
    kind, _, price, _ = ts.first_touch(cur, "long", 2.90, 3.10)
    assert kind == "target" and price == 3.15


def test_first_touch_short_is_mirrored():
    ts = TickStream(None, None, "T", 10)
    ts.on_tick(_t("2026-07-27 08:10:00"), 3.50, 10)
    ts.on_tick(_t("2026-07-27 08:10:01"), 3.62, 10)      # short stop 3.60 (above) crossed
    kind, _, price, _ = ts.first_touch(0, "short", stop=3.60, target=3.20)
    assert kind == "stop" and price == 3.62


# --- slot manager: <=cap, priority order, promote/release ---------------------

class _Rec:
    def __init__(self): self.events = []
    def event(self, k, **p): self.events.append((k, p))


class _FakeTicker:
    def __init__(self): self.tickByTicks = []; self.updateEvent = _Evt()


class _Evt:
    def __iadd__(self, fn): return self


class _FakeIB:
    def __init__(self): self.subs = []
    def reqTickByTickData(self, c, t, n, i): self.subs.append(c); return _FakeTicker()
    def cancelTickByTickData(self, tk): pass


class _FakeBroker:
    def contract(self, sym): return SimpleNamespace(symbol=sym.upper())


def test_slot_manager_caps_and_prioritises():
    sm = SlotManager(_FakeIB(), _FakeBroker(), _Rec(), max_slots=3)
    sm.sync(["AAA", "BBB", "CCC", "DDD"])                # 4 want, 3 slots
    assert set(sm.streams) == {"AAA", "BBB", "CCC"}      # first three (priority order)
    assert sm.get("DDD") is None


def test_slot_manager_releases_when_priority_drops():
    sm = SlotManager(_FakeIB(), _FakeBroker(), _Rec(), max_slots=3)
    sm.sync(["AAA", "BBB"])
    sm.sync(["BBB"])                                     # AAA closed -> slot freed
    assert set(sm.streams) == {"BBB"}
    sm.sync(["BBB", "CCC", "DDD"])                       # now room for two more
    assert set(sm.streams) == {"BBB", "CCC", "DDD"}
