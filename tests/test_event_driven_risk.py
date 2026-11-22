"""The risk path is EVENT-DRIVEN: a stop resolves on the crossing print, not on
the next pass of the ~2s session loop.

Ingestion was always real-time -- ib.sleep() runs the asyncio loop, so ticks land
continuously -- but nothing LOOKED at them until manage() ran. Measured decision
latency was 2.0-4.4s. Against the wedge's median 9c stop (a quarter of them under
5c, 68% of executed trades) price moves roughly a third of a stop width inside a
single 10s bar, so that delay is a first-order cost in realised R.

Firing per tick makes exit idempotency safety-critical: the callback, the loop's
tick backstop and the bar path can all reach the same breach, and a breached bar
stays breached for its whole life. Every surplus exit would be a real order, and
the excess fills would open a position in the opposite direction.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from test_order_layer import _FakeBroker, _om, _trade      # shared harness
from live_bot.tick_stream import TickStream

ET = "America/New_York"


class _Slots:
    def __init__(self, streams=None):
        self.streams = streams or {}
        self.on_ticks = None

    def get(self, sym):
        return self.streams.get(sym.upper())


class _Stream:
    """Resolves whatever `touch` says, once; then nothing (cursor has advanced)."""
    def __init__(self, touch=None):
        self._touch, self._done = touch, False

    def first_touch(self, since, direction, stop, target):
        if self._touch is None or self._done:
            return None, None, None, since
        self._done = True
        return self._touch[0], None, self._touch[1], since + 1


def _pos_with_stream(tmp_path, touch=None, direction="long"):
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade(ticker="ZENA", mode="wedge_break", direction=direction,
                    entry=1.60, stop=1.58, target=1.63, trigger=1.60))
    om.poll_fills()
    pos = book.positions["ZENA::wedge_break"]
    stream = _Stream(touch)
    om.slots = _Slots({"ZENA": stream})
    return om, broker, book, pos, stream


def _bar(low, high):
    return pd.DataFrame([{"dt_et": pd.Timestamp("2026-08-07 07:20:00", tz=ET),
                          "open": 1.60, "high": high, "low": low, "close": 1.60,
                          "volume": 100.0}])


# --- the callback itself exits ------------------------------------------------

def test_a_tick_crossing_the_stop_exits_from_the_callback(tmp_path):
    om, broker, book, pos, _ = _pos_with_stream(tmp_path, touch=("stop", 1.575))
    om.on_ticks("ZENA")                     # no loop involvement at all
    ex = [p for k, p in om.rec.events if k == "exit_submitted"]
    assert len(ex) == 1
    assert ex[0]["source"] == "tick" and ex[0]["tick_price"] == 1.575


def test_a_tick_crossing_the_target_decides_from_the_callback(tmp_path):
    """The callback still gets there first -- that is the latency win, and it is
    what this test has always been for. What it must NOT do is book the exit: the
    target is a resting limit at the exchange, and only the broker can say it
    filled. This asserted `position_closed` until 2026-08-14, when WETO showed
    what booking a print instead costs (see tests/test_unconfirmed_exits.py)."""
    om, broker, book, pos, _ = _pos_with_stream(tmp_path, touch=("target", 1.63))
    om.on_ticks("ZENA")
    touched = [p for k, p in om.rec.events if k == "target_touched"]
    assert touched and touched[0]["touch"] == 1.63, "the callback must decide at tick latency"
    assert not any(k == "position_closed" for k, _ in om.rec.events)
    assert pos.key() in book.positions, "still ours, and still stop-managed, until the fill"


def test_callback_ignores_symbols_it_holds_no_position_in(tmp_path):
    om, broker, book, pos, _ = _pos_with_stream(tmp_path, touch=("stop", 1.575))
    om.on_ticks("SOMETHINGELSE")
    assert not any(k == "exit_submitted" for k, _ in om.rec.events)


# --- ONE exit per position, whichever path gets there first -------------------

def test_callback_then_loop_cannot_double_exit(tmp_path):
    om, broker, book, pos, stream = _pos_with_stream(tmp_path, touch=("stop", 1.575))
    om.on_ticks("ZENA")                                  # callback fires the exit
    om.manage({"ZENA": SimpleNamespace(frame=lambda: _bar(1.50, 1.61))})   # loop follows
    assert len([k for k, _ in om.rec.events if k == "exit_submitted"]) == 1


def test_a_breached_bar_cannot_re_fire_every_loop(tmp_path):
    """The bar stays breached for its whole life. Before the guard, only the tick
    cursor's advance stopped a repeat -- and the bar path had no cursor at all."""
    om, broker, book, pos, _ = _pos_with_stream(tmp_path)      # stream resolves nothing
    feeds = {"ZENA": SimpleNamespace(frame=lambda: _bar(1.50, 1.61))}
    for _ in range(5):
        om.manage(feeds)
    assert len([k for k, _ in om.rec.events if k == "exit_submitted"]) == 1
    assert len([s for s in broker.submitted if s[0] == "marketable"]) == 2   # entry + ONE exit


def test_repeated_tick_events_cannot_double_exit(tmp_path):
    om, broker, book, pos, _ = _pos_with_stream(tmp_path, touch=("stop", 1.575))
    for _ in range(5):
        om.on_ticks("ZENA")
    assert len([k for k, _ in om.rec.events if k == "exit_submitted"]) == 1


# --- the callback must never damage the feed ---------------------------------

def test_callback_never_raises_into_the_dispatcher(tmp_path):
    """An exception escaping here would propagate into ib's event dispatcher and
    could break the feed every other position depends on."""
    om, broker, book, pos, _ = _pos_with_stream(tmp_path)
    om.slots = SimpleNamespace(get=lambda s: (_ for _ in ()).throw(RuntimeError("boom")))
    om.on_ticks("ZENA")                                   # must not raise
    assert any(k == "tick_event_error" for k, _ in om.rec.events)


def test_callback_is_re_entrancy_guarded(tmp_path):
    om, broker, book, pos, _ = _pos_with_stream(tmp_path, touch=("stop", 1.575))
    depth = {"max": 0, "cur": 0}
    real = om._resolve_ticks

    def nested(p, s):
        depth["cur"] += 1
        depth["max"] = max(depth["max"], depth["cur"])
        om.on_ticks("ZENA")            # placing an order re-enters the event stream
        depth["cur"] -= 1
        return real(p, s)

    om._resolve_ticks = nested
    om.on_ticks("ZENA")
    assert depth["max"] == 1


# --- TickStream wiring --------------------------------------------------------

class _T:
    def __init__(self, ticks):
        self.tickByTicks = list(ticks)


def _tick(ts, price, size=100):
    return SimpleNamespace(time=pd.Timestamp(ts, tz=ET), price=price, size=size)


def test_stream_notifies_the_listener_only_when_prints_arrive():
    seen = []
    ts = TickStream(ib=None, contract=None, symbol="ZENA", on_ticks=seen.append)
    ts._on_update(_T([_tick("2026-08-07 07:20:00", 1.59)]))
    assert seen == ["ZENA"]
    ts._on_update(_T([]))                       # an update carrying no prints
    assert seen == ["ZENA"], "no prints -> nothing for the risk layer to act on"


def test_listener_sees_the_buffer_already_updated():
    """The notification must come AFTER the tick is buffered, or first_touch would
    scan a buffer that doesn't yet contain the crossing print."""
    seen = {}
    ts = TickStream(ib=None, contract=None, symbol="ZENA",
                    on_ticks=lambda s: seen.setdefault("n", ts.cursor))
    ts._on_update(_T([_tick("2026-08-07 07:20:00", 1.57)]))
    assert seen["n"] == 1


def test_a_failing_listener_cannot_escape_into_the_feed():
    def boom(_sym):
        raise RuntimeError("listener blew up")
    ts = TickStream(ib=None, contract=None, symbol="ZENA", on_ticks=boom)
    ts._on_update(_T([_tick("2026-08-07 07:20:00", 1.59)]))   # must not raise
    assert ts.cursor == 1                                     # tick still buffered


def test_cancel_uses_the_contract_and_ticktype_not_the_ticker():
    """Passing the Ticker raised TypeError into a bare except, so no subscription
    was ever released and IBKR's allowance leaked until every request hit 10190."""
    calls = []

    class _IB:
        def reqTickByTickData(self, *a, **k):
            return SimpleNamespace(updateEvent=_Ev())

        def cancelTickByTickData(self, contract, tickType):
            calls.append((contract, tickType))

    class _Ev:
        def __iadd__(self, f):
            return self

    ts = TickStream(_IB(), contract="CONTRACT", symbol="ZENA")
    ts.subscribe()
    ts.cancel()
    assert calls == [("CONTRACT", "AllLast")]
    assert not getattr(ts, "rec_error", None)
