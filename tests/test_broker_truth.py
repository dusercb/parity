"""Broker truth outranks the bot's model of itself.

Regression cover for the ZENA 2026-08-07 incident. The bot held a long with a
resting target limit at the exchange AND a tick slot. The target filled at
07:13:40 -- but the resting-fill check lived only in the bar branch of
_resolve_bracket, below the stream branch's unconditional `return`, so a position
holding a tick slot could never see it. The bot tracked the phantom for 2h13m,
then tried to flatten shares it no longer owned, which IBKR read as a naked short
(error 404, held for locate) and refused. Reported P&L was wrong by the whole
trade, and the session ended with `flatten_incomplete`.

Three separate properties are asserted here:
  * a filled resting target settles the position REGARDLESS of the tick slot;
  * a stream that never delivers (IBKR error 10190) cannot suppress the bar stop;
  * the book self-heals against the broker mid-session, but never mistakes a
    just-filled entry for a vanished position.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from test_order_layer import _FakeBroker, _om, _trade      # shared harness
from live_bot.position_state import Position


class _Stream:
    """TickStream stand-in. `ticks=()` models a stream IBKR rejected: the object
    exists, first_touch is callable, and it will never resolve anything."""
    def __init__(self, touch=None):
        self._touch = touch

    def first_touch(self, since, direction, stop, target):
        if self._touch is None:
            return None, None, None, since
        kind, price = self._touch
        return kind, None, price, since + 1


def _frame(low, high):
    return pd.DataFrame([{"dt_et": pd.Timestamp("2026-08-07 07:20:00", tz="America/New_York"),
                          "open": 1.60, "high": high, "low": low, "close": 1.60, "volume": 100.0}])


class _Feed:
    def __init__(self, df):
        self._df = df

    def frame(self):
        return self._df


def _open_zena(tmp_path):
    """A filled long with a resting target, exactly as ZENA was."""
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade(ticker="ZENA", mode="wedge_break", direction="long",
                    entry=1.60, stop=1.58, target=1.63, trigger=1.60))
    om.poll_fills()
    pos = book.positions["ZENA::wedge_break"]
    return om, broker, book, pos


# --- the target fill must be seen even while a tick slot is held --------------

def test_filled_resting_target_settles_even_when_a_tick_slot_is_held(tmp_path):
    om, broker, book, pos = _open_zena(tmp_path)
    tt = om._targets[pos.key()]
    tt.orderStatus.status, tt.orderStatus.avgFillPrice = "Filled", 1.63   # broker fills it
    # a stream holding a slot that has resolved nothing -- the ZENA state exactly
    om._resolve_bracket(pos, _frame(low=1.60, high=1.61), _Stream())
    assert pos.key() not in book.positions, "target fill must close the position"
    kinds = [k for k, _ in om.rec.events]
    assert "position_closed" in kinds


def test_target_fill_is_checked_before_any_predicted_level(tmp_path):
    """Both the target (filled) and the stop (bar-breached) are true at once. The
    broker's FACT must win over the predicted stop -- otherwise the bot sells
    shares it has already sold."""
    om, broker, book, pos = _open_zena(tmp_path)
    tt = om._targets[pos.key()]
    tt.orderStatus.status, tt.orderStatus.avgFillPrice = "Filled", 1.63
    om._resolve_bracket(pos, _frame(low=1.50, high=1.63), _Stream(("stop", 1.58)))
    closed = [p for k, p in om.rec.events if k == "position_closed"]
    assert len(closed) == 1 and closed[0]["reason"] == "target"
    assert pos.key() not in book.positions


# --- a dead stream must not suppress the bar path -----------------------------

def test_stream_that_never_delivers_falls_through_to_the_bar_stop(tmp_path):
    """IBKR rejects a tick subscription with 10190 but the local Ticker object
    still exists, so `stream is not None` stays true forever. The bar stop must
    still fire."""
    om, broker, book, pos = _open_zena(tmp_path)
    om._resolve_bracket(pos, _frame(low=1.55, high=1.61), _Stream())   # dead stream, stop breached
    assert any(k == "exit_submitted" for k, _ in om.rec.events)


def test_a_live_stream_still_resolves_tick_precise_first(tmp_path):
    """The fall-through must not cost the tick path its precedence: a stream that
    DOES resolve still wins, and records the tick price."""
    om, broker, book, pos = _open_zena(tmp_path)
    om._resolve_bracket(pos, _frame(low=1.55, high=1.61), _Stream(("stop", 1.575)))
    ex = [p for k, p in om.rec.events if k == "exit_submitted"][0]
    assert ex["source"] == "tick" and ex["tick_price"] == 1.575


# --- continuous reconciliation ------------------------------------------------

def test_reconcile_drops_a_position_the_broker_no_longer_holds(tmp_path):
    om, broker, book, pos = _open_zena(tmp_path)
    pos.opened_at = "2020-01-01T00:00:00"            # long past the grace window
    broker._pos = {}                                  # broker holds nothing
    alarms = om.reconcile_open(min_interval_s=0.0)
    assert alarms and "broker holds none" in alarms[0]
    assert pos.key() not in book.positions, "the book must self-heal mid-session"


def test_reconcile_does_not_drop_a_just_filled_position(tmp_path):
    """The readback lags a fill. Without the grace window a fresh entry looks
    exactly like a vanished one -- and dropping it would strand a real position
    with nothing watching its stop."""
    om, broker, book, pos = _open_zena(tmp_path)     # opened_at = now
    broker._pos = {}                                  # readback hasn't caught up
    assert om.reconcile_open(min_interval_s=0.0) == []
    assert pos.key() in book.positions


def test_reconcile_skips_symbols_with_an_exit_in_flight(tmp_path):
    om, broker, book, pos = _open_zena(tmp_path)
    pos.opened_at = "2020-01-01T00:00:00"
    om._exiting[pos.key()] = (pos, SimpleNamespace(), "stop")   # exit submitted, not yet filled
    broker._pos = {}
    assert om.reconcile_open(min_interval_s=0.0) == []
    assert pos.key() in book.positions


def test_reconcile_is_rate_limited(tmp_path):
    om, broker, book, pos = _open_zena(tmp_path)
    pos.opened_at = "2020-01-01T00:00:00"
    broker._pos = {}
    assert om.reconcile_open(min_interval_s=0.0)          # first call runs
    om.book.positions[pos.key()] = pos                    # put it back
    assert om.reconcile_open(min_interval_s=60.0) == []   # second is suppressed


# --- event wiring -------------------------------------------------------------

class _Evt:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, f):
        self.handlers.append(f)
        return self

    def fire(self, *a):
        for f in list(self.handlers):
            f(*a)


def test_attach_events_routes_a_broker_callback_into_the_fill_path(tmp_path):
    """A fill must be acted on when the broker reports it, not on the next poll."""
    om, broker, book, pos = _open_zena(tmp_path)
    ib = SimpleNamespace(orderStatusEvent=_Evt(), execDetailsEvent=_Evt())
    om.attach_events(ib)
    tt = om._targets[pos.key()]
    tt.orderStatus.status, tt.orderStatus.avgFillPrice = "Filled", 1.63
    ib.orderStatusEvent.fire(tt)                  # broker says: filled
    assert pos.key() not in book.positions, "the event alone must settle it"


def test_event_handler_does_not_recurse_or_raise(tmp_path):
    om, broker, book, pos = _open_zena(tmp_path)
    ib = SimpleNamespace(orderStatusEvent=_Evt(), execDetailsEvent=_Evt())
    om.attach_events(ib)

    calls = {"n": 0}
    real = om.poll_fills

    def counting():
        calls["n"] += 1
        ib.orderStatusEvent.fire(None)            # re-entrant callback, as placing an order does
        real()

    om.poll_fills = counting
    ib.orderStatusEvent.fire(None)
    assert calls["n"] == 1, "the re-entrancy guard must stop recursion"
    assert not any(k == "order_event_error" for k, _ in om.rec.events)
