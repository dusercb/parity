"""An order the bot has DECIDED on is not an order the broker has DONE.

Regression cover for the two order-lifecycle defects in the 2026-08-14 session,
which are the same mistake on opposite ends of a trade: local state advanced on
something the bot predicted rather than something the broker confirmed.

  * WETO (exit side). Price printed through the 9.45 target at 10:41:44. The tick
    path called _settle() straight away -- P&L banked at the 9.47 PRINT and the
    position deleted from the book -- while the resting limit at the exchange did
    not actually fill for another 88 seconds. For those 88 seconds a live
    242-share long was outside the book, so its synthetic stop was unenforced and
    open_positions.json no longer listed it for crash adoption; the reconciler
    fired 13 alarms it had no authority to act on. _settle also popped the
    resting target WITHOUT cancelling it, so had price reversed instead of
    filling, nothing would ever have cleaned the position up -- flatten_all walks
    only the book and _pending.

  * BOXL (entry side). A short entry was held for a locate (IBKR 404) at 10:01
    and never resolved: poll_fills only cleared _pending on Filled/Cancelled/
    Inactive, so a PreSubmitted order sat there for 3h34m holding its sizing
    reservation (which inflated the recorded leverage telemetry from 0.53x to
    2.86x), keeping the symbol permanently in reconcile's `busy` set, and able to
    fill at any point on a signal hours dead.

The shared property being asserted: the exit DECISION may be predicted, but the
BOOKKEEPING only ever follows the broker.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from test_order_layer import _FakeBroker, _FakeOrderTrade, _om, _trade   # shared harness


class _Stream:
    """TickStream stand-in that also records what it was ASKED about, so a test
    can assert which levels were live on a given pass."""
    def __init__(self, touch=None):
        self._touch = touch
        self.asked = []

    def first_touch(self, since, direction, stop, target):
        self.asked.append({"stop": stop, "target": target})
        if self._touch is None:
            return None, None, None, since
        kind, price = self._touch
        return kind, None, price, since + 1


def _frame(low, high):
    return pd.DataFrame([{"dt_et": pd.Timestamp("2026-08-14 10:41:00", tz="America/New_York"),
                          "open": 8.77, "high": high, "low": low, "close": 8.90, "volume": 100.0}])


def _open_weto(tmp_path):
    """The WETO position as it stood: long 242 @ 8.773, stop 8.34, target 9.45."""
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade(ticker="WETO", mode="wedge_break", direction="long",
                    entry=8.67, stop=8.34, target=9.45, trigger=8.67))
    om.poll_fills()
    pos = book.positions["WETO::wedge_break"]
    broker._pos = {"WETO": pos.remaining}          # the broker holds what we bought
    return om, broker, book, pos


def _kinds(om):
    return [k for k, _ in om.rec.events]


def _event(om, kind):
    return [p for k, p in om.rec.events if k == kind]


# --- exit side: the touch decides, the broker settles -------------------------

def test_target_touch_does_not_book_the_exit_before_the_broker_fills(tmp_path):
    """The WETO bug itself. A print through the target must NOT close the book."""
    om, broker, book, pos = _open_weto(tmp_path)
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    assert pos.key() in book.positions, "the position must stay in the book until the broker fills"
    assert "position_closed" not in _kinds(om), "P&L must not be booked on a print"
    assert _event(om, "target_touched"), "the touch decision must still be recorded"


def test_touched_target_settles_at_the_broker_fill_not_the_touch_print(tmp_path):
    """9.47 printed; the resting limit is at 9.45. The recorded exit must be the
    broker's fill -- booking the print overstates every target win."""
    om, broker, book, pos = _open_weto(tmp_path)
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    tt = om._targets[pos.key()]
    tt.orderStatus.status, tt.orderStatus.avgFillPrice = "Filled", 9.45
    om._resolve_bracket(pos, _frame(low=9.40, high=9.47), _Stream())
    closed = _event(om, "position_closed")
    assert len(closed) == 1
    assert closed[0]["exit"] == 9.45 and closed[0]["reason"] == "target"
    assert pos.key() not in book.positions


def test_stop_is_still_enforced_while_a_touched_target_is_unconfirmed(tmp_path):
    """The safety property that the old code lost outright: between the touch and
    the fill the position is still live, so a reversal through the stop must still
    exit. Under the old code the position was already gone from the book and
    manage() never looked at it again."""
    om, broker, book, pos = _open_weto(tmp_path)
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    om._resolve_bracket(pos, _frame(low=8.30, high=9.00), _Stream(("stop", 8.33)))
    ex = _event(om, "exit_submitted")
    assert ex and ex[0]["reason"] == "stop"
    assert broker.cancelled, "the resting target must be cancelled when the stop takes over"


def test_target_does_not_refire_while_awaiting_confirmation(tmp_path):
    """Once touched, only the stop may fire -- otherwise every subsequent print
    through the level re-arms and re-records the same decision."""
    om, broker, book, pos = _open_weto(tmp_path)
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    s = _Stream()
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), s)
    assert s.asked[0]["target"] == float("inf"), "a confirming long must watch only its stop"
    assert len(_event(om, "target_touched")) == 1


def test_unconfirmed_target_escalates_to_a_marketable_exit(tmp_path):
    """The market traded through the level without taking our size. Waiting on a
    resting limit the market has left is how a winner rides back to the stop."""
    om, broker, book, pos = _open_weto(tmp_path)
    om.target_confirm_s = 0.0
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    om._resolve_bracket(pos, _frame(low=9.00, high=9.40), _Stream())
    assert _event(om, "target_unconfirmed")
    ex = _event(om, "exit_submitted")
    assert ex and ex[0]["reason"] == "target_unconfirmed"
    assert broker.cancelled, "the stale resting limit must be cancelled, not orphaned"


def test_a_fresh_touch_is_given_its_confirmation_window(tmp_path):
    om, broker, book, pos = _open_weto(tmp_path)
    om.target_confirm_s = 60.0
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    om._resolve_bracket(pos, _frame(low=9.00, high=9.40), _Stream())
    assert not _event(om, "target_unconfirmed")
    assert pos.key() in book.positions


def test_escalation_runs_even_with_no_bar_data(tmp_path):
    """A position whose feed has gone quiet must not be able to sit on an
    unconfirmed target forever."""
    om, broker, book, pos = _open_weto(tmp_path)
    om.target_confirm_s = 0.0
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    om._resolve_bracket(pos, None, None)
    assert _event(om, "target_unconfirmed")


def test_reconcile_raises_no_phantom_alarm_while_a_target_is_unconfirmed(tmp_path):
    """The 13 WETO alarms, directly. Book and broker must agree during the wait --
    an alarm here is both noise and a signal the reconciler cannot act on."""
    om, broker, book, pos = _open_weto(tmp_path)
    pos.opened_at = "2020-01-01T00:00:00"            # past the fresh-fill grace
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    assert om.reconcile_open(min_interval_s=0.0) == []
    assert pos.key() in book.positions


def test_flatten_all_still_reaches_a_confirming_position(tmp_path):
    """The premarket-flat guarantee. Under the old code a touched-but-unfilled
    position was in neither the book nor _pending, so the session-end flatten
    could not see it at all."""
    om, broker, book, pos = _open_weto(tmp_path)
    om._resolve_bracket(pos, _frame(low=8.80, high=9.47), _Stream(("target", 9.47)))
    om.flatten_all(reason="session_end")
    ex = _event(om, "exit_submitted")
    assert ex and ex[0]["reason"] == "session_end"


# --- entry side: a held order is not a resting order --------------------------

class _HeldBroker(_FakeBroker):
    """IBKR 404: the order is accepted but held while a locate is found, so it
    sits in PreSubmitted indefinitely rather than filling or cancelling."""
    def marketable_limit(self, ticker, action, qty, limit):
        t = _FakeOrderTrade(status="PreSubmitted")
        self.submitted.append(("marketable", ticker, action, qty, limit, t))
        return t


def _submit_boxl(tmp_path, ttl=0.0):
    om, broker, guard, book = _om(tmp_path, broker=_HeldBroker())
    om.entry_ttl_s = ttl
    om.enter(_trade(ticker="BOXL", mode="wedge_break", direction="short",
                    entry=8.36, stop=8.43, target=8.08, trigger=8.36))
    return om, broker, book


def test_locate_held_entry_is_cancelled_once_stale(tmp_path):
    om, broker, book = _submit_boxl(tmp_path)
    om.poll_fills()
    assert broker.cancelled, "a held entry must not be left working"
    assert _event(om, "entry_stale_cancel"), "and the abandonment must be visible in the recording"


def test_a_fresh_entry_is_never_cancelled(tmp_path):
    om, broker, book = _submit_boxl(tmp_path, ttl=60.0)
    om.poll_fills()
    assert not broker.cancelled
    assert "BOXL" in om._pending


def test_the_stale_cancel_is_sent_only_once(tmp_path):
    om, broker, book = _submit_boxl(tmp_path)
    om.poll_fills()
    om.poll_fills()
    om.poll_fills()
    assert len(broker.cancelled) == 1


def test_the_reservation_is_held_until_the_broker_confirms(tmp_path):
    """A cancel races an in-flight fill. Dropping the reservation on the request
    would recreate the WETO phantom on the entry side -- a real position with
    nothing tracking it."""
    om, broker, book = _submit_boxl(tmp_path)
    om.poll_fills()
    assert "BOXL" in om._pending, "the cancel is a request, not an outcome"
    order = om._pending["BOXL"]["order"]
    order.orderStatus.status, order.orderStatus.avgFillPrice = "Filled", 8.16
    om.poll_fills()
    assert "BOXL::wedge_break" in book.positions, "a fill that beat the cancel must still be managed"


def test_a_confirmed_cancel_releases_the_sizing_reservation(tmp_path):
    """The leverage-telemetry corruption: while BOXL sat pending it held
    1142 x 8.16 of committed notional that no real exposure backed."""
    om, broker, book = _submit_boxl(tmp_path)
    om.poll_fills()
    assert om._committed_notional() > 0
    om._pending["BOXL"]["order"].orderStatus.status = "Cancelled"
    om.poll_fills()
    assert om._committed_notional() == 0
    assert "BOXL" not in om._pending
    assert _event(om, "entry_unfilled")


def test_a_stale_entry_stops_masking_the_symbol_from_reconciliation(tmp_path):
    """A symbol in _pending is in reconcile's `busy` set. Left forever, the symbol
    is never checked against broker truth again for the rest of the session."""
    om, broker, book = _submit_boxl(tmp_path)
    broker._pos = {"BOXL": -1142}                     # broker holds it, book does not
    assert om.reconcile_open(min_interval_s=0.0) == [], "masked while genuinely in flight"
    om.poll_fills()
    om._pending["BOXL"]["order"].orderStatus.status = "Cancelled"
    om.poll_fills()
    alarms = om.reconcile_open(min_interval_s=0.0)
    assert alarms and "NO position in the book" in alarms[0]
