"""A stop may only be resolved by ticks that arrived AFTER the fill.

PPBT 2026-09-02, the trade that exposed this. The tick slot was granted at
07:38:31 and a 2.23 print landed at 07:38:40. The entry filled at 2.2970 at
08:47:39 with its stop at 2.23. Two tenths of a second later the bot "stopped
out" on that 69-minute-old print -- at a moment when price had not traded below
2.26 since the fill. It exited at 2.28 and the trade went on to 3.00 against a
2.35 target. VRAX 2026-08-19 shows the same 0.1s signature.

Cause: TickStream's buffer is never trimmed (by design -- tick index doubles as
the cursor), and _resolve_ticks read `since` as `self._tick_cursor.get(key, 0)`
while NOTHING ever set that entry when a position opened. So the first scan after
a fill walked the entire session's prints and returned the first one under the
stop, whenever it had happened.

The backtest has always guarded this ("ticks before the trigger crossed predate
the fill and must not count as exits"), so it was a Class-0 divergence as well as
a losing trade.

These run against the REAL TickStream, because the bug lived in the interaction
between its untrimmed buffer and the cursor default -- a stand-in that ignores
`since` cannot express it.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot.tick_stream import TickStream
from test_order_layer import _om, _trade

ET = "America/New_York"
T0 = pd.Timestamp("2026-09-02 07:38:31", tz=ET)


class _Slots:
    """SlotManager stand-in holding real TickStreams."""
    def __init__(self, streams=None):
        self.streams = streams or {}

    def get(self, symbol):
        return self.streams.get(symbol.upper())


def _stream(symbol="PPBT", prices=(), start=T0, step_s=1):
    """A real TickStream pre-loaded with `prices`, as the live buffer would be by
    the time a setup finally triggers."""
    ts = TickStream(ib=None, contract=None, symbol=symbol)
    for k, px in enumerate(prices):
        ts.on_tick(start + pd.Timedelta(seconds=step_s * k), float(px))
    return ts


def _open(om, ticker="PPBT", entry=2.29, stop=2.23, target=2.35):
    om.enter(_trade(ticker=ticker, direction="long",
                    entry=entry, stop=stop, target=target, trigger=entry))
    om.poll_fills()
    return om.book.positions[f"{ticker}::dip_red_break"]


def _exits(om):
    return [p for k, p in om.rec.events if k == "exit_submitted"]


# --- the PPBT replay ----------------------------------------------------------

def test_a_stale_pre_entry_tick_cannot_stop_a_position_out(tmp_path):
    """The buffer is full of prints below the stop -- the whole run-up the setup
    was built on. None of them may exit a position that did not exist yet."""
    om, broker, guard, book = _om(tmp_path)
    # the run-up PPBT actually made, all of it below the 2.23 stop
    om.slots = _Slots({"PPBT": _stream(prices=[2.30, 2.28, 2.23, 2.24, 2.19, 2.15,
                                               2.18, 2.25, 2.30, 2.34])})
    pos = _open(om)
    om.manage({})
    assert not _exits(om), "a print from before the fill is not a stop"
    assert pos.key() in book.positions


def test_a_genuine_post_entry_breach_still_exits(tmp_path):
    """The fix must not blunt the stop: a print below it AFTER the fill exits."""
    om, broker, guard, book = _om(tmp_path)
    stream = _stream(prices=[2.30, 2.23, 2.28, 2.30])       # stale 2.23 in there
    om.slots = _Slots({"PPBT": stream})
    pos = _open(om)
    om.manage({})
    assert not _exits(om)
    stream.on_tick(T0 + pd.Timedelta(hours=1), 2.22)        # a REAL breach, now
    om.manage({})
    ex = _exits(om)
    assert ex and ex[0]["reason"] == "stop" and ex[0]["tick_price"] == 2.22


def test_a_post_entry_target_touch_still_registers(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    stream = _stream(prices=[2.35, 2.36, 2.20, 2.30])        # stale prints both sides
    om.slots = _Slots({"PPBT": stream})
    pos = _open(om)
    om.manage({})
    assert not _exits(om) and not [k for k, _ in om.rec.events if k == "target_touched"]
    stream.on_tick(T0 + pd.Timedelta(hours=1), 2.36)
    om.manage({})
    assert [k for k, _ in om.rec.events if k in ("target_touched", "partial_submitted")]


def test_the_cursor_is_seeded_to_the_streams_current_position(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    stream = _stream(prices=[2.30] * 40)
    om.slots = _Slots({"PPBT": stream})
    pos = _open(om)
    assert om._tick_cursor[pos.key()] == stream.cursor == 40


# --- the opposite failure: a replaced stream must not blind the stop ----------

def test_a_regranted_slot_does_not_strand_the_cursor_past_the_new_buffer(tmp_path):
    """A released-then-regranted slot builds a NEW TickStream whose buffer starts
    at zero, while the stored cursor still refers to the old one. Left unclamped,
    `since` would sit past the end of the new buffer and scan nothing -- a MISSED
    stop, the opposite failure to the one seeding fixes."""
    om, broker, guard, book = _om(tmp_path)
    om.slots = _Slots({"PPBT": _stream(prices=[2.30] * 500)})
    pos = _open(om)
    assert om._tick_cursor[pos.key()] == 500
    # the slot is dropped and re-granted: a brand new stream, fresh buffer
    fresh = _stream(prices=[2.30, 2.29])
    om.slots = _Slots({"PPBT": fresh})
    fresh.on_tick(T0 + pd.Timedelta(hours=1), 2.20)          # breach on the new stream
    om.manage({})
    ex = _exits(om)
    assert ex and ex[0]["tick_price"] == 2.20, "the stop must still be seen"


def test_no_tick_slot_falls_back_to_the_bar_path(tmp_path):
    """Seeding must not be required for the bar stop to work -- a position with no
    slot is resolved from bars exactly as before."""
    om, broker, guard, book = _om(tmp_path)
    om.slots = _Slots({})                                    # no stream at all
    pos = _open(om)
    assert pos.key() not in om._tick_cursor
    frame = pd.DataFrame([{"dt_et": pd.Timestamp("2026-09-02 08:48:00", tz=ET),
                           "open": 2.29, "high": 2.30, "low": 2.20,
                           "close": 2.21, "volume": 1000}])

    class _Feed:
        def frame(self):
            return frame

    om.manage({"PPBT": _Feed()})
    assert _exits(om), "the bar path is untouched by the cursor change"
