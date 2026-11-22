"""Live candle_trail exit (the dip's exit mode): the event-driven mirror of the
backtest's runner -- the synthetic stop ratchets to each completed 1-min low,
the first fat-green close drops the target, and a target cross before that banks
the partial and sends the runner to breakeven. Fake broker; no TWS.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "hindsight" / "ibkr_premarket_backtester"))

import signal_core as sc_real   # real build_1m_bars/capped_shares for the minute-fold

from live_bot.order_manager import OrderManager
from live_bot.position_state import PositionBook
from live_bot.risk_guard import RiskGuard

# reuse the order-layer fakes
from test_order_layer import _FakeBroker, _Rec, _bcfg, _cfg


class _Feed:
    def __init__(self, df):
        self._df = df
    def frame(self):
        return self._df


def _om(tmp_path, exit_mode="candle_trail_1min", trail_partial=0.5):
    broker = _FakeBroker()
    guard = RiskGuard(_bcfg(), tmp_path / "state")
    book = PositionBook(tmp_path / "state")
    cfg = _cfg()
    cfg.strategy.exit_mode = exit_mode
    cfg.strategy.trail_partial_at_target = trail_partial
    om = OrderManager(broker, sc_real, cfg, _bcfg(), book, guard, _Rec(), "DU1", slots=None)
    return om, broker, guard, book


def _dip_trade():
    return {"ticker": "VIVK", "date": "2026-07-27", "entry_mode": "dip_red_break",
            "direction": "long", "setup_index": 1, "entry_price": 3.00, "stop_price": 2.90,
            "target_price": 3.10, "trigger_price": 3.00, "volume_at_entry": 500000.0}


def _open(om):
    om.enter(_dip_trade()); om.poll_fills()
    return next(iter(om.book.positions.values()))


def _bars(rows):
    """rows: list of (iso, o,h,l,c) -- iso is a readable label; the actual
    dt_et is a contiguous 10s grid from 08:00 (what the minute-fold reads)."""
    base = pd.Timestamp("2026-07-27 08:00:00", tz="America/New_York")
    return pd.DataFrame([{"dt_et": base + pd.Timedelta(seconds=10 * k),
                          "open": o, "high": h, "low": l, "close": c, "volume": 1000}
                         for k, (_iso, o, h, l, c) in enumerate(rows)])


def test_opens_with_no_resting_target_but_a_sized_partial(tmp_path):
    """candle_trail rests NOTHING. Its target does not close the position, it
    banks a partial and TRANSITIONS the rest, so book and broker have to move
    together -- which a resting limit cannot promise. The partial size is still
    recorded; the touch is what sends the order."""
    om, broker, guard, book = _om(tmp_path)
    pos = _open(om)
    assert pos.exit_mode == "candle_trail" and pos.trail_partial == 0.5
    assert not [s for s in broker.submitted if s[0] == "resting"]
    assert om._target_shares[pos.key()] == round(pos.shares * 0.5)


def test_stop_ratchets_to_prior_minute_low(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    pos = _open(om)                                   # entry 3.19-cap? no: fill=cap; stop 2.90
    pos.entry_price = 3.00; pos.stop_price = 2.90     # normalize for a clean assert
    pos.opened_at = "2026-07-27T08:00:00-04:00"; pos.folded_upto = "2026-07-27T08:00:00-04:00"
    # minute 08:00 completes with a low of 3.05 (above the 2.90 stop); a bar in
    # 08:01 lets the fold see 08:00 as complete.
    frame = _bars([("08:00:00", 3.0, 3.10, 3.05, 3.08)] * 6 + [("08:01:00", 3.12, 3.14, 3.11, 3.13)])
    frame.loc[:, "dt_et"] = [pd.Timestamp("2026-07-27 08:00:00", tz="America/New_York") + pd.Timedelta(seconds=10 * k) for k in range(len(frame))]
    om._fold_trail(pos, frame)
    assert pos.stop_price == 3.05                     # ratcheted up to the 08:00 minute low


def test_fat_green_minute_drops_the_target(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    pos = _open(om)
    pos.opened_at = "2026-07-27T07:59:00-04:00"; pos.folded_upto = "2026-07-27T08:00:00-04:00"
    # a FAT-GREEN 08:00 minute (opens 3.00, closes 3.28 at the top, body>60% of range)
    six = [("08:00:00", 3.00, 3.30, 2.99, 3.05), ("08:00:10", 3.05, 3.30, 3.04, 3.10),
           ("08:00:20", 3.10, 3.30, 3.09, 3.15), ("08:00:30", 3.15, 3.30, 3.14, 3.20),
           ("08:00:40", 3.20, 3.30, 3.19, 3.25), ("08:00:50", 3.25, 3.30, 3.24, 3.29)]
    tail = [("08:01:00", 3.29, 3.31, 3.28, 3.30)]
    frame = _bars(six + tail)
    assert not pos.trail_active
    om._fold_trail(pos, frame)
    assert pos.trail_active                            # fat-green -> target dropped
    assert not om._targets.get(pos.key())              # resting partial target cancelled


def test_target_cross_banks_partial_and_runner_rides_breakeven(tmp_path):
    """The full round trip: a bar tags the target -> a marketable limit goes out
    for the partial -> the broker fills it -> only THEN does the runner drop its
    target and jump to breakeven."""
    om, broker, guard, book = _om(tmp_path)
    pos = _open(om); pos.entry_price = 3.00; pos.stop_price = 2.90; pos.target_price = 3.10
    partial = om._target_shares[pos.key()]             # the sized 50% partial
    runner = pos.shares - partial
    frame = _bars([("08:05:00", 3.05, 3.11, 3.04, 3.10)])
    om._resolve_trail(pos, frame, stream=None)
    # submitted, but nothing has moved yet
    assert pos.key() in om._banking
    assert pos.remaining == pos.shares and not pos.trail_active
    om.poll_banks()                                    # fake broker fills at the cap
    assert pos.remaining == runner                     # partial banked, runner left
    assert pos.trail_active and pos.stop_price == 3.00 # runner stop jumped to breakeven
    assert guard.closed_trades == 1                    # the partial counts as a close leg


def test_runner_stop_exits_the_remainder(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    pos = _open(om); pos.entry_price = 3.00; pos.stop_price = 3.00
    pos.trail_active = True                             # runner riding breakeven
    frame = _bars([("08:06:00", 3.02, 3.03, 2.99, 3.00)])  # low 2.99 pierces the 3.00 stop
    om._resolve_trail(pos, frame, stream=None)
    om.poll_exits()
    assert book.open_count() == 0                      # runner exited on the trailing stop
