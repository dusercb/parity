"""The minute-fold must cost nothing when there is no new minute to fold.

_fold_trail runs for every trailing position on every ~2s pass, and with the
wedge gone the dip's candle_trail is the ONLY live exit mode -- so this is now
the exit path, not a branch of it. It used to resample the entire session frame
and rebuild a dict of every minute since 04:00 on each call: O(session) work to
consume the one new minute that arrives on roughly every 30th call, and growing
all morning. By 09:00 that is ~1800 ten-second rows resampled and ~300 minutes
scanned, per position, thirty times a minute, on VPS hardware that also has to
service tick callbacks.

These tests pin BOTH halves of the fix: that the work is skipped when nothing
completed, and -- more important -- that skipping it changes no decision. The
existing test_candle_trail_exit.py suite runs the same fold against the REAL
signal_core.build_1m_bars, so the behavioural equivalence is covered there too.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent
                      / "hindsight" / "ibkr_premarket_backtester"))

import signal_core as sc_real

from live_bot.order_manager import OrderManager
from live_bot.position_state import PositionBook
from live_bot.risk_guard import RiskGuard

from test_order_layer import _FakeBroker, _Rec, _bcfg, _cfg

ET = "America/New_York"
BASE = pd.Timestamp("2026-07-27 08:00:00", tz=ET)


class _CountingSC:
    """Wraps signal_core and counts the expensive call."""
    def __init__(self):
        self.build_calls = 0
        self.rows_resampled = 0

    def __getattr__(self, name):
        return getattr(sc_real, name)

    def build_1m_bars(self, df, *a, **k):
        self.build_calls += 1
        self.rows_resampled += len(df)
        return sc_real.build_1m_bars(df, *a, **k)


def _om(tmp_path, sc):
    cfg = _cfg()
    cfg.strategy.exit_mode = "candle_trail_1min"
    cfg.strategy.trail_partial_at_target = 0.5
    return OrderManager(_FakeBroker(), sc, cfg, _bcfg(), PositionBook(tmp_path / "state"),
                        RiskGuard(_bcfg(), tmp_path / "state"), _Rec(), "DU1", slots=None)


def _open(om):
    om.enter({"ticker": "VIVK", "date": "2026-07-27", "entry_mode": "dip_red_break",
              "direction": "long", "setup_index": 1, "entry_price": 3.00,
              "stop_price": 2.90, "target_price": 3.10, "trigger_price": 3.00,
              "volume_at_entry": 500000.0})
    om.poll_fills()
    pos = next(iter(om.book.positions.values()))
    pos.entry_price, pos.stop_price = 3.00, 2.90
    pos.opened_at = "2026-07-27T08:00:00-04:00"
    pos.folded_upto = "2026-07-27T08:00:00-04:00"
    return pos


def _frame(minutes: int, bars_into_last: int = 1):
    """`minutes` complete minutes from 08:00, plus a few bars into the next one
    so the fold sees them as complete. Each minute's low ratchets upward."""
    rows = []
    for m in range(minutes):
        low = 3.00 + 0.05 * m
        for s in range(6):
            rows.append((BASE + pd.Timedelta(minutes=m, seconds=10 * s),
                         low + 0.02, low + 0.06, low, low + 0.04))
    for s in range(bars_into_last):
        low = 3.00 + 0.05 * minutes
        rows.append((BASE + pd.Timedelta(minutes=minutes, seconds=10 * s),
                     low + 0.02, low + 0.06, low, low + 0.04))
    return pd.DataFrame(rows, columns=["dt_et", "open", "high", "low", "close"]).assign(volume=1000)


def test_a_new_minute_is_folded_and_ratchets_the_stop(tmp_path):
    sc = _CountingSC()
    om = _om(tmp_path, sc)
    pos = _open(om)
    om._fold_trail(pos, _frame(minutes=3))
    assert sc.build_calls == 1
    assert pos.stop_price == 3.10, "stop ratchets to the last completed minute's low"


def test_a_second_pass_with_no_new_minute_does_no_work(tmp_path):
    """The common case: ~29 of every 30 calls have nothing to fold."""
    sc = _CountingSC()
    om = _om(tmp_path, sc)
    pos = _open(om)
    frame = _frame(minutes=3)
    om._fold_trail(pos, frame)
    before = sc.build_calls
    for _ in range(20):
        om._fold_trail(pos, frame)
    assert sc.build_calls == before, "no new minute -> no resample at all"


def test_only_the_unfolded_tail_is_resampled(tmp_path):
    """The whole point on limited hardware: cost tracks NEW minutes, not session
    length. Folding minute 30 must not re-resample minutes 0-29."""
    sc = _CountingSC()
    om = _om(tmp_path, sc)
    pos = _open(om)
    om._fold_trail(pos, _frame(minutes=30))          # catch up in one pass
    sc.rows_resampled = 0
    om._fold_trail(pos, _frame(minutes=31))          # one further minute completes
    assert sc.rows_resampled <= 12, (
        f"resampled {sc.rows_resampled} rows to fold one new minute -- "
        "the tail slice is not being applied")


def test_incremental_folding_matches_folding_all_at_once(tmp_path):
    """Equivalence, the assertion that actually protects the money: a position
    folded minute-by-minute as it lives must end up exactly where one folded in a
    single catch-up pass does."""
    sc_a, sc_b = _CountingSC(), _CountingSC()
    om_a, om_b = _om(tmp_path / "a", sc_a), _om(tmp_path / "b", sc_b)
    pos_a, pos_b = _open(om_a), _open(om_b)
    for m in range(1, 13):                            # live: a minute at a time
        om_a._fold_trail(pos_a, _frame(minutes=m))
    om_b._fold_trail(pos_b, _frame(minutes=12))       # restart: one catch-up pass
    assert pos_a.stop_price == pos_b.stop_price
    assert pos_a.trail_active == pos_b.trail_active
    assert pos_a.folded_upto == pos_b.folded_upto


def test_an_empty_or_missing_frame_is_safe(tmp_path):
    sc = _CountingSC()
    om = _om(tmp_path, sc)
    pos = _open(om)
    om._fold_trail(pos, None)
    om._fold_trail(pos, _frame(minutes=0, bars_into_last=0))
    assert sc.build_calls == 0
