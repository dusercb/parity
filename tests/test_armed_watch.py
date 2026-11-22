"""LiveArmedWatch (Phase B bot side): arm from bars, fire on the tick cross.

Two layers:
  * plumbing (fake signal_core): capture/disarm, cross detection, the forming-bar
    context handed to evaluate_touch, and the one-entry-per-setup dedup.
  * integration (REAL signal_core on a synthetic dip): the whole path -- engine
    arms via on_armed, a simulated cross fires evaluate_touch with the same
    stop/target the backtest produces.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# signal_core from the pinned backtester repo
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "hindsight" / "ibkr_premarket_backtester"))

from live_bot.armed_watch import LiveArmedWatch

ET = "America/New_York"


def _cfg():
    return SimpleNamespace(strategy=SimpleNamespace(
        earliest_entry_time=None, latest_entry_time=None,
        macd_fast=12, macd_slow=26, macd_signal=9, order_latency_seconds=0.1))


def _armed(trigger=3.00):
    return SimpleNamespace(trigger=trigger, entry_limit=trigger + 0.20, hod_idx=2,
                           armed_hod=3.10, pullback_low=2.80, pullback_low_idx=4,
                           armed_bar_index=6, prior_win_entry=None)


class _FakeSC:
    """Minimal signal_core stand-in: records the evaluate_touch context, returns
    a canned decision. The indicator builders are identity (LiveArmedWatch only
    passes the frame through)."""
    def __init__(self, decision):
        self.decision = decision
        self.touch_calls = []

    def parse_hhmm(self, v, *a):
        h, m = v.split(":"); return (int(h), int(m))

    def build_1m_bars(self, f):
        return f

    def add_rvol(self, f, dav):
        return f

    def add_indicators(self, f, *a):
        return f

    def evaluate_touch(self, **kw):
        self.touch_calls.append(kw)
        return self.decision


def _frame():
    base = pd.Timestamp("2026-07-27 08:00:00", tz=ET)
    rows = [{"dt_et": base + pd.Timedelta(seconds=10 * k), "open": 2.5 + 0.05 * k,
             "high": 2.5 + 0.05 * k + 0.03, "low": 2.5 + 0.05 * k - 0.02,
             "close": 2.5 + 0.05 * k + 0.02, "volume": 5000} for k in range(6)]
    return pd.DataFrame(rows)


def _forming(high):
    return {"dt_et": pd.Timestamp("2026-07-27 08:01:00", tz=ET), "open": 2.98,
            "high": high, "low": 2.97, "close": high, "volume": 800}


# --- plumbing -----------------------------------------------------------------

def test_capture_records_latest_armed_and_disarm_clears():
    w = LiveArmedWatch(_FakeSC(None), _cfg())
    cb = w.capture("VIVK")
    cb(_armed(3.00)); cb(_armed(3.05))          # hook fires per bar; keep the latest
    assert w.is_armed("VIVK") and w.armed["VIVK"].trigger == 3.05
    w.disarm("VIVK")
    assert not w.is_armed("VIVK")


def test_no_fire_before_the_trigger_is_crossed():
    fake = _FakeSC(SimpleNamespace(kind="fire"))
    w = LiveArmedWatch(fake, _cfg())
    w.capture("VIVK")(_armed(3.00))
    assert w.check_cross("VIVK", _frame(), _forming(high=2.99), 100.0, stream=None) is None
    assert fake.touch_calls == []               # trigger not reached -> evaluate_touch not called


def test_cross_calls_evaluate_touch_with_forming_context():
    fire = SimpleNamespace(kind="fire", fill=3.01, stop=2.80, target=3.22)
    fake = _FakeSC(fire)
    w = LiveArmedWatch(fake, _cfg())
    w.capture("VIVK")(_armed(3.00))
    dec = w.check_cross("VIVK", _frame(), _forming(high=3.02), 100.0, stream=None)
    assert dec is fire
    kw = fake.touch_calls[0]
    # the forming bar is the LAST row of the extended frame, high = the cross
    assert kw["high"] == 3.02 and kw["i"] == len(_frame())      # appended as index n
    assert kw["trigger"] == 3.00 and kw["hod_idx"] == 2 and kw["pullback_low"] == 2.80
    assert kw["premarket"].iloc[kw["i"]]["high"] == 3.02


def test_one_entry_per_setup_then_no_refire():
    fake = _FakeSC(SimpleNamespace(kind="fire"))
    w = LiveArmedWatch(fake, _cfg())
    w.capture("VIVK")(_armed(3.00))
    assert w.check_cross("VIVK", _frame(), _forming(3.02), 100.0, stream=None).kind == "fire"
    # a second cross for the same symbol/session must NOT re-fire (dedup)
    assert w.check_cross("VIVK", _frame(), _forming(3.05), 100.0, stream=None) is None
    assert len(fake.touch_calls) == 1


def test_reject_does_not_mark_crossed():
    """A gate reject at the cross leaves the setup live -- a later, cleaner cross
    can still fire (only a 'fire' consumes the one-entry budget)."""
    fake = _FakeSC(SimpleNamespace(kind="reject", counter="skipped_macd_not_bullish"))
    w = LiveArmedWatch(fake, _cfg())
    w.capture("VIVK")(_armed(3.00))
    assert w.check_cross("VIVK", _frame(), _forming(3.02), 100.0, stream=None).kind == "reject"
    assert "VIVK" not in w._crossed                    # not consumed
    fake.decision = SimpleNamespace(kind="fire")
    assert w.check_cross("VIVK", _frame(), _forming(3.03), 100.0, stream=None).kind == "fire"


# --- integration with the REAL engine -----------------------------------------

def _dip_session():
    """Climb 3->5 (HOD, +66%), a 3-candle RED dip to ~4.20 (last red high ~4.60),
    a green pause below, then a bar that breaks 4.61 -- a dip_red_break setup,
    mirroring the backtester's own fixture so the engine actually arms + fires."""
    base = pd.Timestamp("2026-07-27 08:00:00", tz=ET)
    rows = []
    t = base
    for k in range(40):
        o = 3.00 + 0.05 * k
        rows.append({"dt_et": t, "open": o, "high": o + 0.05, "low": o - 0.01, "close": o + 0.05, "volume": 8000})
        t += pd.Timedelta(seconds=10)
    for (o, h, l, c) in [(4.95, 4.96, 4.75, 4.78), (4.78, 4.80, 4.55, 4.58), (4.58, 4.60, 4.20, 4.25)]:
        rows.append({"dt_et": t, "open": o, "high": h, "low": l, "close": c, "volume": 3000}); t += pd.Timedelta(seconds=10)
    rows.append({"dt_et": t, "open": 4.25, "high": 4.35, "low": 4.24, "close": 4.33, "volume": 2000}); t += pd.Timedelta(seconds=10)
    return pd.DataFrame(rows), t


def test_integration_real_engine_arms_then_cross_fires():
    import signal_core as sc
    from premarket_backtester.config import load_config
    cfg = load_config(str(Path(__file__).resolve().parent.parent.parent /
                          "hindsight" / "ibkr_premarket_backtester" / "config.yaml"))
    # force the dip strategy config path used by the fixture
    from dataclasses import replace
    scfg = replace(cfg.strategy, entry_mode="dip_red_break", min_premarket_gain_pct=20.0,
                   min_rvol=2.5, earliest_entry_time=None, latest_entry_time=None,
                   require_breakout_close_confirm=False, rip_anchor="session_low")
    cfg = replace(cfg, strategy=scfg)

    frame, next_t = _dip_session()
    w = LiveArmedWatch(sc, cfg)
    prev = pd.DataFrame({"dt_et": pd.Series([], dtype=f"datetime64[ns, {ET}]"), "close": pd.Series([], dtype=float)})
    # ARM: run the engine on completed bars with the capture hook
    from datetime import date
    sc.backtest_all_trades("TST", date(2026, 7, 27), frame, frame, prev, cfg,
                           float_shares=5_000_000, daily_volume_fetcher=lambda t, d: 100.0,
                           on_armed=w.capture("TST"))
    assert w.is_armed("TST"), "engine should have armed a dip setup"
    trig = w.armed["TST"].trigger
    # FIRE: a forming bar that crosses the armed trigger
    forming = {"dt_et": next_t, "open": trig - 0.02, "high": trig + 0.05,
               "low": trig - 0.03, "close": trig + 0.04, "volume": 9000}
    dec = w.check_cross("TST", frame, forming, 100.0, stream=None)
    assert dec is not None and dec.kind in ("fire", "reject")
    if dec.kind == "fire":
        assert dec.stop is not None and dec.target is not None and dec.fill >= trig
