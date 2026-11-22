"""Integration: the session controller's coverage wiring (_reconcile_coverage +
confirm/demote + _pinned_symbols) on a REAL Observer against a fake IB. Proves
the streamed set (self.feeds) tracks the current top movers, a pinned position is
never demoted, and a demoted symbol re-promotes -- the losslessness guarantees,
exercised through the actual controller code, not the pure core.
"""
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import yaml

VPS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(VPS))
sys.path.insert(0, str(VPS.parent / "hindsight" / "ibkr_premarket_backtester"))

from live_bot.coverage import CoverageManager
from live_bot.live_data import ET
from live_bot.position_state import Position
from live_bot.session_controller import Observer
from replay.parity_diff import load_frames


class _Evt:                       # stand-in for ib_insync's updateEvent (+=/-=)
    def __iadd__(self, f): return self
    def __isub__(self, f): return self


class _FakeIB:
    """Just enough IB for confirm()/demote(): no bars, no snapshots, no socket."""
    def qualifyContracts(self, *a, **k): return None
    def reqHistoricalData(self, *a, **k): return []           # backfill + baseline -> empty
    def reqRealTimeBars(self, *a, **k): return SimpleNamespace(updateEvent=_Evt())
    def cancelRealTimeBars(self, *a, **k): return None
    def reqTickers(self, *a, **k): return []
    def sleep(self, *a, **k): return None


def _observer(tmp_path, max_streamed=3):
    real = yaml.safe_load((VPS / "bot_config.yaml").read_text())
    cfg = copy.deepcopy(real)
    cfg["signal"]["backtester_path"] = str((VPS.parent / "hindsight" / "ibkr_premarket_backtester").resolve())
    cfg["signal"]["pin"] = "UNPINNED-DEV"
    cfg["trading"]["enabled"] = False                          # observer mode: no watch
    cfg["recording"]["dir"] = str(tmp_path / "rec")
    cfg["state"]["dir"] = str(tmp_path / "state")
    p = tmp_path / "bot_config.yaml"
    p.write_text(yaml.safe_dump(cfg))
    obs = Observer(p)
    obs.ib = _FakeIB()
    # small, deterministic budget; no dwell/snapshot noise for the wiring assertions
    obs.coverage = CoverageManager(max_streamed=max_streamed, min_dwell_polls=0,
                                   max_demote_per_poll=5, max_promote_per_poll=10)
    obs._max_snapshots = 0
    return obs


def test_streamed_set_fills_then_a_higher_mover_displaces_the_weakest(tmp_path):
    obs = _observer(tmp_path, max_streamed=3)
    obs._reconcile_coverage(["A", "B", "C", "D", "E"])
    assert set(obs.feeds) == {"A", "B", "C"}          # top 3 by rank streamed
    obs._reconcile_coverage(["Z", "A", "B", "C", "D"])  # Z is the new #1
    assert "Z" in obs.feeds and "C" not in obs.feeds
    assert set(obs.feeds) == {"Z", "A", "B"}


def test_open_position_is_never_demoted(tmp_path):
    obs = _observer(tmp_path, max_streamed=2)
    obs._reconcile_coverage(["POS", "A"])
    assert set(obs.feeds) == {"POS", "A"}
    pos = Position(ticker="POS", entry_mode="dip_red_break", direction="long",
                   date="2026-07-27", shares=10, entry_price=1.0, stop_price=0.9,
                   target_price=1.2, remaining=10)
    obs.book.positions[pos.key()] = pos               # now holding POS
    # POS falls off the scanner entirely AND two fresh movers arrive
    obs._reconcile_coverage(["X", "Y", "A"])
    assert "POS" in obs.feeds                          # sacred: its synthetic stop needs the feed


def test_demoted_symbol_stays_known_and_re_promotes(tmp_path):
    obs = _observer(tmp_path, max_streamed=1)
    obs._reconcile_coverage(["A", "B"])
    assert set(obs.feeds) == {"A"}
    obs._reconcile_coverage(["B", "A"])                # B outranks A -> A demotes
    assert set(obs.feeds) == {"B"}
    assert "A" in obs.coverage.monitor                 # not forgotten
    obs._reconcile_coverage(["A", "B"])                # A re-ignites -> re-promotes
    assert set(obs.feeds) == {"A"}


# -- the demoted feed's bars must survive the demote -------------------------

def _events(obs, kind):
    rows = [json.loads(l) for l in Path(obs.rec.path).read_text(encoding="utf-8").splitlines() if l.strip()]
    return [r for r in rows if r["kind"] == kind]


def _seed_bars(feed, n, start=None):
    """Put `n` closed 10s bars into a feed, ending safely in the past so frame()'s
    forming-bar cutoff (now - 10s) can never exclude them."""
    t0 = (start or pd.Timestamp.now(tz=ET).floor("10s") - pd.Timedelta(minutes=30))
    feed._bars10 = {t0 + pd.Timedelta(seconds=10 * i): [1.0 + i, 1.5 + i, 0.9 + i, 1.4 + i, 100.0 * (i + 1)]
                    for i in range(n)}
    return t0


def test_demote_records_the_frame_before_releasing_the_feed(tmp_path):
    """Demote used to drop the feed without writing its bars, deleting the only
    copy. Pinning covers open positions and armed setups ONLY, so a symbol that
    fires, closes and disarms is demotion-eligible with its trade already in the
    recording -- the replay could not then reproduce it and parity_diff would
    report a phantom Class-0 `missing_on_replay`."""
    obs = _observer(tmp_path, max_streamed=1)
    obs._reconcile_coverage(["A", "B"])
    _seed_bars(obs.feeds["A"], 3)
    obs._reconcile_coverage(["B", "A"])                # B outranks A -> A demotes
    assert "A" not in obs.feeds

    frames = [e for e in _events(obs, "frame") if e["symbol"] == "A"]
    assert len(frames) == 1, "the demoted feed's frame must reach the recording"
    assert len(frames[0]["bars"]) == 3


def test_replay_unions_a_demote_frame_with_the_later_session_end_frame(tmp_path):
    """A re-promoted symbol is dumped twice. The replay must UNION the dumps: if a
    re-promotion's backfill failed, its session-end frame is only a short tail, and
    taking the last dump alone would shrink the session the engines replay on."""
    obs = _observer(tmp_path, max_streamed=1)
    obs._reconcile_coverage(["A", "B"])
    t0 = _seed_bars(obs.feeds["A"], 4)                 # early stint: bars 0-3
    obs._reconcile_coverage(["B", "A"])                # A demotes -> dump #1 (4 bars)

    obs._reconcile_coverage(["A", "B"])                # A re-promotes (backfill returns [])
    assert "A" in obs.feeds
    _seed_bars(obs.feeds["A"], 2, start=t0 + pd.Timedelta(seconds=40))   # late stint only
    obs._record_frame("A", obs.feeds["A"])             # dump #2 (2 bars, no overlap)

    rows = [json.loads(l) for l in Path(obs.rec.path).read_text(encoding="utf-8").splitlines() if l.strip()]
    frames = load_frames(rows)
    assert len(frames["A"]) == 6, "union of both dumps, not just the last one"
    assert frames["A"]["dt_et"].is_monotonic_increasing


def test_replay_dedupes_overlapping_dumps_of_the_same_bar(tmp_path):
    """The normal case: re-promotion backfills to the session start, so dump #2 is
    a superset of dump #1. The union must not double-count the shared bars."""
    obs = _observer(tmp_path, max_streamed=1)
    obs._reconcile_coverage(["A", "B"])
    t0 = _seed_bars(obs.feeds["A"], 3)
    obs._reconcile_coverage(["B", "A"])                # dump #1: bars 0-2

    obs._reconcile_coverage(["A", "B"])                # re-promote
    _seed_bars(obs.feeds["A"], 5, start=t0)            # backfilled superset: bars 0-4
    obs._record_frame("A", obs.feeds["A"])             # dump #2

    frames = load_frames([json.loads(l) for l in
                          Path(obs.rec.path).read_text(encoding="utf-8").splitlines() if l.strip()])
    assert len(frames["A"]) == 5                       # 3 + 5 with 3 overlapping
    assert not frames["A"]["dt_et"].duplicated().any()
