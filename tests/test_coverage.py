"""Tiered mover-coverage decision core (Tier C monitor -> B stream). The whole
point is LOSSLESSNESS: nothing the scanner ever surfaces is forgotten, movement
between tiers goes both ways, positions/armed are never demoted, and a re-woken
symbol re-promotes. Pure -- no IB.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot.coverage import CoverageManager


def _apply(cm, promote, demote):
    for s in promote:
        cm.mark_promoted(s)
    for s in demote:
        cm.mark_demoted(s)


def test_empty_budget_fills_from_the_top():
    cm = CoverageManager(max_streamed=3, max_promote_per_poll=10)
    cm.observe_scanner(["A", "B", "C", "D", "E"])
    promote, demote = cm.plan(pinned=set())
    _apply(cm, promote, demote)
    assert set(cm.streamed_symbols()) == {"A", "B", "C"}   # top 3 by rank
    assert not demote


def test_higher_ranked_mover_displaces_the_weakest_streamed():
    cm = CoverageManager(max_streamed=3, min_dwell_polls=0, max_demote_per_poll=3)
    cm.observe_scanner(["A", "B", "C"])
    _apply(cm, *cm.plan(set()))                # A,B,C streamed
    # A new #1 mover appears; C (rank 3) is the weakest -> C demotes, Z promotes.
    cm.observe_scanner(["Z", "A", "B", "C"])
    promote, demote = cm.plan(pinned=set())
    assert "Z" in promote and "C" in demote
    _apply(cm, promote, demote)
    assert set(cm.streamed_symbols()) == {"Z", "A", "B"}


def test_pinned_position_never_demoted_even_when_it_falls_off_scanner():
    cm = CoverageManager(max_streamed=2, min_dwell_polls=0)
    cm.observe_scanner(["POS", "A"])
    _apply(cm, *cm.plan(pinned={"POS"}))       # POS, A streamed
    # POS fades completely off the scanner AND two fresh movers arrive.
    cm.observe_scanner(["X", "Y", "A"])
    promote, demote = cm.plan(pinned={"POS"})  # still holding the position
    assert "POS" not in demote                 # sacred -- its synthetic stop needs the feed
    _apply(cm, promote, demote)
    assert "POS" in cm.streamed_symbols()


def test_pinned_beyond_budget_streams_all_pinned():
    # 3 pinned but budget 2: safety wins, all pinned stream (over budget).
    cm = CoverageManager(max_streamed=2)
    cm.observe_scanner(["P1", "P2", "P3", "A"])
    promote, _ = cm.plan(pinned={"P1", "P2", "P3"})
    _apply(cm, promote, [])
    assert {"P1", "P2", "P3"} <= set(cm.streamed_symbols())


def test_early_warning_beats_a_higher_gain_rank_dormant_name():
    # D is a lower %-gain rank than A/B/C, but it's actively setting up (dip
    # pullback); it should be promoted over the idle higher-ranked C.
    cm = CoverageManager(max_streamed=3, min_dwell_polls=0, min_samples=4)
    cm.observe_scanner(["A", "B", "C", "D"])
    _apply(cm, *cm.plan(set()))                # A,B,C streamed; D monitored
    # feed D a run-up then a pullback off the high -> "dip" early-warning
    for px in (10.0, 10.5, 11.0, 11.4, 10.9):  # +14% run then ~4% pullback
        cm.ingest_snapshot("D", px)
    assert cm.monitor["D"].warning == "dip"
    promote, demote = cm.plan(pinned=set())
    assert "D" in promote and "C" in demote    # active D in, idle C out
    _apply(cm, promote, demote)
    assert "D" in cm.streamed_symbols()


def test_a_coil_no_longer_triggers_any_early_warning():
    """The wedge's coil proxy was removed with the strategy on 2026-08-16. A
    symbol coiling near its high is no longer worth a stream slot -- nothing left
    can trade it -- so it must NOT outrank an idle streamer."""
    cm = CoverageManager(max_streamed=1, min_dwell_polls=0, min_samples=4)
    cm.observe_scanner(["A", "W"])
    _apply(cm, *cm.plan(set()))                # only A streamed (budget 1)
    # W coils: wide early swings, then a tight range near the high
    for px in (9.0, 10.0, 9.2, 9.9, 9.85, 9.9):
        cm.ingest_snapshot("W", px)
    assert cm.monitor["W"].warning is None
    promote, _demote = cm.plan(pinned=set())
    assert "W" not in promote


def test_demoted_symbol_stays_known_and_re_promotes_when_it_rewakes():
    cm = CoverageManager(max_streamed=1, min_dwell_polls=0)
    cm.observe_scanner(["A", "B"])
    _apply(cm, *cm.plan(set()))                # A streamed, B monitored
    # B outranks A -> A demotes to MONITOR (not deleted), B promotes
    cm.observe_scanner(["B", "A"])
    _apply(cm, *cm.plan(set()))
    assert "A" in cm.monitored_not_streamed()  # still known, just not streamed
    assert cm.streamed_symbols() == ["B"]
    # A re-ignites and outranks B again -> A re-promotes (re-backfill on the IB side)
    cm.observe_scanner(["A", "B"])
    promote, demote = cm.plan(set())
    assert "A" in promote and "B" in demote


def test_min_dwell_protects_a_freshly_promoted_setup():
    cm = CoverageManager(max_streamed=1, min_dwell_polls=2)
    cm.observe_scanner(["A"])
    _apply(cm, *cm.plan(set()))                # A promoted at poll 1
    cm.observe_scanner(["B", "A"])             # poll 2: B outranks A
    _promote, demote = cm.plan(set())
    assert "A" not in demote                   # within dwell -> not yet demotable


def test_demote_churn_is_capped_per_poll():
    cm = CoverageManager(max_streamed=4, min_dwell_polls=0, max_demote_per_poll=2,
                         max_promote_per_poll=10)
    cm.observe_scanner(["A", "B", "C", "D"])
    _apply(cm, *cm.plan(set()))                # A,B,C,D streamed
    # four fresh higher movers arrive; only 2 demotions allowed this poll
    cm.observe_scanner(["W", "X", "Y", "Z", "A", "B", "C", "D"])
    _promote, demote = cm.plan(set())
    assert len(demote) == 2


def test_nothing_is_ever_deleted_from_the_monitor():
    cm = CoverageManager(max_streamed=1, min_dwell_polls=0)
    cm.observe_scanner(["A", "B", "C"])
    _apply(cm, *cm.plan(set()))
    # churn through several polls; every symbol ever seen stays in the monitor
    for ranked in (["D", "E"], ["F"], ["A", "G"]):
        cm.observe_scanner(ranked)
        _apply(cm, *cm.plan(set()))
    assert {"A", "B", "C", "D", "E", "F", "G"} <= set(cm.monitor)
