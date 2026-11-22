"""Tiered mover-coverage (PORTING_TO_LIVE.md §7 Tier C/B/A) -- the PURE decision
core that decides which symbols get a scarce RealTimeBars stream, so the wide
mover universe is never lost to a flat cap.

THE PROBLEM IT REPLACES. The old flat `max_tracked` confirmed the first N scanner
symbols and STREAMED them for the whole session: first-come-first-served, no
eviction, no re-ranking. A late igniter arriving after N earlier (often already
faded) names was never tracked -- lossy by construction.

THE FIX -- three tiers, movement BOTH ways, nothing ever forgotten:
  * Tier C (monitor): every scanner mover, recorded once, kept ALL session. No
    RealTimeBars line. Refreshed with cheap snapshot quotes.
  * Tier B (stream): `max_streamed` RealTimeBars feeds, filled by PRIORITY --
    pinned (open position / armed) FIRST, then a strategy EARLY-WARNING (a dip
    pullback spotted from snapshots), then %-gain rank. A
    promotion backfills the full session-so-far, so a promoted symbol is
    evaluated exactly as if it had streamed from 04:00.
  * Tier A (tick): armed / positioned (SlotManager, elsewhere).
A dormant streamer demotes back to monitor -- NEVER if pinned -- and re-promotes,
re-backfilling exact state, the moment it wakes. Awareness is lossless; the only
residual is scanner coverage + promotion latency, both MEASURED, not designed-in.

The early-warning is deliberately a cheap PROXY (snapshot geometry), not the real
detector: fitting the dip's HOD->pullback->break properly needs
bars, and bars for the whole monitor set are pacing-limited. So the proxy only
decides WHEN to spend a stream slot; the authoritative decision still runs in the
engine on the streamed bars, so parity is untouched.

PURE: no IB. Given scanner ranks + snapshot samples + the pinned set + the
currently-streamed set, it returns (to_promote, to_demote). The session
controller executes the IB side (backfill+stream / cancel) and reports back via
mark_promoted / mark_demoted so this core's state always mirrors reality.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class MonitorEntry:
    """One symbol the scanner has surfaced this session. Never deleted -- a
    demoted symbol stays here so re-promotion can rebuild it; that is what makes
    awareness lossless."""
    symbol: str
    first_poll: int
    rank: float = math.inf         # index in the current scanner list; inf = off-scanner now
    samples: list[float] = field(default_factory=list)  # recent snapshot last-prices
    high: float = 0.0              # running session high seen via snapshots
    gain_pct: float | None = None  # if the caller has it (else rank carries momentum)
    streamed: bool = False
    promoted_poll: int | None = None
    warning: str | None = None     # "dip" | None -- whether the early-warning fired


class CoverageManager:
    def __init__(self, max_streamed: int, *, min_dwell_polls: int = 2,
                 max_promote_per_poll: int = 10, max_demote_per_poll: int = 3,
                 max_samples: int = 10,
                 dip_min_pullback: float = 0.02, dip_max_pullback: float = 0.20,
                 dip_min_run: float = 0.03,
                 min_samples: int = 4):
        self.max_streamed = int(max_streamed)
        self.min_dwell = int(min_dwell_polls)
        self.max_promote = int(max_promote_per_poll)
        self.max_demote = int(max_demote_per_poll)
        self.max_samples = int(max_samples)
        self.dip_min_pullback = dip_min_pullback
        self.dip_max_pullback = dip_max_pullback
        self.dip_min_run = dip_min_run
        self.min_samples = int(min_samples)
        self.monitor: dict[str, MonitorEntry] = {}
        self.poll = 0

    # -- Tier C intake --------------------------------------------------------
    def observe_scanner(self, ranked: list[str]) -> None:
        """Fold one scanner read (symbols in %-gain rank order) into the monitor.
        New symbols are added; existing ones get their current rank; symbols no
        longer on the scanner keep their entry but go to rank=inf (candidates for
        demotion, NOT deletion -- they stay known)."""
        self.poll += 1
        present = set(ranked)
        for i, s in enumerate(ranked):
            e = self.monitor.get(s)
            if e is None:
                self.monitor[s] = MonitorEntry(symbol=s, first_poll=self.poll, rank=float(i))
            else:
                e.rank = float(i)
        for s, e in self.monitor.items():
            if s not in present:
                e.rank = math.inf

    def ingest_snapshot(self, symbol: str, last: float, gain_pct: float | None = None) -> None:
        """A cheap snapshot last-price for a monitored symbol. Updates the running
        high and the recent-sample window, then recomputes the strategy early-
        warning. Unknown symbols are ignored (only the monitor set is snapshotted)."""
        e = self.monitor.get(symbol)
        if e is None or last is None or last <= 0:
            return
        e.samples.append(float(last))
        if len(e.samples) > self.max_samples:
            e.samples = e.samples[-self.max_samples:]
        e.high = max(e.high, float(last))
        if gain_pct is not None:
            e.gain_pct = gain_pct
        e.warning = self._early_warning(e)

    def _early_warning(self, e: MonitorEntry) -> str | None:
        """The dip promotion proxy, from snapshot geometry only: ran up, then
        pulled back from the running high by a modest fraction (the pullback the
        dip strategy enters on).

        Loose by design: a false positive costs one promotion (cheap, demotes
        later); a false negative risks a missed setup, which we won't accept.

        The wedge's coil proxy lived here too until 2026-08-16 -- a second pass
        over the same samples (halving, two min/max scans, a near-high test) run
        for every monitored symbol on every poll, to promote symbols for an engine
        that no longer exists."""
        s = e.samples
        if len(s) < self.min_samples or e.high <= 0:
            return None
        run = (e.high - s[0]) / s[0] if s[0] > 0 else 0.0
        pullback = (e.high - s[-1]) / e.high
        if run >= self.dip_min_run and self.dip_min_pullback <= pullback <= self.dip_max_pullback:
            return "dip"
        return None

    # -- the plan -------------------------------------------------------------
    def _priority(self, e: MonitorEntry, pinned: set[str]) -> tuple:
        """Higher tuple = keep/stream sooner. Tier 3 pinned (sacred) > 2 early-
        warning active > 1 on-scanner (by rank, lower index better) > 0 off-scanner
        idle. Ties break toward the lower scanner rank then earlier arrival."""
        if e.symbol in pinned:
            tier = 3
        elif e.warning is not None:
            tier = 2
        elif math.isfinite(e.rank):
            tier = 1
        else:
            tier = 0
        return (tier, -e.rank if math.isfinite(e.rank) else -1e9, -e.first_poll)

    def plan(self, pinned: set[str]) -> tuple[list[str], list[str]]:
        """Return (to_promote, to_demote) for THIS poll, guardrailed.

        Pinned symbols are ALWAYS streamed (a position's client-side stop needs
        its feed live -- safety, not preference), even past max_streamed. The rest
        of the budget goes to the highest-priority monitored symbols. Demotions
        never touch a pinned symbol, respect a minimum dwell (so a freshly
        promoted setup gets time to form), and are capped per poll to bound the
        state churn / re-backfill pacing cost."""
        # A pinned symbol might not be in the monitor yet (a brand-new position);
        # make sure it exists so it can be streamed.
        for s in pinned:
            if s not in self.monitor:
                self.monitor[s] = MonitorEntry(symbol=s, first_poll=self.poll, rank=math.inf)

        ordered = sorted(self.monitor.values(), key=lambda e: self._priority(e, pinned), reverse=True)
        pinned_entries = [e for e in ordered if e.symbol in pinned]
        rest = [e for e in ordered if e.symbol not in pinned]
        budget = max(self.max_streamed - len(pinned_entries), 0)
        desired = {e.symbol for e in pinned_entries} | {e.symbol for e in rest[:budget]}

        streamed = {s for s, e in self.monitor.items() if e.streamed}

        # Promotions: pinned first (mandatory, uncapped), then discretionary up to
        # the per-poll cap so the budget fills over a few polls rather than a
        # single backfill burst.
        want_promote = [s for s in desired if s not in streamed]
        pinned_promote = [s for s in want_promote if s in pinned]
        disc_promote = [s for s in want_promote if s not in pinned][:self.max_promote]
        to_promote = pinned_promote + disc_promote

        # Demotions: streamed but no longer desired, never pinned, past the dwell,
        # capped per poll. Weakest priority first (they sort last in `ordered`).
        want_demote = [e.symbol for e in reversed(ordered)
                       if e.streamed and e.symbol not in desired and e.symbol not in pinned]
        to_demote = []
        for s in want_demote:
            e = self.monitor[s]
            if e.promoted_poll is not None and (self.poll - e.promoted_poll) < self.min_dwell:
                continue
            to_demote.append(s)
            if len(to_demote) >= self.max_demote:
                break
        return to_promote, to_demote

    # -- state sync (called by the controller after the IB action succeeds) ---
    def mark_promoted(self, symbol: str) -> None:
        e = self.monitor.get(symbol)
        if e is not None:
            e.streamed = True
            e.promoted_poll = self.poll

    def mark_demoted(self, symbol: str) -> None:
        e = self.monitor.get(symbol)
        if e is not None:
            e.streamed = False   # entry stays in the monitor -- awareness is never lost

    # -- introspection --------------------------------------------------------
    def streamed_symbols(self) -> list[str]:
        return [s for s, e in self.monitor.items() if e.streamed]

    def monitored_not_streamed(self) -> list[str]:
        return [s for s, e in self.monitor.items() if not e.streamed]

    def snapshot_targets(self, k: int) -> list[str]:
        """The k best monitored-but-not-streamed symbols to spend cheap snapshot
        quotes on this poll -- highest %-gain rank first, since those are the
        likeliest next promotions. Bounds the snapshot request count so the
        early-warning stays cheap."""
        ents = [e for e in self.monitor.values() if not e.streamed]
        ents.sort(key=lambda e: (e.rank, e.first_poll))
        return [e.symbol for e in ents[:max(k, 0)]]
