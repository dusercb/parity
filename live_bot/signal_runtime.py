"""Signal evaluation (PORTING_TO_LIVE.md §6 signal_runtime): ALL decisions are
made by re-running the backtester's own engine (signal_core.backtest_all_trades)
over the live-accumulated session frame -- the replay-on-accumulated-frame form.
Slower than an incremental state machine, but parity is BY CONSTRUCTION
(identical code path to the backtest), which is the entire point of the observer
stage; the incremental rewrite, if ever needed for latency, must diff zero
against this.

SINGLE STRATEGY since 2026-08-16. This used to dispatch through an engine table
driven by signal_core.active_strategies(); with the wedge gone that table had one
row, so the dispatch is inlined. Two costs went with it, both paid per symbol per
poll: a second full engine pass over the whole session frame, and the
complete_tf_frame resample that trimmed the frame to whole wedge candles (the dip
scans the native 10s bars, which frame() already delivers complete, so it never
needed trimming). Observer.__init__ refuses to start if the anchor ever selects
anything but the dip, so the removed dispatch cannot silently be needed again.

An "intended trade" (entry_price set) is deduped on (ticker, date, entry_mode,
entry_time) and logged ONCE with two timestamps: the entry_time the engine
assigns (when the backtester says the fill happens) and detected_wall (when
the observer actually saw it). detected - entry = the observer's decision
latency, the number Stage 3 exists to measure."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

ENGINE = "dip"                # the recorded engine name for every intended trade


class SignalRuntime:
    def __init__(self, sc, app_config, recorder, watch=None):
        self.sc = sc                      # the signal_core module
        self.cfg = app_config             # the BACKTESTER AppConfig (parity anchor)
        self.rec = recorder
        self.watch = watch                # LiveArmedWatch (trading mode) or None
        self.seen: set[tuple] = set()
        self.intended: list[dict] = []

    def evaluate(self, symbol: str, day: date, frame: pd.DataFrame,
                 daily_avg_volume: float | None, float_shares: float | None) -> None:
        if frame.empty or daily_avg_volume is None:
            return
        prev_rth = pd.DataFrame({"dt_et": pd.Series([], dtype=f"datetime64[ns, America/New_York]"),
                                 "close": pd.Series([], dtype=float)})
        dvf = lambda t, d: daily_avg_volume  # recorded value; replay uses the same
        # Re-derive the armed setup from THIS poll's completed frame: disarm first
        # so a setup that has since invalidated doesn't linger, then let the
        # engine's on_armed hook re-arm it if it's still valid. on_armed reports
        # the dip's ArmedSetup (a trigger + frozen HOD/pullback) to its
        # trigger-watch.
        if self.watch is not None:
            self.watch.disarm(symbol)
            extra = {"on_armed": self.watch.capture(symbol)}
        else:
            extra = {}
        # The dip scans chart_timeframe = the native 10s bars, which frame()
        # already delivers complete (the forming bar is dropped there), so the
        # frame is handed over untouched -- no resample on this path.
        try:
            results = self.sc.backtest_all_trades(
                symbol, day, frame, frame, prev_rth, self.cfg,
                float_shares=float_shares, tick_fetcher=None,
                daily_volume_fetcher=dvf, **extra)
        except Exception as e:
            self.rec.event("engine_error", symbol=symbol, engine=ENGINE, error=repr(e))
            return
        for tr in results or []:
            if tr.entry_price is None:
                continue
            key = (tr.ticker, tr.date, tr.entry_mode, tr.entry_time)
            if key in self.seen:
                continue
            self.seen.add(key)
            row = {
                "ticker": tr.ticker, "date": tr.date, "engine": ENGINE,
                "entry_mode": tr.entry_mode, "direction": tr.direction,
                "setup_index": tr.setup_index, "entry_time": tr.entry_time,
                "entry_price": tr.entry_price, "stop_price": tr.stop_price,
                "target_price": tr.target_price, "trigger_price": tr.trigger_price,
                "detected_wall": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "daily_avg_volume": daily_avg_volume, "float_shares": float_shares,
                # volume_at_entry feeds the order manager's session-volume
                # participation cap; without it threaded, live sizing would
                # silently skip a cap the backtest applied.
                "volume_at_entry": tr.volume_at_entry,
                "result_so_far": tr.result,
            }
            self.intended.append(row)
            self.rec.event("intended_trade", **row)
