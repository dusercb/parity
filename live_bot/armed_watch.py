"""Live trigger-watch (Phase B, bot side): pre-arm the buy parameters, then fire
the entry with minimal latency the instant price crosses the trigger.

Two halves, both delegating to signal_core so the decision matches the backtest:

  1. ARM -- each poll (bar close) the engine runs on the accumulated frame with
     signal_core's on_armed hook; `capture()` records the latest ArmedSetup per
     symbol (trigger + frozen HOD/pullback structure). No decision here, just
     "here is the level to watch."

  2. FIRE -- on every loop while a symbol is armed, `check_cross()` asks its tick
     stream whether price has crossed the armed trigger since we last looked. On
     the crossing PRINT it builds the live forming-bar context and calls
     signal_core.evaluate_touch -- the SAME gate stack + fill/stop/target the
     backtest uses -- and returns its TouchDecision. A "fire" is handed to the
     order manager immediately, sub-second, instead of waiting ~11s for the 10s
     bar to close and the bar-detected path to notice.

The bar-detected path in signal_runtime still runs; the order manager's
idempotency (one order per ticker/date/strategy/setup) means whichever fires
first wins and the other is a no-op -- so the tick-watch simply gets there
first, and nothing double-enters.
"""
from __future__ import annotations

import pandas as pd

from .live_data import ET


def _ticks_fetcher(stream):
    """Wrap a TickStream's buffered ticks as the (ticker, start, end) -> DataFrame
    fetcher evaluate_touch expects, so it tick-RESOLVES the entry fill at the
    crossing print (matching the backtest's fill) instead of a bar estimate."""
    def fetch(ticker, start, end):
        start = pd.Timestamp(start); end = pd.Timestamp(end)
        if start.tzinfo is None:
            start = start.tz_localize(ET)
        if end.tzinfo is None:
            end = end.tz_localize(ET)
        rows = [(t, p) for (t, p, _sz) in stream._ticks
                if start <= (t.tz_convert(ET) if t.tzinfo else t.tz_localize(ET)) <= end]
        return pd.DataFrame({"time": [t for t, _ in rows], "price": [p for _, p in rows]})
    return fetch


class LiveArmedWatch:
    def __init__(self, sc, cfg):
        self.sc = sc                 # signal_core module
        self.cfg = cfg               # backtester AppConfig (strategy/data)
        self.armed: dict[str, object] = {}      # symbol -> latest ArmedSetup
        self._crossed: set[str] = set()          # symbols already fired this session
        s = cfg.strategy
        self._earliest = self.sc.parse_hhmm(s.earliest_entry_time, "earliest_entry_time", "cfg") if s.earliest_entry_time else None
        self._latest = self.sc.parse_hhmm(s.latest_entry_time, "latest_entry_time", "cfg") if s.latest_entry_time else None

    # -- arm ------------------------------------------------------------------
    def capture(self, symbol: str):
        """An on_armed callback for signal_core.backtest_all_trades that records
        the LATEST armed setup for `symbol` (the hook fires once per armed bar;
        we keep the most recent)."""
        def _cb(a):
            self.armed[symbol] = a
        return _cb

    def disarm(self, symbol: str) -> None:
        self.armed.pop(symbol, None)

    def is_armed(self, symbol: str) -> bool:
        return symbol in self.armed

    # -- fire -----------------------------------------------------------------
    def check_cross(self, symbol: str, frame: pd.DataFrame, forming_bar: dict,
                    daily_avg_volume: float, stream) -> object | None:
        """If `symbol` is armed and `forming_bar` has crossed the armed trigger,
        run signal_core.evaluate_touch on the live forming-bar context and return
        its TouchDecision; None when not armed, already fired, or not yet crossed.
        `frame` is the completed-bar frame the arming ran on (same index basis);
        `forming_bar` is the current partial bar from the tick stream."""
        a = self.armed.get(symbol)
        if a is None or symbol in self._crossed:
            return None
        high = float(forming_bar["high"])
        # direction is long for the dip; the trigger is an upside break
        if high < a.trigger:
            return None

        import datetime as _dt
        _t = lambda hm: _dt.time(*hm) if hm else None  # noqa: E731
        earliest_t = _t(self._earliest)
        latest_t = _t(self._latest)

        # Extended frame = the completed bars the setup armed on + the forming bar
        # as the current bar. concat(ignore_index=True) keeps the RangeIndex the
        # engine used, so the ArmedSetup's hod_idx/pullback_low_idx stay valid.
        ext = pd.concat([frame.reset_index(drop=True), pd.DataFrame([forming_bar])], ignore_index=True)
        i = len(ext) - 1
        one_min = self.sc.add_indicators(
            self.sc.add_rvol(self.sc.build_1m_bars(ext), daily_avg_volume),
            self.cfg.strategy.macd_fast, self.cfg.strategy.macd_slow, self.cfg.strategy.macd_signal)
        move_bars = ext.iloc[a.hod_idx:i + 1]
        row = ext.iloc[i]
        bar_len = pd.Timedelta(seconds=10)
        latency = pd.Timedelta(seconds=self.cfg.strategy.order_latency_seconds) if self.cfg.strategy.order_latency_seconds else None
        decision = self.sc.evaluate_touch(
            ticker=symbol, i=i, row=row, high=high, low=float(forming_bar["low"]),
            open_=float(forming_bar["open"]), trigger=a.trigger, entry_limit=a.entry_limit,
            premarket=ext, one_min=one_min, move_bars=move_bars, hod_idx=a.hod_idx,
            pullback_low=a.pullback_low, pullback_low_idx=a.pullback_low_idx,
            armed_hod=a.armed_hod, s=self.cfg.strategy, prior_win_entry=a.prior_win_entry,
            earliest_t=earliest_t, latest_t=latest_t,
            tick_fetcher=_ticks_fetcher(stream) if stream is not None else None,
            bid_ask_fetcher=None, latency=latency, bar_len=bar_len)
        if decision.kind == "fire":
            self._crossed.add(symbol)   # one entry per setup; bar path is then a no-op
        return decision
