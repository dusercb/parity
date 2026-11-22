"""Per-symbol tick-by-tick stream + CLOCK-ALIGNED candle builder (Phase A of the
tiered data plan).

For a stock in one of the scarce tick-by-tick slots (open positions first), this
subscribes to reqTickByTickData('AllLast') and does two things:

  1. Builds our OWN candles from the ticks -- but on the STANDARD CLOCK
     BOUNDARIES (a 10s bar is :00-:10, :10-:20; a 1m bar is :00-:01), the exact
     grid IBKR's historical bars and every retail chart use. This matters twice
     over: the chart formations we detect must match what other traders see, and
     the candles must line up with the backtest's bars or parity breaks. The win
     over reqRealTimeBars is that a tick-built bar closes the instant the boundary
     passes -- no 5s bar-delivery lag -- and it's aligned, not arrival-timed.

  2. Keeps a rolling tick buffer with a monotonic index, so a watcher (the exit
     manager) can scan only the NEW ticks since it last looked and resolve a
     stop/target on the exact crossing print, in tick order -- the same
     first-touch resolution the backtester's _first_touch_directional does.

Framework-agnostic core: ib wiring calls on_tick(); tests feed ticks directly.
"""
from __future__ import annotations

import pandas as pd

ET = "America/New_York"


def bar_start(ts: pd.Timestamp, seconds: int) -> pd.Timestamp:
    """The clock-aligned bar this tick belongs to. pandas floor() snaps to the
    wall-clock grid (:00/:10/:20...), NOT to when we started collecting -- the
    whole point of 'matching the market norm'."""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(ET)
    else:
        ts = ts.tz_convert(ET)
    return ts.floor(f"{seconds}s")


class TickStream:
    def __init__(self, ib, contract, symbol: str, bar_seconds: int = 10, on_ticks=None):
        self.ib = ib
        self.contract = contract
        self.symbol = symbol.upper()
        self.bar_seconds = bar_seconds
        # Called with the symbol the moment new prints land, so the risk layer can
        # resolve a stop on the crossing tick instead of on the next ~2s poll.
        # Ingestion here is already real-time; without this hook the REACTION was
        # not, which on a single-digit-cent stop is a first-order cost.
        self.on_ticks = on_ticks
        self._ticker = None
        # completed clock-aligned bars: {bar_start: [o,h,l,c,v]}
        self._bars: dict[pd.Timestamp, list[float]] = {}
        self._cur_start: pd.Timestamp | None = None
        self._cur: list[float] | None = None
        # rolling tick buffer + monotonic counter for since-cursor scanning
        self._ticks: list[tuple[pd.Timestamp, float, float]] = []
        self._count = 0

    # -- IB wiring ------------------------------------------------------------
    def subscribe(self) -> None:
        self._ticker = self.ib.reqTickByTickData(self.contract, "AllLast", 0, False)
        self._ticker.updateEvent += self._on_update

    def cancel(self) -> None:
        if self._ticker is not None:
            try:
                # (contract, tickType) -- NOT the Ticker. Passing the Ticker raised
                # TypeError straight into a bare except, so no subscription was ever
                # actually released: IBKR's tick-by-tick allowance leaked all session
                # (16 acquired, 0 freed on 2026-08-07) until every further request was
                # refused with error 10190, which in turn handed positions a stream
                # that silently never delivered.
                self.ib.cancelTickByTickData(self.contract, "AllLast")
            except Exception as e:
                self.rec_error = repr(e)   # surfaced by the slot manager, never swallowed
            self._ticker = None

    def _on_update(self, ticker) -> None:
        got = False
        for t in getattr(ticker, "tickByTicks", []) or []:
            price = getattr(t, "price", None)
            if price is None or price <= 0:
                continue
            self.on_tick(t.time, float(price), float(getattr(t, "size", 0) or 0))
            got = True
        try:
            ticker.tickByTicks.clear()
        except Exception:
            pass
        # Notify AFTER the buffer is updated, so the listener sees these prints.
        # Never let a listener's failure escape into ib's dispatcher -- an
        # exception here would surface as a broken feed for every other symbol.
        if got and self.on_ticks is not None:
            try:
                self.on_ticks(self.symbol)
            except Exception:
                pass

    # -- core (testable) ------------------------------------------------------
    def on_tick(self, ts, price: float, size: float = 0.0) -> None:
        ts = pd.Timestamp(ts)
        start = bar_start(ts, self.bar_seconds)
        # roll the forming bar when the clock boundary passes; the just-completed
        # bar is banked, the new one opens at THIS tick.
        if self._cur_start is None or start > self._cur_start:
            if self._cur_start is not None:
                self._bars[self._cur_start] = self._cur
            self._cur_start = start
            self._cur = [price, price, price, price, size]   # o, h, l, c, v
        else:
            b = self._cur
            b[1] = max(b[1], price)   # high
            b[2] = min(b[2], price)   # low
            b[3] = price              # close
            b[4] += size              # volume
        self._ticks.append((ts, price, size))                # raw tick time, for exit scanning
        self._count += 1

    def frame(self) -> pd.DataFrame:
        """Completed clock-aligned bars as the backtester's frame shape. The
        forming (current) bar is EXCLUDED -- evaluating a partial candle would be
        a live-only peek the backtest could never reproduce, exactly as with the
        RealTimeBars frame."""
        if not self._bars:
            return pd.DataFrame(columns=["dt_et", "open", "high", "low", "close", "volume"])
        rows = [(ts, *vals) for ts, vals in sorted(self._bars.items())]
        return pd.DataFrame(rows, columns=["dt_et", "open", "high", "low", "close", "volume"])

    # -- tick-level level watching (the exit path) ----------------------------
    @property
    def cursor(self) -> int:
        """Current tick index -- a watcher records this and asks for crossings
        among ticks that arrive AFTER it."""
        return self._count

    def first_touch(self, since: int, direction: str, stop: float, target: float):
        """Scan ticks arriving after index `since` for the FIRST stop/target
        crossing, in tick order -- the live mirror of the backtest's
        _first_touch_directional. Returns (kind, time, price, new_cursor) where
        kind is 'stop'|'target', or (None, None, None, new_cursor) if neither
        crossed yet. Direction-aware: long stop sits below/target above; short
        mirrored. The buffer is never trimmed, so tick index == cursor and new
        ticks are simply self._ticks[since:]."""
        for i in range(max(since, 0), len(self._ticks)):
            t_time, price, _ = self._ticks[i]
            if direction == "long":
                if price <= stop:
                    return "stop", t_time, price, self._count
                if price >= target:
                    return "target", t_time, price, self._count
            else:
                if price >= stop:
                    return "stop", t_time, price, self._count
                if price <= target:
                    return "target", t_time, price, self._count
        return None, None, None, self._count

    def last_price(self) -> float | None:
        return self._ticks[-1][1] if self._ticks else None

    def forming_bar(self) -> dict | None:
        """The CURRENT partial bar (the one still accumulating ticks), as a frame
        row dict. This is the live 'current bar' the trigger-watch hands to
        evaluate_touch on a cross -- its high-so-far reflects the latest tick, so
        'has the trigger been crossed' is a causal, real read. None before any
        tick has arrived."""
        if self._cur is None or self._cur_start is None:
            return None
        o, h, low_, c, v = self._cur
        return {"dt_et": self._cur_start, "open": o, "high": h, "low": low_, "close": c, "volume": v}
