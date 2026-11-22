"""Live bars per tracked symbol (PORTING_TO_LIVE.md §7 Tier B): one 10s
HISTORICAL BACKFILL at confirm (late discovery still reconstructs the exact
session-so-far the backtester would have seen -- HOD, pole, coil and all),
then 5-second RealTimeBars paired into 10s bars, folded into the SAME frame
shape the backtester's split_sessions produces (dt_et/open/high/low/close/
volume). The engines then run on frames indistinguishable from backtest input,
which is what makes decision parity even possible."""
from __future__ import annotations

from datetime import datetime, time as dtime

import pandas as pd

from .ibcompat import Stock

ET = "America/New_York"


class SymbolFeed:
    def __init__(self, ib, symbol: str, session_start: dtime, recorder, session_date=None):
        self.ib = ib
        self.symbol = symbol
        self.session_start = session_start
        # The ET calendar date this feed belongs to. Bars are filtered on it as
        # well as on time-of-day: the backfill reaches back far enough to cover
        # the whole window, which on an early-session call crosses midnight, and
        # a time-of-day-only filter would then let YESTERDAY's evening bars
        # (also >= 04:00 by clock) into today's frame.
        self.session_date = session_date or pd.Timestamp.now(tz=ET).date()
        self.rec = recorder
        self.contract = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(self.contract)
        self._bars10: dict[pd.Timestamp, list[float]] = {}   # start_ts -> [o,h,l,c,v]
        self._rtb = None
        self.new_data = False

    # -- backfill -----------------------------------------------------------
    def _backfill_seconds(self, now=None) -> int:
        """Enough seconds to reach the session start from NOW, plus a buffer.

        A fixed 3h window silently truncated late discoveries: a symbol confirmed
        at 09:00 would have started its frame at 06:00, so premarket_open (the
        frame's first bar) and therefore gain% would differ from the backtest,
        which always splits the session from 04:00 -- a Class-0 parity break
        disguised as a data-fetch detail."""
        now = now or pd.Timestamp.now(tz=ET)
        start = pd.Timestamp.combine(now.date(), self.session_start).tz_localize(ET)
        need = int((now - start).total_seconds()) + 900        # 15-min buffer
        return max(3600, min(need, 28800))                     # 1h floor, 8h ceiling

    def backfill(self) -> int:
        """Fetch today's premarket-so-far as 10s bars (one paced request), back
        to the session start so a late discovery still reconstructs the exact
        session-so-far the backtester would have seen."""
        try:
            bars = self.ib.reqHistoricalData(
                self.contract, endDateTime="", durationStr=f"{self._backfill_seconds()} S",
                barSizeSetting="10 secs", whatToShow="TRADES", useRTH=False,
                formatDate=2, timeout=45)
        except Exception as e:
            self.rec.event("backfill_error", symbol=self.symbol, error=repr(e))
            return 0
        n = 0
        for b in bars or []:
            ts = pd.Timestamp(b.date).tz_convert(ET)
            if ts.date() != self.session_date or ts.time() < self.session_start:
                continue
            self._bars10[ts] = [float(b.open), float(b.high), float(b.low),
                                float(b.close), float(b.volume)]
            n += 1
        self.new_data = n > 0
        self.rec.event("backfill", symbol=self.symbol, bars=n,
                       seconds_requested=self._backfill_seconds())
        return n

    # -- streaming ----------------------------------------------------------
    def start_stream(self) -> None:
        self._rtb = self.ib.reqRealTimeBars(self.contract, 5, "TRADES", useRTH=False)
        self._rtb.updateEvent += self._on_bar

    def _on_bar(self, bars, has_new: bool) -> None:
        if not has_new or not bars:
            return
        b = bars[-1]
        ts = pd.Timestamp(b.time).tz_convert(ET)
        if ts.date() != self.session_date or ts.time() < self.session_start:
            return
        # Pair 5s bars into the 10s frame: floor to the 10s boundary, merge.
        key = ts.floor("10s")
        row = self._bars10.get(key)
        o, h, low_, c, v = float(b.open_), float(b.high), float(b.low), float(b.close), float(b.volume)
        if row is None:
            self._bars10[key] = [o, h, low_, c, v]
        else:
            row[1] = max(row[1], h)
            row[2] = min(row[2], low_)
            row[3] = c
            row[4] += v
        self.new_data = True

    def stop_stream(self) -> None:
        if self._rtb is not None:
            try:
                self.ib.cancelRealTimeBars(self._rtb)
            except Exception:
                pass
            self._rtb = None

    # -- frame --------------------------------------------------------------
    def frame(self, upto: datetime | None = None) -> pd.DataFrame:
        """The session-so-far as the backtester's premarket frame. Only bars
        whose 10s window has fully CLOSED are included (a forming bar's
        high/low/close are still moving -- evaluating it would be a live-only
        peek the backtest replay could never reproduce)."""
        if not self._bars10:
            return pd.DataFrame(columns=["dt_et", "open", "high", "low", "close", "volume"])
        # Accept naive or tz-aware `upto`. (The previous form,
        # pd.Timestamp(x, tz=ET), raises whenever x is ALREADY tz-aware -- which
        # includes the default upto=None path, so every production call to
        # frame() crashed. It was never hit offline because the e2e built its
        # frame directly; the unit tests now cover both branches.)
        now = pd.Timestamp(upto) if upto is not None else pd.Timestamp.now(tz=ET)
        now = now.tz_localize(ET) if now.tzinfo is None else now.tz_convert(ET)
        cutoff = now - pd.Timedelta(seconds=10)
        rows = [(ts, *vals) for ts, vals in sorted(self._bars10.items()) if ts <= cutoff]
        return pd.DataFrame(rows, columns=["dt_et", "open", "high", "low", "close", "volume"])
