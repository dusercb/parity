"""Discovery: IBKR market scanner -> CONFIRM (PORTING_TO_LIVE.md §8).

The scanner is a LOOSE pre-filter (change_perc_above well under the strategy's
threshold); CONFIRM fetches what the real gates need -- the daily-volume RVOL
baseline (same median/mean-over-lookback rules the backtester's
get_daily_volume_baseline applies, recomputed here from daily bars) and the
float (reference CSV override, then a cached yfinance lookup). Every scanner
sighting and confirmation is recorded with wall-clock timestamps: that data IS
the Stage-3 fast-mover latency / miss-rate measurement (DP-2)."""
from __future__ import annotations

import json
from pathlib import Path

from .ibcompat import ScannerSubscription, Stock, TagValue


def _build_subscription(dcfg):
    """The ScannerSubscription + its abstract filters. The filter list MUST be
    TagValue objects -- ib_insync/ib_async iterate them expecting a `.tag`
    attribute, so a plain dict raises AttributeError('dict' object has no
    attribute 'tag'), which returned nothing on every poll live. The price band
    (mirroring the strategy filter) stops a name the price gate would reject
    from ever reaching CONFIRM."""
    sub = ScannerSubscription(
        instrument=dcfg["instrument"], locationCode=dcfg["location"],
        scanCode=dcfg["scan_code"], numberOfRows=int(dcfg["scanner_rows"]))
    filters = [TagValue("changePercAbove", str(dcfg["change_perc_above"]))]
    lo, hi = dcfg.get("price_min"), dcfg.get("price_max")
    if lo is not None:
        filters.append(TagValue("priceAbove", str(lo)))
    if hi is not None:
        filters.append(TagValue("priceBelow", str(hi)))
    return sub, filters


def _rows_to_symbols(rows) -> list[str]:
    out = []
    for r in rows or []:
        try:
            out.append(r.contractDetails.contract.symbol)
        except Exception:
            continue
    return out


def open_scanner(ib, dcfg):
    """Open a PERSISTENT streaming scanner subscription (reqScannerSubscription),
    kept alive for the whole session. This is the root-cause fix for the flood
    of benign 'error 162 -- API scanner subscription cancelled' messages: the
    one-shot reqScannerData subscribes-and-cancels on every poll, and each cancel
    emits a 162 (204 of them across one morning's polling). A single streaming
    subscription cancels exactly ONCE, at session end. Returns the live
    ScanDataList (updates itself in place); read it with read_scanner()."""
    sub, filters = _build_subscription(dcfg)
    return ib.reqScannerSubscription(sub, [], filters)


def read_scanner(scan_data) -> list[str]:
    """Current symbols from a live subscription -- no request, no cancel, so no
    162. The list updates itself between polls as the IB event loop runs."""
    return _rows_to_symbols(scan_data)


def close_scanner(ib, scan_data) -> None:
    """Cancel the persistent subscription once (the only 162 of the session)."""
    if scan_data is not None:
        try:
            ib.cancelScannerSubscription(scan_data)
        except Exception:
            pass


def scanner_symbols(ib, dcfg) -> list[str]:
    """One-shot scan (subscribe->read->cancel). Retained for smoke tests / ad-hoc
    use; the live observer uses the persistent open_scanner/read_scanner path so
    it doesn't emit a 162 per poll."""
    sub, filters = _build_subscription(dcfg)
    try:
        rows = ib.reqScannerData(sub, [], filters)
    except TypeError:  # very old API: no filter-options parameter
        rows = ib.reqScannerData(sub)
    return _rows_to_symbols(rows)


def daily_volume_baseline(ib, symbol: str, scfg, session_date) -> float | None:
    """The backtester's RVOL baseline rules (config: rvol_lookback_days,
    rvol_baseline_stat median|mean, rvol_min_history_days), recomputed from
    IBKR daily bars EXCLUDING the session day. The recorded value feeds the
    parity replay, so live and replay always agree by construction.

    session_date is the ET calendar date of the session (from the Observer),
    NOT the host's local date: on a non-ET host the two can differ, and
    excluding the wrong day's bar would shift the baseline and break RVOL
    qualification parity. This project is anchored to America/New_York
    regardless of where the box physically sits."""
    try:
        c = Stock(symbol, "SMART", "USD")
        ib.qualifyContracts(c)
        bars = ib.reqHistoricalData(c, endDateTime="", durationStr=f"{int(scfg.rvol_lookback_days) + 5} D",
                                    barSizeSetting="1 day", whatToShow="TRADES",
                                    useRTH=True, formatDate=2, timeout=30)
    except Exception:
        return None
    today = session_date.isoformat() if hasattr(session_date, "isoformat") else str(session_date)
    vols = [float(b.volume) for b in bars or [] if str(b.date)[:10] != today and b.volume and b.volume > 0]
    vols = vols[-int(scfg.rvol_lookback_days):]
    if len(vols) < int(scfg.rvol_min_history_days):
        return None
    vols.sort()
    if scfg.rvol_baseline_stat == "median":
        m = len(vols) // 2
        return float(vols[m]) if len(vols) % 2 else 0.5 * (vols[m - 1] + vols[m])
    return float(sum(vols) / len(vols))


class FloatCache:
    """reference_floats.csv (authoritative override, same file the backtester
    uses) -> local yfinance cache -> live yfinance lookup."""

    def __init__(self, reference_csv: Path | None, state_dir: Path):
        self.ref: dict[str, float] = {}
        if reference_csv and reference_csv.exists():
            for line in reference_csv.read_text().splitlines()[1:]:
                parts = line.split(",")
                if len(parts) >= 2 and parts[0].strip():
                    try:
                        self.ref[parts[0].strip().upper()] = float(parts[1])
                    except ValueError:
                        pass
        state_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path = state_dir / "float_cache.json"
        try:
            self.cache = json.loads(self.cache_path.read_text())
        except Exception:
            self.cache = {}

    def get(self, symbol: str) -> float | None:
        s = symbol.upper()
        if s in self.ref:
            return self.ref[s]
        if s in self.cache:
            return self.cache[s]
        try:
            import yfinance as yf
            info = yf.Ticker(s).get_info()
            val = info.get("floatShares") or info.get("sharesOutstanding")
            val = float(val) if val else None
        except Exception:
            val = None
        self.cache[s] = val
        try:
            self.cache_path.write_text(json.dumps(self.cache))
        except Exception:
            pass
        return val
