"""Human-readable view of one recorded observer session.

The observer holds no positions (Stage 3), so a session's "results" are its
DECISIONS: which movers it discovered, which it tracked, and the intended
trades it would have taken -- plus the wall-clock detection latency and a real-
error count. Actual P&L results arrive at Stage 4, when order code exists.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .recorder import session_strategies

ET = ZoneInfo("America/New_York")


def _load(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _et(iso_utc: str) -> str:
    try:
        return datetime.fromisoformat(iso_utc).astimezone(ET).strftime("%H:%M:%S")
    except Exception:
        return iso_utc


def show_session(rec_dir: Path, session_date: str | None) -> None:
    files = sorted(rec_dir.glob("session_*.jsonl"))
    if not files:
        print(f"No recordings in {rec_dir}")
        return
    path = (rec_dir / f"session_{session_date}.jsonl") if session_date else files[-1]
    if not path.exists():
        print(f"No recording for {session_date}. Have: {[f.stem.replace('session_', '') for f in files]}")
        return
    rows = _load(path)
    meta = next((r for r in rows if r["kind"] == "session_meta"), {})
    intended = [r for r in rows if r["kind"] == "intended_trade"]
    confirms = [r for r in rows if r["kind"] == "confirm"]
    seen = [r for r in rows if r["kind"] == "scanner_seen"]
    real_errs = [r for r in rows if (r["kind"] == "ib_message" and not r.get("benign"))
                 or r["kind"] in ("scanner_error", "backfill_error", "engine_error")]
    reconnects = [r for r in rows if r["kind"] == "reconnected"]

    date = path.stem.replace("session_", "")
    bar = "=" * 68
    print(bar)
    print(f" SESSION {date}   signal {str(meta.get('signal_sha', '?'))[:12]}   "
          f"readonly={meta.get('readonly')}")
    print(f" strategies: {' + '.join(session_strategies(meta))}")
    print(bar)
    print(f" scanner: {len(seen)} movers seen, {len(confirms)} tracked")
    print(f" real errors: {len(real_errs)}   reconnects: {len(reconnects)}"
          + ("   [benign IB heartbeats are hidden]" if not real_errs else ""))

    if intended:
        print(f"\n INTENDED TRADES (what the strategy would have taken): {len(intended)}")
        print(f" {'ticker':<8}{'engine':<15}{'dir':<6}{'entry':>7}{'stop':>7}{'target':>8}"
              f"{'signal(ET)':>12}{'lag':>6}")
        for t in intended:
            sig = (t.get("entry_time") or "")[11:19]
            lag = ""
            try:
                s = datetime.fromisoformat(t["entry_time"]).astimezone(ET)
                d = datetime.fromisoformat(t["detected_wall"]).astimezone(ET)
                lag = f"{(d - s).total_seconds():.0f}s"
            except Exception:
                pass
            print(f" {t['ticker']:<8}{t['engine']:<15}{(t.get('direction') or 'long'):<6}"
                  f"{t['entry_price']:>7}{t['stop_price']:>7}{t['target_price']:>8}{sig:>12}{lag:>6}")
    else:
        print("\n No intended trades this session (no candidate cleared all gates).")

    if real_errs:
        print("\n *** REAL ERRORS -- look at these: ***")
        for e in real_errs[:12]:
            print(f"   {e.get('kind')}  {e.get('code', '')} {str(e.get('msg') or e.get('error'))[:70]}")

    print(f"\n recording: {path}")
    print(" parity check:  python replay/parity_diff.py " + str(path).replace('\\', '/'))
