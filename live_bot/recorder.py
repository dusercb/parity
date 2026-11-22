"""Session recorder (PORTING_TO_LIVE.md §11): append-only JSONL, one file per
session, capturing every input the decisions saw (bars, baselines, floats),
every intended trade, and the config/signal identity -- sufficient for the
nightly dev-side replay to re-run the engines on the RECORDED feed and diff
decisions (Class 0 must be zero)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def session_strategies(meta: dict) -> tuple[str, ...]:
    """Which engines a RECORDED session ran, read from its own session_meta.

    A replay must reproduce the session it is replaying, not today's config --
    otherwise re-running an old recording after a strategy toggle silently
    compares different strategy sets and reports the difference as a parity
    failure (or, worse, as a pass).

    Sessions from 2026-08-16 carry `strategies` directly. Earlier ones carry the
    wedge_enabled/wedge_concurrent pair instead, so the old truth table is
    reproduced here for them -- the bot no longer trades the wedge, but the
    recordings that did must stay readable. Anything older still (neither field)
    predates the wedge entirely and was dip-only."""
    listed = meta.get("strategies")
    if listed:
        return tuple(listed)
    if not meta.get("wedge_enabled"):
        return ("dip",)
    return ("dip", "wedge") if meta.get("wedge_concurrent") else ("wedge",)


class Recorder:
    def __init__(self, rec_dir: Path, session_date: str):
        rec_dir.mkdir(parents=True, exist_ok=True)
        self.path = rec_dir / f"session_{session_date}.jsonl"
        self._fh = open(self.path, "a", encoding="utf-8")

    def event(self, kind: str, **payload) -> None:
        row = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
               "kind": kind, **payload}
        self._fh.write(json.dumps(row, default=str) + "\n")
        self._fh.flush()  # observer favors durability over syscall count

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass
