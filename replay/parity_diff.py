"""Nightly parity replay (PORTING_TO_LIVE.md §11): feed a session RECORDING's
bars back through the backtester engines (via signal_core, at the recorded
SHA) and diff the intended trades the engines produce now against the ones the
observer recorded live. Class-0 divergence (any difference in ticker /
entry_mode / direction / entry_time / entry_price / stop / target) must be
ZERO; ten straight zero-divergence sessions is the Stage-3 exit gate.

Run DEV-SIDE in the backtester venv:
    python replay/parity_diff.py recordings/session_2026-07-23.jsonl
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd

BOT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BOT_ROOT))

from live_bot.recorder import session_strategies  # noqa: E402 (after BOT_ROOT on path)


def load_frames(rows: list[dict]) -> dict[str, pd.DataFrame]:
    """Each symbol's session frame, rebuilt from the RECORDED feed (never a
    refetch -- live and replay must see the same inputs).

    A symbol can be dumped MORE THAN ONCE: every demote records the frame it was
    holding, and a re-promoted symbol dumps again at session end. UNION them on
    dt_et rather than letting the last one win. Re-promotion backfills to the
    session start so the later dump is normally a superset -- but if that backfill
    failed it is only a short tail, and last-wins would then silently shrink the
    replay's view of the session and manufacture divergences that never happened.
    """
    frames: dict[str, pd.DataFrame] = {}
    for r in rows:
        if r["kind"] != "frame":
            continue
        f = pd.DataFrame(r["bars"], columns=["dt_et", "open", "high", "low", "close", "volume"])
        f["dt_et"] = pd.to_datetime(f["dt_et"])
        prev = frames.get(r["symbol"])
        if prev is not None:
            f = pd.concat([prev, f], ignore_index=True)
        frames[r["symbol"]] = (f.drop_duplicates(subset="dt_et", keep="last")
                                .sort_values("dt_et").reset_index(drop=True))
    return frames


def main(path_str: str) -> int:
    rows = [json.loads(line) for line in Path(path_str).read_text(encoding="utf-8").splitlines() if line.strip()]
    meta = next((r for r in rows if r["kind"] == "session_meta"), {})
    cfg_yaml = yaml_path_from_config()
    sys.path.insert(0, str(signal_root()))
    import signal_core as sc
    cfg = sc.load_config(str(cfg_yaml))

    confirms = {r["symbol"]: r for r in rows if r["kind"] == "confirm"}
    live_intended = [r for r in rows if r["kind"] == "intended_trade"]
    frames = load_frames(rows)

    prev_rth = pd.DataFrame({"dt_et": pd.Series([], dtype="datetime64[ns, America/New_York]"),
                             "close": pd.Series([], dtype=float)})
    replay_intended: list[dict] = []
    for sym, frame in frames.items():
        c = confirms.get(sym, {})
        dav = c.get("daily_avg_volume")
        if dav is None or frame.empty:
            continue
        day = date.fromisoformat(meta.get("date", frame["dt_et"].iloc[0].date().isoformat())) \
            if "date" in meta else frame["dt_et"].iloc[0].date()
        # The engines the RECORDED session ran, from its own session_meta -- not
        # today's config. The bot itself is dip-only since 2026-08-16, but the
        # backtester still ships backtest_wedge, so an older recording that traded
        # the wedge still replays against the same engines it ran with instead of
        # silently diffing two different strategy sets.
        engine_by_name = {"dip": sc.backtest_all_trades, "wedge": sc.backtest_wedge}
        for key in session_strategies(meta):
            engine = engine_by_name[key]
            for tr in engine(sym, day, frame, frame, prev_rth, cfg,
                             float_shares=c.get("float_shares"), tick_fetcher=None,
                             daily_volume_fetcher=lambda t, d: dav) or []:
                if tr.entry_price is not None:
                    replay_intended.append({
                        "ticker": tr.ticker, "entry_mode": tr.entry_mode,
                        "direction": tr.direction, "entry_time": tr.entry_time,
                        "entry_price": tr.entry_price, "stop_price": tr.stop_price,
                        "target_price": tr.target_price})

    key = lambda r: (r["ticker"], r["entry_mode"], r["entry_time"])
    live_map = {key(r): r for r in live_intended}
    rep_map = {key(r): r for r in replay_intended}
    class0 = []
    for k in set(live_map) | set(rep_map):
        a, b = live_map.get(k), rep_map.get(k)
        if a is None or b is None:
            class0.append((k, "missing_on_" + ("replay" if b is None else "live")))
            continue
        for f in ("direction", "entry_price", "stop_price", "target_price"):
            if a.get(f) != b.get(f):
                class0.append((k, f"{f}: live={a.get(f)} replay={b.get(f)}"))
    print(f"session: {Path(path_str).name}  sha={meta.get('signal_sha', '?')[:12]}")
    print(f"live intended: {len(live_intended)}  replay intended: {len(replay_intended)}")
    if not frames:
        print("NOTE: no `frame` events in this recording (pre-first-session format) -- differ ran on 0 symbols.")
    print(f"CLASS-0 DIVERGENCES: {len(class0)}")
    for k, why in class0:
        print(f"  {k}: {why}")
    return 1 if class0 else 0


def signal_root() -> Path:
    import yaml
    bcfg = yaml.safe_load((BOT_ROOT / "bot_config.yaml").read_text())
    return (BOT_ROOT / bcfg["signal"]["backtester_path"]).resolve()


def yaml_path_from_config() -> Path:
    return signal_root() / "config.yaml"


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
