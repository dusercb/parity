"""CLI:

  python -m live_bot serve            # DAEMON -- stays up across days (the VPS mode)
  python -m live_bot run              # observe TODAY's session, then exit
  python -m live_bot run --once       # one poll+evaluate pass, then exit (smoke test)
  python -m live_bot status           # one-line-per-session table of recent sessions
  python -m live_bot show [DATE]      # full detail of one session (default: latest)
  python -m live_bot flatten          # EMERGENCY: drop the kill-file so the running bot flattens

`serve` is what you actually want running on the VPS: it idles outside the
premarket window (no connection, no IBKR requests), wakes for each session,
records it, and sleeps until the next one -- surviving blackout days and
per-session errors without dropping the process.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .session_controller import Observer, serve

DEFAULT_CFG = Path(__file__).resolve().parent.parent / "bot_config.yaml"


def _status(cfg_path: Path, limit: int = 10) -> None:
    import yaml
    bcfg = yaml.safe_load(cfg_path.read_text())
    rec_dir = cfg_path.parent / bcfg["recording"]["dir"]
    files = sorted(rec_dir.glob("session_*.jsonl"), reverse=True)[:limit]
    if not files:
        print(f"No recordings yet in {rec_dir}")
        return
    print(f"{'session':12} {'tracked':>7} {'intended':>8} {'scanner':>7}  signal SHA")
    for f in files:
        meta, summary = {}, {}
        for line in f.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get("kind") == "session_meta":
                meta = row
            elif row.get("kind") == "summary":
                summary = row
        print(f"{f.stem.replace('session_', ''):12} {summary.get('tracked', '-'):>7} "
              f"{summary.get('intended_trades', '-'):>8} {summary.get('scanner_seen', '-'):>7}  "
              f"{str(meta.get('signal_sha', '?'))[:12]}")


def main() -> None:
    p = argparse.ArgumentParser(prog="live_bot", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["serve", "run", "status", "show", "flatten"])
    p.add_argument("date", nargs="?", default=None, help="show: session date YYYY-MM-DD (default latest)")
    p.add_argument("--once", action="store_true", help="run: single poll+evaluate pass, then exit")
    p.add_argument("--config", default=str(DEFAULT_CFG))
    args = p.parse_args()
    cfg = Path(args.config)
    if args.command == "serve":
        serve(cfg)
    elif args.command == "status":
        _status(cfg)
    elif args.command == "show":
        import yaml
        from .session_view import show_session
        bcfg = yaml.safe_load(cfg.read_text())
        show_session(cfg.parent / bcfg["recording"]["dir"], args.date)
    elif args.command == "flatten":
        # Emergency: drop the kill-file the running bot polls every loop. It
        # flattens all open positions and stops opening new ones. This does NOT
        # itself connect to the broker -- it signals the live process to act,
        # which is safer than a second client racing the first.
        import yaml
        bcfg = yaml.safe_load(cfg.read_text())
        t = bcfg.get("trading", {}) or {}
        kf = cfg.parent / (t.get("kill_file") or "STOP")
        kf.write_text(f"flatten requested {Path(__file__).name}\n")
        print(f"Kill-file written: {kf}\nThe running bot will flatten and halt new entries within a few seconds.")
        print("Delete this file to resume trading on a later run.")
    else:
        Observer(cfg).run(once=args.once)


if __name__ == "__main__":
    main()
