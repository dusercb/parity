"""Load signal_core from the pinned backtester repo -- the cross-repo parity
discipline (PORTING_TO_LIVE.md D1/§5): the bot stamps the SHA it ran with into
every recording, and REFUSES to start when the working tree's HEAD doesn't
match the configured pin ("UNPINNED-DEV" bypasses for local development)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _git(root: Path, *args: str) -> str | None:
    try:
        r = subprocess.run(["git", "-C", str(root), *args],
                           capture_output=True, text=True, timeout=15)
        return r.stdout if r.returncode == 0 else None
    except Exception:
        return None


def attach_signal_core(backtester_path: str, pin: str,
                       config_root: Path) -> tuple[object, str, bool]:
    """sys.path-attach the backtester repo, verify the pin, import signal_core.
    Returns (signal_core module, actual SHA or 'no-git', tree-is-dirty).

    The SHA alone is NOT a description of what ran. Python imports the working
    TREE, not the commit, so a modified backtester makes every recording's
    signal_sha a claim about code that never executed -- and the pin check, which
    only compares rev-parse HEAD, passes happily while unvalidated strategy code
    places real orders. On 2026-08-14 the bot stamped bd954e10 into a live
    trading session while signal_core/__init__.py, wedge.py, config.py and
    config.yaml (the parity anchor itself) were all modified in the tree.

    So dirtiness is measured and returned for the recording, and once a pin is
    set -- the deliberate 'this is validated code' statement, and the VPS
    posture -- a dirty tree is refused outright. UNPINNED-DEV still runs, because
    editing the strategy and re-running locally is the whole point of dev, but it
    warns and the recording carries the flag."""
    root = (config_root / backtester_path).resolve()
    if not (root / "signal_core" / "__init__.py").exists():
        raise SystemExit(f"signal_core not found under {root} -- check signal.backtester_path")
    sha = (_git(root, "rev-parse", "HEAD") or "").strip() or "no-git"
    # Tracked modifications only: an untracked scratch file next to the strategy
    # doesn't change what imports, and flagging it would train the alarm away.
    status = _git(root, "status", "--porcelain", "--untracked-files=no")
    edits = [ln for ln in (status or "").splitlines() if ln.strip()]
    dirty = bool(edits)
    if pin != "UNPINNED-DEV" and sha != pin:
        raise SystemExit(
            f"SIGNAL PIN MISMATCH: bot pinned to {pin[:12]} but {root} is at {sha[:12]}. "
            "Refusing to run -- decisions would not match the validated code. "
            "Update signal.pin deliberately, or check out the pinned SHA.")
    if pin != "UNPINNED-DEV" and dirty:
        listed = "\n  ".join(edits[:10])
        raise SystemExit(
            f"SIGNAL TREE DIRTY: {root} is at the pinned SHA {sha[:12]} but has "
            f"uncommitted changes:\n  {listed}\n"
            "Refusing to run -- the imported code is not the pinned code, and the "
            "recording would claim a SHA that never executed. Commit or stash them.")
    if dirty:
        print(f"WARNING: signal tree {root} has {len(edits)} uncommitted change(s); "
              f"signal_sha {sha[:12]} does NOT describe the code being imported.", flush=True)
    sys.path.insert(0, str(root))
    import signal_core  # noqa: PLC0415  (deliberate: after path attach)
    return signal_core, sha, dirty
