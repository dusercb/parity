"""The recorded signal_sha must describe the code that actually ran.

Python imports the working TREE, not the commit. A pin check that only compares
`git rev-parse HEAD` therefore passes while modified strategy code places real
orders, and every recording from that session claims a SHA that never executed.
On 2026-08-14 the bot stamped bd954e10 into a live trading session while
signal_core/__init__.py, wedge.py, config.py and config.yaml -- the parity anchor
itself -- were all modified in the backtester tree.
"""
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot.signal_path import attach_signal_core


def _git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args],
                          capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture
def bt(tmp_path):
    """A minimal committed backtester repo with an importable signal_core."""
    root = tmp_path / "bt"
    (root / "signal_core").mkdir(parents=True)
    (root / "signal_core" / "__init__.py").write_text("VALUE = 1\n")
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@t.t")
    _git(root, "config", "user.name", "t")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "base")
    yield root, _git(root, "rev-parse", "HEAD")
    sys.modules.pop("signal_core", None)
    while str(root) in sys.path:
        sys.path.remove(str(root))


def test_a_dirty_tree_is_refused_when_pinned(bt):
    root, sha = bt
    (root / "signal_core" / "__init__.py").write_text("VALUE = 2\n")   # unvalidated edit
    with pytest.raises(SystemExit) as e:
        attach_signal_core("bt", sha, root.parent)
    assert "DIRTY" in str(e.value)
    assert "signal_core/__init__.py" in str(e.value), "the operator needs to see WHICH files"


def test_a_clean_pinned_tree_runs_and_reports_clean(bt):
    root, sha = bt
    _sc, got, dirty = attach_signal_core("bt", sha, root.parent)
    assert got == sha and dirty is False


def test_dev_mode_still_runs_dirty_but_flags_it(bt):
    """Editing the strategy and re-running locally is the point of dev mode, so
    UNPINNED-DEV must not block -- but the recording has to carry the flag."""
    root, sha = bt
    (root / "signal_core" / "__init__.py").write_text("VALUE = 3\n")
    _sc, got, dirty = attach_signal_core("bt", "UNPINNED-DEV", root.parent)
    assert got == sha, "the SHA is still recorded"
    assert dirty is True, "but it is marked as not describing what ran"


def test_a_mismatched_pin_is_still_refused(bt):
    root, sha = bt
    with pytest.raises(SystemExit) as e:
        attach_signal_core("bt", "0" * 40, root.parent)
    assert "PIN MISMATCH" in str(e.value)


def test_an_untracked_scratch_file_is_not_dirtiness(bt):
    """Flagging scratch files next to the strategy would train the alarm away;
    they do not change what gets imported."""
    root, sha = bt
    (root / "notes.txt").write_text("scratch\n")
    _sc, _got, dirty = attach_signal_core("bt", sha, root.parent)
    assert dirty is False
