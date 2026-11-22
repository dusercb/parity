"""open_positions.json is written from latency-critical paths, so it must be
written only when it actually changes.

_settle runs inside an ib event callback, _fold_trail calls update() for every
trailing position on every ~2s pass, and reconcile ran an unconditional flush
every 5 seconds for the whole session. Each was a synchronous write+rename
executed inside ib's dispatcher, which delays not just that symbol but every
other symbol's tick processing queued behind it.

The assertions below delete the file and check it is not recreated: a flush that
writes nothing cannot touch the disk at all.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot.position_state import Position, PositionBook


class _Rec:
    def __init__(self):
        self.events = []

    def event(self, kind, **p):
        self.events.append((kind, p))


def _pos(ticker="WETO", shares=242):
    return Position(ticker=ticker, entry_mode="wedge_break", direction="long",
                    date="2026-08-14", shares=shares, entry_price=8.773,
                    stop_price=8.34, target_price=9.45, remaining=shares,
                    opened_at="2026-08-14T10:40:40-04:00")


def test_a_real_change_is_written(tmp_path):
    book = PositionBook(tmp_path)
    book.add(_pos())
    assert book.path.exists()
    assert "WETO" in book.path.read_text()


def test_an_unchanged_update_does_not_touch_the_disk(tmp_path):
    book = PositionBook(tmp_path)
    p = _pos()
    book.add(p)
    book.path.unlink()
    book.update(p)                       # _fold_trail does exactly this every pass
    assert not book.path.exists(), "an unchanged flush must not write"


def test_a_changed_field_is_still_written(tmp_path):
    """The skip must be driven by content, never by 'we already wrote once' --
    a ratcheted stop that never reached disk is an unprotected position."""
    book = PositionBook(tmp_path)
    p = _pos()
    book.add(p)
    book.path.unlink()
    p.stop_price = 8.55                  # the trail ratchets
    book.update(p)
    assert book.path.exists() and "8.55" in book.path.read_text()


def test_reconcile_that_changes_nothing_does_not_touch_the_disk(tmp_path):
    book = PositionBook(tmp_path)
    p = _pos()
    book.add(p)
    book.path.unlink()
    assert book.reconcile({"WETO": 242}, _Rec()) == []
    assert not book.path.exists(), "an agreeing reconcile ran every 5s all session"


def test_reconcile_that_adopts_broker_qty_is_written(tmp_path):
    book = PositionBook(tmp_path)
    p = _pos()
    book.add(p)
    book.path.unlink()
    alarms = book.reconcile({"WETO": 200}, _Rec())
    assert alarms and book.path.exists()
    assert '"remaining": 200' in book.path.read_text()


def test_reconcile_that_drops_a_vanished_position_is_written(tmp_path):
    book = PositionBook(tmp_path)
    book.add(_pos())
    book.path.unlink()
    assert book.reconcile({}, _Rec())
    assert book.path.exists() and book.path.read_text().strip() == "{}"


def test_a_reloaded_book_does_not_rewrite_identical_state(tmp_path):
    """Adoption on restart must not immediately rewrite what it just read."""
    book = PositionBook(tmp_path)
    book.add(_pos())
    reloaded = PositionBook(tmp_path)
    reloaded.path.unlink()
    reloaded.update(reloaded.positions["WETO::wedge_break"])
    assert not reloaded.path.exists()
