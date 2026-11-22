"""Durable open-position state (PORTING_TO_LIVE.md §6/§9.2).

Every open position is written to disk the instant it changes, because the
premarket stop is CLIENT-SIDE and synthetic: if the bot process dies with a
position open, there is no exchange-resident stop protecting it. On restart the
bot must ADOPT whatever it finds -- both from this file and from the broker's
reported positions -- and resume managing the stop. A position the broker holds
but this file doesn't (or vice-versa) is a reconciliation alarm, never silently
ignored.

State is intentionally a plain JSON file, one object per open position keyed by
(ticker, entry_mode), so it's human-readable during an incident.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class Position:
    ticker: str
    # "dip_red_break" for anything this bot opens. A book written before
    # 2026-08-16 may still hold a "wedge_break" position, and adoption manages it
    # normally -- exit handling reads exit_mode off the position, not the engine.
    entry_mode: str
    direction: str             # "long" | "short"
    date: str
    shares: int
    entry_price: float
    stop_price: float          # the CURRENT synthetic-stop level (ratchets)
    target_price: float | None
    remaining: int             # shares still open (partials reduce this)
    setup_index: int = 1
    entry_order_id: int | None = None
    target_order_id: int | None = None
    exit_mode: str = "bracket"      # "bracket" | "candle_trail"
    trail_active: bool = False      # candle_trail: target dropped, riding the stop only
    trail_partial: float = 1.0      # fraction banked at the target before the runner (candle_trail)
    folded_upto: str | None = None  # ISO minute-start cursor: minutes already folded into the trail
    opened_at: str = ""
    # legs banked so far (for the realized-vs-backtest comparison)
    legs: list = field(default_factory=list)

    def key(self) -> str:
        return f"{self.ticker.upper()}::{self.entry_mode}"


class PositionBook:
    def __init__(self, state_dir: Path):
        state_dir.mkdir(parents=True, exist_ok=True)
        self.path = state_dir / "open_positions.json"
        self.positions: dict[str, Position] = {}
        self._last_written: str | None = None
        self._load()

    def _serialize(self) -> str:
        return json.dumps({k: asdict(p) for k, p in self.positions.items()}, indent=2)

    def _load(self) -> None:
        if self.path.exists():
            try:
                raw = json.loads(self.path.read_text())
                self.positions = {k: Position(**v) for k, v in raw.items()}
                self._last_written = self._serialize()
            except Exception:
                self.positions = {}

    def _flush(self) -> None:
        """Persist, but only when the state actually CHANGED.

        Every caller here is on a latency-sensitive path: _settle runs inside an
        ib event callback, _fold_trail calls update() for every trailing position
        on every ~2s pass, and reconcile ran unconditionally every 5s all session.
        Each of those was a synchronous write+rename -- real blocking file I/O on
        a VPS disk, executed inside ib's dispatcher, so it delayed not just this
        symbol but every other symbol's tick processing behind it. Serialising and
        comparing a few small dataclasses is microseconds; the write it skips is
        not. The write-then-rename is kept for the writes that do happen, so a
        crash mid-write still can't corrupt the file the next process adopts."""
        payload = self._serialize()
        if payload == self._last_written:
            return
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(payload)
        tmp.replace(self.path)
        self._last_written = payload

    def add(self, pos: Position) -> None:
        self.positions[pos.key()] = pos
        self._flush()

    def update(self, pos: Position) -> None:
        self.positions[pos.key()] = pos
        self._flush()

    def remove(self, pos: Position) -> None:
        self.positions.pop(pos.key(), None)
        self._flush()

    def open_count(self) -> int:
        return len(self.positions)

    def reconcile(self, broker_positions: dict[str, int], recorder,
                  ignore: set[str] | None = None) -> list[str]:
        """Compare the durable book against what the broker actually holds
        (symbol -> signed share qty). Returns human-readable alarms; the caller
        surfaces them. A mismatch means the bot's view of its risk is wrong,
        which near a synthetic stop is the highest-severity condition there is.

        `ignore` is a set of UPPERCASE symbols to skip -- those with an order in
        flight, or filled so recently the broker's position readback may not have
        caught up. Without it this is only safe to run at connect (crash
        adoption): run continuously, an entry that filled a moment ago looks
        exactly like a position that vanished, and the book would drop it.
        """
        alarms = []
        changed = False
        skip = ignore or set()
        for key, pos in list(self.positions.items()):
            if pos.ticker.upper() in skip:
                continue
            held = broker_positions.get(pos.ticker.upper(), 0)
            want = pos.remaining if pos.direction == "long" else -pos.remaining
            if held == 0:
                alarms.append(f"book has {pos.ticker} {pos.entry_mode} ({pos.remaining} sh) but broker holds none "
                              f"-- position closed while bot was down; dropping from book")
                self.positions.pop(key, None)
                changed = True
            elif held != want:
                alarms.append(f"book/broker share mismatch on {pos.ticker}: book {want}, broker {held} "
                              f"-- adopting broker qty")
                pos.remaining = abs(held)
                changed = True
        for sym, qty in broker_positions.items():
            if sym in skip:
                continue
            if qty != 0 and not any(p.ticker.upper() == sym for p in self.positions.values()):
                alarms.append(f"broker holds {qty} {sym} with NO position in the book "
                              f"-- unmanaged position, flatten manually or investigate")
        if changed:
            self._flush()
        for a in alarms:
            recorder.event("reconcile_alarm", detail=a)
        return alarms
