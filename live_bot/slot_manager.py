"""Tick-by-tick slot allocator (the 'zoom in on the priority stocks' engine).

IBKR grants only a handful of simultaneous tick-by-tick subscriptions (~3 by
default). This hands them to the highest-priority symbols and frees them when a
symbol no longer needs one. Phase A priority = OPEN POSITIONS (their stop is
client-side, so tick-precise exit is the money-losing path). Phase B will add
armed setups approaching their trigger.

Deliberately simple and testable: it's handed the CURRENT set of priority
symbols each loop and reconciles subscriptions to match, within the cap.
"""
from __future__ import annotations

from .tick_stream import TickStream


class SlotManager:
    def __init__(self, ib, broker, recorder, max_slots: int = 3, bar_seconds: int = 10,
                 on_ticks=None):
        self.ib = ib
        self.broker = broker
        self.rec = recorder
        self.max_slots = int(max_slots)
        self.bar_seconds = bar_seconds
        # Handed to every promoted stream so new prints reach the risk layer as an
        # EVENT. Set once by the controller; streams created later pick it up.
        self.on_ticks = on_ticks
        self.streams: dict[str, TickStream] = {}

    def get(self, symbol: str) -> TickStream | None:
        return self.streams.get(symbol.upper())

    def sync(self, priority_symbols: list[str]) -> None:
        """Reconcile live tick subscriptions to `priority_symbols` (already in
        priority order, highest first). Releases streams no longer needed, then
        promotes new ones up to the slot cap."""
        want = [s.upper() for s in priority_symbols]
        want_set = set(want)
        # release streams whose symbol dropped out of the priority set
        for sym in list(self.streams):
            if sym not in want_set:
                self._release(sym)
        # promote, respecting the cap and priority order
        for sym in want:
            if sym in self.streams:
                continue
            if len(self.streams) >= self.max_slots:
                self.rec.event("tick_slot_full", symbol=sym, cap=self.max_slots)
                break
            self._promote(sym)

    def _promote(self, symbol: str) -> None:
        try:
            contract = self.broker.contract(symbol)
            ts = TickStream(self.ib, contract, symbol, bar_seconds=self.bar_seconds,
                            on_ticks=self.on_ticks)
            ts.subscribe()
            self.streams[symbol] = ts
            self.rec.event("tick_slot_acquired", symbol=symbol, used=len(self.streams))
        except Exception as e:
            self.rec.event("tick_slot_error", symbol=symbol, error=repr(e))

    def _release(self, symbol: str) -> None:
        ts = self.streams.pop(symbol, None)
        if ts is not None:
            ts.cancel()
            # A cancel that failed means IBKR still holds the subscription even
            # though we no longer count it -- the leak that produced the 10190
            # storm. Record it loudly rather than letting the local count drift
            # away from the broker's silently.
            err = getattr(ts, "rec_error", None)
            if err:
                self.rec.event("tick_cancel_failed", symbol=symbol, error=err)
            self.rec.event("tick_slot_released", symbol=symbol, used=len(self.streams))

    def release_all(self) -> None:
        for sym in list(self.streams):
            self._release(sym)
