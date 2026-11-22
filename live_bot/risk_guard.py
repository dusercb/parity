"""Risk guards & fail-safes for the order layer (PORTING_TO_LIVE.md §13).

These are the checks that stand between a signal and a live order. Every one is
a HARD gate: if a guard can't confirm it's safe, the order does not go. The
user chose 'no auto-halt', so there is no daily-loss circuit breaker here -- but
the manual kill-file, the paper-account guard, order idempotency, and the
position cap are NOT optional and are always enforced.
"""
from __future__ import annotations

from pathlib import Path


class RiskGuard:
    def __init__(self, bcfg: dict, state_dir: Path):
        t = bcfg.get("trading", {}) or {}
        self.require_paper = t.get("require_paper_account", True)
        self.max_open = t.get("max_open_positions")           # None = leverage-funded concurrent
        self.kill_file = state_dir.parent / (t.get("kill_file") or "STOP")
        self._account: str | None = None
        # Idempotency: an order key that has been ACTED ON this session never fires
        # again -- the guard against a reconnect/re-evaluation double-submitting.
        self._acted: set[tuple] = set()
        # Realized P&L tracking (surfaced even though no auto-halt), for the log.
        self.realized_pnl = 0.0
        self.closed_trades = 0
        self.losses = 0

    # -- account safety --------------------------------------------------------
    def bind_account(self, account: str) -> None:
        """Called right after connect with the broker-reported account id. On a
        paper account it starts with 'DU' (or 'DF' for advisor paper); a live
        account is 'U'/'F'. Refuse to arm trading on anything but paper while the
        interlock is on."""
        self._account = account
        if self.require_paper and not (account.startswith("DU") or account.startswith("DF")):
            raise SystemExit(
                f"PAPER-ACCOUNT GUARD: connected account {account!r} is not a paper "
                f"account (expected DU/DF prefix). Refusing to place orders. Set "
                f"trading.require_paper_account: false ONLY on a deliberate live account.")

    # -- kill file -------------------------------------------------------------
    def kill_requested(self) -> bool:
        """True if the operator has dropped the kill-file. When set: no new
        entries, and the session controller flattens and stays flat."""
        return self.kill_file.exists()

    # -- idempotency -----------------------------------------------------------
    @staticmethod
    def order_key(ticker: str, date: str, entry_mode: str, setup_index) -> tuple:
        """One trade per (ticker, date, strategy, setup) -- a re-evaluation of the
        same accumulated frame, or a reconnect mid-session, must never place a
        second entry for a setup already acted on."""
        return (ticker.upper(), date, entry_mode, setup_index)

    def already_acted(self, key: tuple) -> bool:
        return key in self._acted

    def mark_acted(self, key: tuple) -> None:
        self._acted.add(key)

    # -- position cap ----------------------------------------------------------
    def can_open(self, open_count: int) -> bool:
        """Whether another position may open. None max = rely on the sizing walk's
        leverage funding to bound exposure (the 'full concurrent' choice); an int
        max is a hard ceiling on simultaneous positions."""
        if self.max_open is None:
            return True
        return open_count < int(self.max_open)

    # -- realized P&L bookkeeping (reported, not auto-halted) -------------------
    def record_close(self, pnl: float) -> None:
        self.realized_pnl = round(self.realized_pnl + pnl, 2)
        self.closed_trades += 1
        if pnl < 0:
            self.losses += 1
