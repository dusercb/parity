"""Order lifecycle (PORTING_TO_LIVE.md §4 Stage 4 / §9.2).

Turns an intended trade from signal_runtime into real (paper) orders and manages
them to an exit. Deliberately event-poll driven so it runs inside the existing
session loop and is unit-testable with a fake broker.

FILL MODEL, matched to the backtester:
  * entry  = marketable limit at trigger +/- entry_limit_slippage_cents. Can't
             fill worse than the cap (the backtest's exact protection); a
             gap-through simply doesn't fill and the setup is abandoned.
  * target = a resting limit at the exchange (the backtest's no-latency target).
  * stop   = CLIENT-SIDE SYNTHETIC. No native premarket stop exists, so the
             manager watches each position's latest bar and, when the stop level
             is breached (bar low<=stop for a long, high>=stop for a short),
             submits a marketable-limit exit. => while a position is open, the
             bot being UP is the only thing enforcing the stop.

EXIT: whichever mode strategy.exit_mode selects, recorded as exit_mode on the
position.
  * candle_trail -- the 1-minute ratcheting trail (the dip's configured mode, so
    the live default): the stop ratchets to each completed minute's low, a
    fat-green close drops the target, and a target cross banks trail_partial
    before the runner rides on at breakeven.
  * bracket      -- fixed structural stop + fixed target, held to fill or the
    09:30 flatten. Selected by exit_mode: "standard".

SIZING (user choice 'full config sizing'): signal_core.capped_shares against the
broker's real NetLiquidation, then clamped to broker BuyingPower -- real numbers,
per §9.1, never a hardcoded margin rule.
"""
from __future__ import annotations

import time as systime
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from .position_state import Position

ET = ZoneInfo("America/New_York")

# Order states from which nothing further can fill. Reaching one of these is the
# broker's final word on an order -- but NOT a statement that nothing filled, so
# every caller must still read the filled quantity before discarding it.
TERMINAL = ("Cancelled", "ApiCancelled", "Inactive")

# IBKR's "no value" sentinel. CommissionReport.realizedPNL carries it on OPENING
# executions (there is no realised P&L yet); summing it as a number would produce
# a P&L of ~1.8e308 rather than an obviously-missing one.
UNSET_DOUBLE = 1.7976931348623157e+308


class OrderManager:
    def __init__(self, broker, sc, cfg, bcfg, book, guard, recorder, account, slots=None):
        self.broker = broker
        self.sc = sc                 # signal_core module (for capped_shares)
        self.cfg = cfg               # backtester AppConfig (sizing/risk/strategy)
        self.bcfg = bcfg
        self.book = book             # PositionBook
        self.guard = guard           # RiskGuard
        self.rec = recorder
        self.account = account
        self.slots = slots           # SlotManager (tick-by-tick), or None (bar-only)
        t = bcfg.get("trading", {}) or {}
        self.sizing = t.get("sizing", "config")
        self.fixed_notional = float(t.get("fixed_notional", 200))
        # Seconds an unfilled entry may rest before it is abandoned as a dead
        # signal, and seconds a touched target may wait for its resting limit to
        # confirm before the exit is escalated to a marketable one.
        self.entry_ttl_s = float(t.get("entry_ttl_s", 30))
        self.target_confirm_s = float(t.get("target_confirm_s", 20))
        # How far THROUGH a level a software-triggered exit is priced. IBKR has no
        # premarket market orders, so every exit is a limit; pricing it through the
        # level is what makes it behave like a market order while still capping the
        # worst fill. 0.10 is the value the stop exit has always used.
        self.exit_slippage = float(t.get("exit_slippage_cents", 0.10))
        self._pending: dict[str, dict] = {}   # entry orders awaiting fill
        self._targets: dict[str, object] = {}  # position key -> resting target trade
        self._target_shares: dict[str, int] = {}  # position key -> resting target's share count
        self._exiting: dict[str, tuple] = {}   # position key -> (pos, exit trade, reason)
        # position key -> (touch price, monotonic touch time). A target the price
        # has TOUCHED but the broker has not yet confirmed filled. The position
        # stays in the book and keeps its stop enforced while it sits here.
        self._confirming: dict[str, tuple] = {}
        # position key -> (exit trade, shares, touch price). A candle_trail partial
        # SOLD but not yet confirmed. Unlike _exiting this does NOT suspend the
        # position: the runner is still live and its stop must still be watched
        # while the partial fills.
        self._banking: dict[str, tuple] = {}
        self._tick_cursor: dict[str, int] = {}  # position key -> last tick index scanned
        self._in_event = False                 # re-entrancy guard for broker callbacks
        self._last_reconcile = 0.0
        # Broker truth, accumulated from commissionReportEvent. Deliberately kept
        # apart from guard.realized_pnl (the model) so the two can be compared
        # rather than silently conflated.
        self.broker_realized = 0.0
        self.commission_total = 0.0

    # -- broker events (A: the broker TELLS us, we don't ask) -----------------
    def attach_events(self, ib) -> None:
        """Subscribe to IBKR's order-status / execution stream so a fill is acted
        on the moment it happens instead of on the next 2s poll.

        The handler deliberately just runs the SAME poll functions the loop runs.
        They are idempotent dict scans, so calling them early is free and cannot
        diverge from the polled path -- events supply latency, polling remains the
        backstop if an event is ever missed (a reconnect drops the subscription;
        a callback that raises would otherwise strand the state machine)."""
        def _on_event(*_args) -> None:
            if self._in_event:
                return                       # placing an order re-enters; do not recurse
            self._in_event = True
            try:
                self.poll_fills()
                self.poll_exits()
                self.poll_banks()
                for pos in list(self.book.positions.values()):
                    self._settle_if_target_filled(pos)
            except Exception as e:
                self.rec.event("order_event_error", error=repr(e))
            finally:
                self._in_event = False

        for name in ("orderStatusEvent", "execDetailsEvent"):
            ev = getattr(ib, name, None)
            if ev is not None:
                ev += _on_event
        self._event_handler = _on_event      # keep a ref; ib holds these weakly

        def _on_commission(_trade, fill, report) -> None:
            if self._in_event:
                return
            self._in_event = True
            try:
                self._record_commission(fill, report)
            except Exception as e:
                self.rec.event("commission_event_error", error=repr(e))
            finally:
                self._in_event = False

        ev = getattr(ib, "commissionReportEvent", None)
        if ev is not None:
            ev += _on_commission
        self._commission_handler = _on_commission

    # -- broker-truth P&L (what the account actually made) --------------------
    def _record_commission(self, fill, report) -> None:
        """IBKR's own account of one execution: what it cost, and -- on a CLOSING
        execution -- what it actually realised.

        This is the authoritative answer to "what did that trade really make".
        _settle's figure is a MODEL: (exit - entry) x shares, gross of commission,
        computed from fill prices the bot observed. Both are kept. The model is
        what parity is measured against (it is what the backtest computes); this
        is what the account statement will say. Their difference, accumulated over
        a session, is the execution cost the backtest cannot see.

        Pushed, not polled -- it arrives on the same event stream the fills do, so
        it costs nothing on the hot path and needs no request pacing.

        realizedPNL carries IBKR's UNSET sentinel on opening executions (there is
        nothing realised yet); treating that as a number would make session P&L
        ~1.8e308 instead of visibly absent."""
        sym = getattr(getattr(fill, "contract", None), "symbol", None) or "?"
        commission = _num(getattr(report, "commission", None))
        realized = _num(getattr(report, "realizedPNL", None))
        if commission is not None:
            self.commission_total = round(self.commission_total + commission, 4)
        if realized is not None:
            self.broker_realized += realized
            self.broker_realized = round(self.broker_realized, 4)
        self.rec.event("commission_report", ticker=sym,
                       exec_id=getattr(getattr(fill, "execution", None), "execId", None),
                       shares=getattr(getattr(fill, "execution", None), "shares", None),
                       price=getattr(getattr(fill, "execution", None), "price", None),
                       commission=commission, realized_pnl=realized,
                       broker_realized_session=self.broker_realized,
                       commission_session=self.commission_total)

    # -- continuous reconciliation (B: broker truth, every loop) --------------
    def reconcile_open(self, min_interval_s: float = 5.0, grace_s: float = 15.0) -> list[str]:
        """Re-check the book against the broker's actual positions DURING the
        session, not just at connect.

        Symbols with an order in flight, or filled within `grace_s`, are skipped:
        the broker's position readback lags a fill by a moment, and without that
        grace a just-opened position looks identical to one that vanished. Rate-
        limited because it is called from the hot loop; ib.positions() is a locally
        cached push-updated view, so the real cost is the book flush on a change."""
        now = systime.monotonic()
        if now - self._last_reconcile < min_interval_s:
            return []
        self._last_reconcile = now
        busy = {s.upper() for s in self._pending}
        for key in self._exiting:
            busy.add(key.split("::")[0].upper())
        cutoff = datetime.now(ET) - timedelta(seconds=grace_s)
        for pos in self.book.positions.values():
            try:
                opened = datetime.fromisoformat(pos.opened_at) if pos.opened_at else None
            except ValueError:
                opened = None
            if opened is None:
                busy.add(pos.ticker.upper())   # unreadable timestamp: never guess, skip
                continue
            # open_positions.json is meant to be hand-editable during an incident,
            # so a naive timestamp is a realistic input; read it as ET rather than
            # letting the comparison raise and take the whole reconcile down.
            if opened.tzinfo is None:
                opened = opened.replace(tzinfo=ET)
            if opened > cutoff:
                busy.add(pos.ticker.upper())
        alarms = self.book.reconcile(self.broker.positions(), self.rec, ignore=busy)
        for a in alarms:
            print(f"RECONCILE: {a}", flush=True)
        return alarms

    # -- sizing ---------------------------------------------------------------
    def _size(self, trade: dict) -> tuple[int, str]:
        entry = float(trade["entry_price"])
        if self.sizing == "fixed":
            return max(int(self.fixed_notional // entry), 0), "fixed"
        equity = self.broker.account_value("NetLiquidation", self.account) or self.cfg.risk.starting_equity
        # Dip sizing, exactly as apply_position_sizing does it in the backtester.
        # The wedge's two extra volume caps (vol_pct and the lookback-window cap)
        # were only ever applied under its is_wedge gate; with the wedge gone the
        # dip takes risk.max_pct_of_volume_at_entry through capped_shares' own
        # default, as the backtest does for a dip trade.
        basis, shares = self.sc.capped_shares(
            equity, entry, trade.get("stop_price"),
            trade.get("volume_at_entry"), self.cfg.risk)
        # FUNDING cap -- the backtester's concurrent-leverage model, live: total
        # exposure across open + pending positions may not exceed equity *
        # max_leverage. `committed` EXCLUDES this trade (it isn't pending yet), so
        # a second entry fired in the SAME loop already sees the first's
        # reservation -- the fix for the same-loop over-commit gap. This mirrors
        # apply_position_sizing (headroom = equity*max_leverage - committed) so a
        # concurrent live book sizes exactly as the backtest.
        if self.cfg.risk.max_leverage is not None:
            committed = self._committed_notional()
            headroom = max(equity * self.cfg.risk.max_leverage - committed, 0.0)
            fundable = int(headroom // entry)
            if fundable < shares:
                shares, basis = fundable, "funding"
        # Then clamp to what the broker will ACTUALLY fund (BuyingPower): the real
        # ceiling on top of the model, so a thin account can never over-order even
        # if max_leverage would allow it.
        bp = self.broker.account_value("BuyingPower", self.account)
        if bp is not None and shares * entry > bp:
            shares = int(bp // entry)
            basis = "buying_power"
        return max(shares, 0), basis

    def _committed_notional(self) -> float:
        """Notional currently tied up: filled positions (remaining * entry) plus
        submitted-but-unfilled entries (shares * limit). Computed fresh each call
        so concurrent same-loop entries see each other's reservations -- the live
        analogue of the backtester's running `committed`."""
        c = sum(p.remaining * p.entry_price for p in self.book.positions.values())
        c += sum(pd["shares"] * pd["limit"] for pd in self._pending.values())
        return c

    # -- entry ----------------------------------------------------------------
    def enter(self, trade: dict) -> None:
        """Place a marketable-limit entry for one intended trade, if guards pass."""
        key = self.guard.order_key(trade["ticker"], trade["date"],
                                   trade["entry_mode"], trade.get("setup_index", 1))
        if self.guard.already_acted(key) or trade["ticker"].upper() in {p.ticker.upper() for p in self.book.positions.values()}:
            return
        if not self.guard.can_open(self.book.open_count() + len(self._pending)):
            self.rec.event("entry_skipped", ticker=trade["ticker"], reason="position cap")
            return
        shares, basis = self._size(trade)
        if shares <= 0:
            self.rec.event("entry_skipped", ticker=trade["ticker"], reason="size 0", basis=basis)
            return
        direction = trade.get("direction") or "long"
        action = "BUY" if direction == "long" else "SELL"
        slip = self.cfg.strategy.entry_limit_slippage_cents
        trig = float(trade.get("trigger_price") or trade["entry_price"])
        limit = round(trig + slip, 2) if direction == "long" else round(trig - slip, 2)
        self.guard.mark_acted(key)          # idempotent BEFORE the call -- a retry can't double-submit
        entry_trade = self.broker.marketable_limit(trade["ticker"], action, shares, limit)
        self._pending[trade["ticker"].upper()] = {
            "trade": trade, "shares": shares, "basis": basis, "action": action,
            "limit": limit, "order": entry_trade, "direction": direction,
            # Monotonic, not wall-clock: this is measured on the hot path and a
            # DST/NTP step must never make an entry look hours old (or negative).
            "submitted_at": systime.monotonic(), "cancel_sent": False}
        self.rec.event("entry_submitted", ticker=trade["ticker"], engine=trade["entry_mode"],
                       direction=direction, shares=shares, limit=limit, basis=basis)

    def poll_fills(self) -> None:
        """Promote filled entries into managed positions (target + synthetic stop),
        and abandon entries that have gone stale without filling."""
        if not self._pending:
            return                           # the common case: no clock read, no work
        now = systime.monotonic()
        for sym, p in list(self._pending.items()):
            order = p["order"]
            status = _status(order)
            if status == "Filled":
                got = _filled_qty(order, p["shares"])
                fill = _avg_fill(order, p["limit"])
                self._pending.pop(sym, None)      # drop the reservation before measuring committed
                self._open_position(p, fill, shares=got)
            elif status in TERMINAL:
                # TERMINAL IS NOT "NOTHING HAPPENED". A cancelled entry may have
                # filled part way first, and those shares are real, held, and --
                # if we discard the order here -- carry no stop and no position in
                # the book. Adopt whatever actually filled and manage it normally.
                got = _filled_qty(order, 0)
                self._pending.pop(sym, None)
                if got > 0:
                    fill = _avg_fill(order, p["limit"])
                    self.rec.event("entry_partial_fill", ticker=sym, status=status,
                                   filled=got, ordered=p["shares"], fill=fill)
                    self._open_position(p, fill, shares=got)
                else:
                    self.rec.event("entry_unfilled", ticker=sym, status=status)
            elif p["cancel_sent"]:
                continue                          # waiting on the broker's verdict
            elif self._price_through_limit(sym, p):
                self._abandon_stale_entry(sym, p, status, reason="price_through_limit")
            elif now - p["submitted_at"] >= self.entry_ttl_s:
                self._abandon_stale_entry(sym, p, status, reason="ttl")

    def _price_through_limit(self, sym: str, p: dict) -> bool:
        """True once the market has traded BEYOND our entry limit.

        The entry is a marketable limit at trigger +/- entry_limit_slippage_cents,
        so it can never fill worse than that cap. Once price is through the cap the
        remainder cannot fill at all -- it is a resting order waiting for the move
        to come back, which is the opposite of what a momentum break wants. Cancel
        it now rather than holding the sizing reservation for the full entry_ttl_s
        while the setup runs away.

        Anything already filled is kept: a partial at our price is a real position
        at the price we asked for, and is managed exactly like a full one.

        Needs a tick slot to see the price. Without one the TTL remains the
        backstop -- this only ever fires EARLIER than the TTL would, never
        instead of a check that would otherwise have happened."""
        stream = self.slots.get(sym) if self.slots else None
        if stream is None:
            return False
        px = stream.last_price()
        if px is None:
            return False
        return px > p["limit"] if p["direction"] == "long" else px < p["limit"]

    def _abandon_stale_entry(self, sym: str, p: dict, status: str, reason: str) -> None:
        """Give up on an entry that will not fill -- either because price has
        traded through our limit ("price_through_limit") or because it has simply
        sat there for entry_ttl_s ("ttl").

        Either way it is a DEAD signal: the fill model already says a gap-through simply doesn't fill
        and the setup is abandoned, and on a momentum break a trigger that is
        seconds old has moved. Left alone such an order is not merely idle -- it
        holds its sizing reservation in _committed_notional (starving later
        entries of leverage headroom and corrupting the leverage telemetry), it
        keeps the symbol in reconcile's `busy` set so the symbol is never checked
        against broker truth again, and it can still fill hours later on a signal
        that is long dead. BOXL 2026-08-14 was held for a short locate at 10:01
        (IBKR 404) and leaked all three of those for 3h34m until the session-end
        flatten silently cancelled it.

        The order is NOT dropped here. A cancel races an in-flight fill, so the
        reservation stays until the BROKER says which one won -- poll_fills keeps
        reading its status and will still open the position if it filled. Dropping
        it now would recreate, on the entry side, exactly the phantom-position bug
        this module already carries a scar for."""
        p["cancel_sent"] = True
        self.broker.cancel(p["order"])
        self.rec.event("entry_stale_cancel", ticker=sym, status=status, reason=reason,
                       shares=p["shares"], limit=p["limit"],
                       filled_so_far=_filled_qty(p["order"], 0),
                       age_s=round(systime.monotonic() - p["submitted_at"], 1))

    def _open_position(self, p: dict, fill: float, shares: int | None = None) -> None:
        """Promote a filled (or partly filled) entry into a managed position.

        `shares` is what the BROKER filled, which is not always what we ordered --
        a partial fill is a smaller but entirely real position, and everything
        downstream (the resting target, the leverage telemetry, the flatten) has
        to be sized off the real number."""
        t = p["trade"]
        direction = p["direction"]
        shares = int(p["shares"] if shares is None else shares)
        if shares <= 0:
            return
        # Exit mode comes from the SAME config the backtest used. Single strategy,
        # so it is always strategy.exit_mode -- "candle_trail_1min" maps to the
        # live trailing manager, anything else to the bracket.
        cfg_exit = self.cfg.strategy.exit_mode
        trail_partial = self.cfg.strategy.trail_partial_at_target
        exit_mode = "candle_trail" if cfg_exit == "candle_trail_1min" else "bracket"
        now_min = datetime.now(ET).replace(second=0, microsecond=0)
        pos = Position(
            ticker=t["ticker"], entry_mode=t["entry_mode"], direction=direction,
            date=t["date"], shares=shares, entry_price=fill,
            stop_price=float(t["stop_price"]), target_price=t.get("target_price"),
            remaining=shares, setup_index=t.get("setup_index", 1),
            exit_mode=exit_mode, trail_partial=float(trail_partial),
            folded_upto=now_min.isoformat(),
            opened_at=datetime.now(ET).isoformat(timespec="seconds"))
        # SEED THE TICK CURSOR TO NOW. The stream's buffer is never trimmed, so it
        # holds every print since the slot was granted -- often an hour or more of
        # them, from before this position existed. _resolve_ticks defaults `since`
        # to 0, so without this the first scan walks that whole history and stops
        # out on the first tick below the stop, whenever it happened.
        #
        # PPBT 2026-09-02: slot granted 07:38:31, a 2.23 print at 07:38:40, entry
        # filled 2.297 at 08:47:39 with the stop at 2.23. The stop "fired" 0.2s
        # later on that 69-minute-old tick, at a moment when price had not traded
        # below 2.26 since the fill. It exited at 2.28; the trade ran to 3.00
        # against a 2.35 target. VRAX 2026-08-19 shows the same 0.1s signature.
        #
        # The backtest has always guarded this ("ticks before the trigger crossed
        # predate the fill and must not count as exits"); only the live side did
        # not, which makes it a Class-0 divergence as well as a losing trade.
        stream = self.slots.get(pos.ticker) if self.slots else None
        if stream is not None:
            self._tick_cursor[pos.key()] = _stream_cursor(stream, 0)
        # Resting target limit. Bracket rests the WHOLE position at the target.
        # candle_trail rests only the PARTIAL fraction (trail_partial); the runner
        # remainder carries no target and rides the trailing stop. The stop side
        # is client-side synthetic in both modes (no native premarket stop).
        if pos.target_price and exit_mode == "candle_trail":
            # candle_trail rests NOTHING. Its target does not close the position --
            # it banks trail_partial and TRANSITIONS the rest (target dropped, stop
            # to breakeven), so book and broker have to move together. A resting
            # limit cannot promise that: the backtest banks whenever price touches
            # the level, while a resting order only fills if the queue reaches it,
            # and a wick that tags the target and reverses is exactly the case the
            # trail exists for. Instead the touch sends a marketable limit for the
            # partial, and the transition applies on the fill. See _bank_partial.
            self._target_shares[pos.key()] = max(
                int(round(pos.remaining * pos.trail_partial)), 1)                 if pos.trail_partial < 1.0 - 1e-9 else pos.remaining
        elif pos.target_price:
            # bracket: the whole position rests at the target, and a touch only
            # has to agree about a CLOSE, so the resting limit is kept (it can fill
            # passively at our price) and _arm_target_confirm waits on it.
            tgt_action = "SELL" if direction == "long" else "BUY"
            tt = self.broker.resting_limit(pos.ticker, tgt_action, pos.remaining, pos.target_price)
            pos.target_order_id = id(tt)
            self._targets[pos.key()] = tt
            self._target_shares[pos.key()] = pos.remaining
        # Leverage telemetry, mirroring the backtester's per-trade fields, for the
        # live-vs-backtest leverage calibration. committed_before excludes this
        # position (its reservation was popped in poll_fills and it isn't in the
        # book yet), exactly like the sizing walk's `committed` at entry.
        equity = self.broker.account_value("NetLiquidation", self.account) or self.cfg.risk.starting_equity
        committed_before = self._committed_notional()
        notional = pos.shares * fill
        open_at_entry = self.book.open_count()
        cash_free = max(equity - committed_before, 0.0)
        margin_used = round(max(notional - cash_free, 0.0), 2)
        leverage_at_entry = round((committed_before + notional) / equity, 3) if equity > 0 else None
        self.book.add(pos)
        self.rec.event("position_opened", ticker=pos.ticker, engine=pos.entry_mode,
                       direction=direction, shares=pos.shares, entry=fill,
                       stop=pos.stop_price, target=pos.target_price, exit_mode=exit_mode,
                       basis=p.get("basis"), open_positions_at_entry=open_at_entry,
                       margin_used=margin_used, leverage_at_entry=leverage_at_entry)

    # -- management (synthetic stop + target watch) ---------------------------
    def manage(self, feeds: dict) -> None:
        """Called every loop tick. For each open position, resolve stop/target.

        TICK-PRECISE when the position holds a tick slot (SlotManager): the
        stream is scanned for the FIRST stop/target crossing in tick order --
        the exact live analogue of the backtest's first-touch resolution, firing
        on the crossing PRINT rather than a 5s-late bar. Falls back to the bar
        low/high check when no slot is held (a lower-priority open position, or
        Phase-A not enabled)."""
        for pos in list(self.book.positions.values()):
            stream = self.slots.get(pos.ticker) if self.slots else None
            feed = feeds.get(pos.ticker.upper())
            frame = feed.frame() if feed else None
            if pos.exit_mode == "candle_trail":
                # Fold newly-completed 1-min candles FIRST -- ratchet the synthetic
                # stop to the prior minute's low and drop the target on a fat-green
                # close -- then resolve against the updated levels (fold-then-check,
                # the backtest's order).
                self._fold_trail(pos, frame)
                self._resolve_trail(pos, frame, stream)
            else:
                self._resolve_bracket(pos, frame, stream)

    def _fold_trail(self, pos: Position, frame) -> None:
        """Fold every 1-minute candle that has fully COMPLETED since we last
        looked (causal -- only minutes strictly before the current bar's minute):
        ratchet the synthetic stop toward the prior minute's extreme (low for a
        long, high for a short; never against the trade), and on the first
        FAT-bodied close in the trade's favour after the entry minute, DROP the
        target (cancel the resting partial). Mirrors the backtest's minute fold.

        HOT PATH. This runs for every trailing position on every ~2s pass, and
        with the dip's candle_trail as the only live exit mode it is now THE exit
        path. It used to resample the entire session frame and rebuild a dict of
        every minute since 04:00 on each call -- O(session) work to consume the
        one new minute that appears every 30th call, growing all morning. Two
        changes, both exactly behaviour-preserving: return before touching pandas
        when no new minute has completed, and resample only the tail from
        `folded` (a minute boundary, so bucketing is identical) rather than the
        whole frame."""
        if frame is None or frame.empty:
            return
        entry_min = pd.Timestamp(pos.opened_at).floor("min")
        folded = pd.Timestamp(pos.folded_upto) if pos.folded_upto else entry_min
        cur_min = pd.Timestamp(frame.iloc[-1]["dt_et"]).floor("min")
        if folded >= cur_min:
            # Nothing has completed since the last fold: no resample, no scan, no
            # flush. This is the overwhelmingly common case.
            if pos.folded_upto is None:
                pos.folded_upto = folded.isoformat()
                self.book.update(pos)
            return
        # Only the minutes still to fold. searchsorted keeps this O(log n) on the
        # sorted dt_et rather than a full-frame boolean mask.
        tail = frame.iloc[frame["dt_et"].searchsorted(folded):]
        one_min = self.sc.build_1m_bars(tail) if not tail.empty else tail
        if one_min.empty:
            return
        fg = self.cfg.strategy.fatgreen_body_min
        info = {}
        for m in one_min.itertuples():
            rng = float(m.high) - float(m.low)
            up = bool(rng > 0 and m.close > m.open and (m.close - m.open) >= fg * rng and (m.close - m.low) / rng >= 2.0 / 3.0)
            dn = bool(rng > 0 and m.close < m.open and (m.open - m.close) >= fg * rng and (m.high - m.close) / rng >= 2.0 / 3.0)
            info[pd.Timestamp(m.dt_et).floor("min")] = (float(m.low), float(m.high), up, dn)
        dropped = False
        while folded < cur_min:
            mi = info.get(folded)
            if mi is not None:
                low, high, up_fat, dn_fat = mi
                if pos.direction == "long":
                    pos.stop_price = max(pos.stop_price, round(low, 2))
                    fat = up_fat
                else:
                    pos.stop_price = min(pos.stop_price, round(high, 2))
                    fat = dn_fat
                if fat and folded > entry_min and not pos.trail_active:
                    pos.trail_active = True
                    dropped = True
            folded = folded + pd.Timedelta(minutes=1)
        pos.folded_upto = folded.isoformat()
        if dropped:
            tt = self._targets.pop(pos.key(), None)
            self._target_shares.pop(pos.key(), None)
            if tt is not None:
                self.broker.cancel(tt)
            self.rec.event("trail_target_dropped", ticker=pos.ticker, stop=pos.stop_price)
        self.book.update(pos)

    def _resolve_bracket(self, pos: Position, frame, stream) -> None:
        """Fixed stop + resting target (the dip's mode when
        exit_mode is 'standard'). Tick-precise when a slot is held, else bar."""
        # THE BROKER FIRST. The target is a real resting limit at the exchange, so
        # whether it filled is a FACT to read, not a level to predict -- and it can
        # fill without any local signal at all. This check used to live only in the
        # bar branch, below the stream branch's unconditional return, so a position
        # holding a tick slot never noticed its own target fill. ZENA 2026-08-07
        # filled its target at 07:13:40 and the bot tracked the phantom for 2h13m,
        # then tried to sell shares it no longer held -- which IBKR correctly read
        # as a naked short (error 404, held for locate). Broker truth outranks
        # every predicted level, so it is tested first and unconditionally.
        if self._settle_if_target_filled(pos):
            return
        # Normally already resolved by the tick callback; re-run as the backstop.
        if self._resolve_ticks(pos, stream):
            return
        # Fall THROUGH to bars rather than returning when a stream exists but
        # resolved nothing: a stream IBKR rejected (error 10190, "max tick-by-tick
        # requests reached") still exists as a local object and simply never
        # delivers, so an unconditional return let a dead stream suppress the
        # working bar path entirely. Ticks may only ever fire EARLIER than bars,
        # never instead of them.
        if frame is not None and not frame.empty and pos.key() not in self._exiting:
            last = frame.iloc[-1]
            bar_low, bar_high = float(last["low"]), float(last["high"])
            breached = (bar_low <= pos.stop_price) if pos.direction == "long" else (bar_high >= pos.stop_price)
            if breached:
                self._exit(pos, reason="stop")
                return
        # Last: a target that was touched but never confirmed. Runs even with no
        # frame -- an unconfirmed target must never depend on bar data arriving.
        self._escalate_unconfirmed_target(pos)

    def _resolve_ticks(self, pos: Position, stream) -> bool:
        """Tick-precise stop/target resolution for ONE position. True when the
        position was acted on.

        The single shared implementation behind BOTH the tick callback (which runs
        the instant a print arrives) and the loop's bar-path fallback, so the
        latency of a decision can differ but the decision itself never can."""
        if stream is None or pos.key() in self._exiting:
            return False
        if pos.exit_mode == "candle_trail":
            target = pos.target_price if (pos.target_price and not pos.trail_active) else None
        else:
            target = pos.target_price
        if pos.key() in self._confirming or pos.key() in self._banking:
            target = None      # already acted on; only the stop may fire now
        no_target = float("inf") if pos.direction == "long" else 0.0
        # A released-then-regranted slot builds a NEW TickStream whose buffer
        # starts at zero, while our stored cursor still refers to the old one.
        # Left alone, that out-of-range `since` scans nothing at all -- a MISSED
        # stop, the opposite failure to the one seeding fixes. A cursor ahead of
        # the stream can only mean a replacement (it is set from the stream's own
        # count, which never shrinks), and every tick in a stream created after
        # the position opened is post-fill by construction, so the fresh buffer is
        # scanned from the start rather than from its end.
        stored = self._tick_cursor.get(pos.key(), 0)
        cur = _stream_cursor(stream, stored)
        since = stored if stored <= cur else 0
        kind, _t, price, cursor = stream.first_touch(
            since, pos.direction, pos.stop_price, target if target else no_target)
        self._tick_cursor[pos.key()] = cursor
        if kind == "stop":
            trailing = pos.exit_mode == "candle_trail" and pos.trail_active
            self._exit(pos, reason="trail_stop" if trailing else "stop", tick_price=price)
            return True
        if kind == "target":
            if pos.exit_mode == "candle_trail":
                # NOTE: the candle_trail partial still banks on the touch and so
                # carries the same unconfirmed-exit exposure fixed below for the
                # bracket. Deferring a PARTIAL needs the escalation to exit only
                # _target_shares rather than pos.remaining, so it is left for the
                # focused candle_trail build rather than half-changed here.
                self._bank_partial(pos, price)
            else:
                self._arm_target_confirm(pos, price)
            return True
        return False

    # -- EVENT-DRIVEN risk path (the money path does not wait for the loop) ----
    def on_ticks(self, symbol: str) -> None:
        """Called from the TickStream the instant new prints arrive: resolve this
        symbol's stop/target NOW and place the exit from inside the callback.

        The loop's cadence is ~2s. On a median
        stop ~9c, a quarter under 5c -- price routinely moves a third of a stop
        width inside a single 10s bar, so waiting for the next poll is a
        first-order cost in realised R, not a rounding error. The loop still runs
        the same resolution afterwards as a backstop; this only gets there first.

        Guarded against re-entrancy because placing an order re-enters the broker's
        event stream, and never allowed to raise: an exception escaping here would
        propagate into ib's dispatcher and could take down the feed that every
        other position depends on."""
        if self._in_event:
            return
        self._in_event = True
        try:
            sym = symbol.upper()
            for pos in list(self.book.positions.values()):
                if pos.ticker.upper() != sym:
                    continue
                stream = self.slots.get(pos.ticker) if self.slots else None
                if stream is not None:
                    self._resolve_ticks(pos, stream)
        except Exception as e:
            self.rec.event("tick_event_error", symbol=symbol, error=repr(e))
        finally:
            self._in_event = False

    def _settle_if_target_filled(self, pos: Position) -> bool:
        """Settle the position if its resting target limit has filled at the
        broker. True when it did (the caller must stop managing this position)."""
        tt = self._targets.get(pos.key())
        if tt is None or _status(tt) != "Filled":
            return False
        price = _avg_fill(tt, pos.target_price or pos.entry_price)
        if pos.exit_mode == "candle_trail":
            # candle_trail rests only trail_partial of the position; the runner
            # carries no target and rides the trail. Settling the WHOLE position
            # here -- which this did for every caller, including the broker event
            # handler that runs over every open position -- booked P&L on shares
            # that were never sold and dropped a still-held runner from the book.
            self._bank_partial(pos, price)
        else:
            self._settle(pos, price, reason="target")
        return True

    # -- target: DECIDE on the touch, SETTLE on the broker --------------------
    def _arm_target_confirm(self, pos: Position, price: float) -> None:
        """The price touched the target. Record the decision -- but do NOT book
        the exit, because the resting limit is a real order at the exchange and
        only IT can say whether we are flat.

        This used to call _settle() straight from the tick: P&L banked at the
        TOUCH price and the position deleted from the book, with nothing asked of
        the broker. WETO 2026-08-14 printed through its 9.45 target at 10:41:44,
        the bot booked the exit at the print of 9.47 and dropped the position, and
        the broker did not actually fill for another 88 SECONDS -- during which a
        live 242-share long sat outside the book, so manage() never enforced its
        synthetic stop, open_positions.json no longer listed it for crash
        adoption, and the reconciler could only shout about an unmanaged position
        it had no authority to fix. Worse, _settle popped the resting target
        WITHOUT cancelling it, so had price reversed instead of filling, nothing
        would ever have cleaned it up: flatten_all only walks the book and
        _pending, and the premarket-flat guarantee would have been broken.

        Keeping the position in the book costs nothing and fixes all of it -- the
        stop stays enforced, reconcile sees a matching book/broker pair instead of
        an alarm, and the fill price we eventually record is the broker's, not a
        print we happened to see. The exit DECISION is still made here, on the
        first touch, so parity with the backtest's first-touch resolution is
        unchanged; only the bookkeeping now waits for the truth."""
        self._confirming[pos.key()] = (price, systime.monotonic())
        self.rec.event("target_touched", ticker=pos.ticker, touch=price,
                       target=pos.target_price, shares=pos.remaining)

    def _escalate_unconfirmed_target(self, pos: Position) -> None:
        """A touched target whose resting limit still has not filled after
        target_confirm_s: the market traded through the level without taking our
        size, so stop waiting on it and take the exit actively. Cancels the
        resting limit and submits the same marketable exit the stop path uses."""
        c = self._confirming.get(pos.key())
        if c is None or pos.key() in self._exiting:
            return
        touch, at = c
        waited = systime.monotonic() - at
        if waited < self.target_confirm_s:
            return
        self._confirming.pop(pos.key(), None)
        self.rec.event("target_unconfirmed", ticker=pos.ticker, touch=touch,
                       target=pos.target_price, waited_s=round(waited, 1))
        self._exit(pos, reason="target_unconfirmed")

    def _resolve_trail(self, pos: Position, frame, stream) -> None:
        """candle_trail: the ratcheting synthetic stop always applies; the target
        applies only until it's dropped (fat-green, via _fold_trail) or its
        partial has banked (trail_active). A stop breach exits the remainder; a
        target cross banks the partial and sends the runner to breakeven."""
        target = pos.target_price if (pos.target_price and not pos.trail_active) else None
        # Normally already resolved by the tick callback; re-run as the backstop,
        # then fall through to bars (a dead stream must not suppress them).
        if self._resolve_ticks(pos, stream):
            return
        if frame is None or frame.empty or pos.key() in self._exiting:
            return
        last = frame.iloc[-1]
        bar_low, bar_high = float(last["low"]), float(last["high"])
        breached = (bar_low <= pos.stop_price) if pos.direction == "long" else (bar_high >= pos.stop_price)
        if breached:
            self._exit(pos, reason="trail_stop" if pos.trail_active else "stop")
            return
        if target is not None and pos.key() not in self._banking:
            # Bar fallback for the target, mirroring the backtest's own
            # (t_target = bar_high >= cur_target). This used to ask whether the
            # RESTING order had filled -- with the partial no longer resting,
            # that question has no answer and a position holding no tick slot
            # would never bank at all.
            touched = (bar_high >= target) if pos.direction == "long" else (bar_low <= target)
            if touched:
                self._bank_partial(pos, target)

    def _bank_partial(self, pos: Position, price: float) -> None:
        """A candle_trail target cross before any fat-green: SELL the partial now,
        and apply the trail transition when the broker says it went through.

        The backtest banks on the touch and immediately drops the target and jumps
        the remainder's stop to breakeven -- a transition that only makes sense
        BECAUSE half the position was sold. Mirroring that on an unconfirmed touch
        moved four pieces of state (booked P&L, reduced remaining, dropped the
        target, ratcheted the stop) on the strength of a price we merely saw. If
        the sale had not actually happened, reconcile would then heal `remaining`
        back from broker truth while the P&L stayed booked -- the same shares paid
        for twice -- and the runner would be managed at breakeven having banked
        nothing.

        So the touch submits a real order instead of assuming one. A marketable
        limit, priced through the target by exit_slippage: IBKR has no premarket
        market orders, so this is the same instrument the synthetic stop uses --
        it fills like a market order but can never fill worse than the cap. The
        cost is the spread on the partial, paid deliberately, in exchange for book
        and broker transitioning together.

        Nothing about the position changes here. The runner is untouched and its
        stop keeps being watched -- _banking, unlike _exiting, does not suspend
        the position -- until poll_banks sees the fill."""
        key = pos.key()
        if key in self._banking or key in self._exiting:
            return                                   # one bank order per position
        qty = min(self._target_shares.get(key, pos.remaining), pos.remaining)
        if qty <= 0:
            return
        action = "SELL" if pos.direction == "long" else "BUY"
        tgt = pos.target_price if pos.target_price is not None else price
        cap = (round(tgt - self.exit_slippage, 2) if pos.direction == "long"
               else round(tgt + self.exit_slippage, 2))
        ex = self.broker.marketable_limit(pos.ticker, action, qty, cap)
        self._banking[key] = (ex, qty, price)
        self.rec.event("partial_submitted", ticker=pos.ticker, shares=qty,
                       touch=round(price, 4), cap=cap, target=pos.target_price)

    def poll_banks(self) -> None:
        """Resolve in-flight candle_trail partials against the broker."""
        for key, (ex, qty, touch) in list(self._banking.items()):
            pos = self.book.positions.get(key)
            if pos is None:                          # position closed out from under it
                self._banking.pop(key, None)
                continue
            st = _status(ex)
            if st == "Filled":
                self._banking.pop(key, None)
                self._apply_bank(pos, _avg_fill(ex, pos.target_price or pos.entry_price),
                                 _filled_qty(ex, qty))
            elif st in TERMINAL:
                got = _filled_qty(ex, 0)
                self._banking.pop(key, None)
                if got > 0:
                    self.rec.event("partial_incomplete", ticker=pos.ticker,
                                   filled=got, wanted=qty, status=st)
                    self._apply_bank(pos, _avg_fill(ex, pos.target_price or pos.entry_price), got)
                else:
                    # Nothing sold: the position is exactly as it was, so the trail
                    # keeps its target and the next touch may try again.
                    self.rec.event("partial_unfilled", ticker=pos.ticker,
                                   wanted=qty, status=st)

    def _apply_bank(self, pos: Position, price: float, qty: int) -> None:
        """The transition, applied to SOLD shares: book the leg, reduce the
        position, drop the target and send the runner to breakeven."""
        qty = min(int(qty), pos.remaining)
        if qty <= 0:
            return
        pnl_sh = (price - pos.entry_price) if pos.direction == "long" else (pos.entry_price - price)
        pos.legs.append({"kind": "target", "shares": qty, "price": round(price, 2)})
        self.guard.record_close(round(pnl_sh * qty, 2))
        self.rec.event("partial_banked", ticker=pos.ticker, shares=qty, price=round(price, 2),
                       pnl=round(pnl_sh * qty, 2), realized_session_pnl=self.guard.realized_pnl)
        pos.remaining -= qty
        self._target_shares.pop(pos.key(), None)
        tt = self._targets.pop(pos.key(), None)      # bracket-shaped books only
        if tt is not None:
            self.broker.cancel(tt)
        if pos.remaining <= 0:
            self._tick_cursor.pop(pos.key(), None)
            self.book.remove(pos)                    # partial was the whole position
            return
        pos.trail_active = True                      # runner rides the trail...
        # ...from breakeven: the stop jumps to the entry (never down).
        pos.stop_price = (max(pos.stop_price, pos.entry_price) if pos.direction == "long"
                          else min(pos.stop_price, pos.entry_price))
        self.book.update(pos)

    def _exit(self, pos: Position, reason: str, tick_price: float | None = None) -> None:
        """Cancel the resting target and submit a marketable-limit exit for the
        remainder (the software-triggered stop / flatten). tick_price, when the
        breach was detected tick-precise, is recorded for the fill comparison."""
        # ONE exit per position. The tick callback, the loop's tick backstop and
        # the bar path can all reach the same breach, and a bar stays breached for
        # its whole life -- without this guard each would submit another order and
        # the surplus fills would open an unintended position in the opposite
        # direction. Previously only the tick cursor's advance prevented this, by
        # accident; the bar path had no protection at all.
        if pos.key() in self._exiting:
            return
        tt = self._targets.pop(pos.key(), None)
        self._target_shares.pop(pos.key(), None)
        self._confirming.pop(pos.key(), None)   # superseded: this exit is the real one
        if tt is not None:
            self.broker.cancel(tt)
        # Shares already committed to an in-flight partial are NOT re-sold here: a
        # bank order is a sell on the same side, so counting them twice would fill
        # past flat and open a position in the opposite direction. It is left to
        # run rather than cancelled -- cancelling races its fill, and it is helping.
        banking = self._banking.get(pos.key())
        committed = banking[1] if banking else 0
        qty = max(pos.remaining - committed, 0)
        if qty <= 0:
            return                              # the partial already covers it all
        action = "SELL" if pos.direction == "long" else "BUY"
        # marketable limit priced through the stop so it fills but bounds slippage
        cap = (round(pos.stop_price - self.exit_slippage, 2) if pos.direction == "long"
               else round(pos.stop_price + self.exit_slippage, 2))
        ex = self.broker.marketable_limit(pos.ticker, action, qty, cap)
        self.rec.event("exit_submitted", ticker=pos.ticker, reason=reason,
                       shares=qty, cap=cap, tick_price=tick_price,
                       also_banking=committed or None,
                       source=("tick" if tick_price is not None else "bar"))
        # Settle optimistically at the cap; poll_exits reconciles the real fill.
        self._exiting[pos.key()] = (pos, ex, reason)

    def poll_exits(self) -> None:
        """Resolve submitted exits against what the broker actually did.

        An exit that reaches a terminal state having filled only part of the
        position used to sit in _exiting forever: _exit refuses to re-submit while
        a key is in there, _resolve_ticks skips those positions, and reconcile
        masks them as busy. The remainder was left with no exit in flight, no
        stop being evaluated and no alarm -- silently, and indefinitely. Release
        the key so the normal resolution can fire again on what is still open."""
        for key, (pos, ex, reason) in list(self._exiting.items()):
            st = _status(ex)
            if st == "Filled":
                self._exiting.pop(key, None)
                self._settle(pos, _avg_fill(ex, pos.stop_price), reason=reason,
                             shares=_filled_qty(ex, pos.remaining))
            elif st in TERMINAL:
                got = _filled_qty(ex, 0)
                self._exiting.pop(key, None)
                if got > 0:
                    self.rec.event("exit_partial_fill", ticker=pos.ticker, reason=reason,
                                   filled=got, wanted=pos.remaining, status=st)
                    self._settle(pos, _avg_fill(ex, pos.stop_price), reason=reason,
                                 shares=min(got, pos.remaining))
                else:
                    self.rec.event("exit_unfilled", ticker=pos.ticker, reason=reason, status=st)

    def _settle(self, pos: Position, exit_price: float, reason: str,
                shares: int | None = None) -> None:
        """Book a close for `shares` (default: the whole remaining position).

        A partial close REDUCES the position and leaves it in the book still
        managed -- closing the book entry on a partial exit fill would strand the
        rest exactly as the WETO phantom did.

        The P&L recorded here is the MODEL's: (exit - entry) x shares, gross of
        commission. It is the number the backtest computes, so it is the one
        parity is measured against. The broker's own realised figure is recorded
        separately from commissionReportEvent, and the two are reconciled in the
        end-of-session audit -- a persistent gap between them IS the execution
        divergence signal."""
        qty = int(pos.remaining if shares is None else min(shares, pos.remaining))
        if qty <= 0:
            return
        pnl_sh = (exit_price - pos.entry_price) if pos.direction == "long" else (pos.entry_price - exit_price)
        pnl = round(pnl_sh * qty, 2)
        self.guard.record_close(pnl)
        full = qty >= pos.remaining
        self.rec.event("position_closed", ticker=pos.ticker, engine=pos.entry_mode,
                       reason=reason, exit=exit_price, shares=qty,
                       partial=not full, still_open=max(pos.remaining - qty, 0),
                       pnl_per_share=round(pnl_sh, 4), pnl=pnl,
                       realized_session_pnl=self.guard.realized_pnl)
        if not full:
            pos.remaining -= qty
            self.book.update(pos)
            return
        self._targets.pop(pos.key(), None)
        self._target_shares.pop(pos.key(), None)
        self._confirming.pop(pos.key(), None)
        self._tick_cursor.pop(pos.key(), None)
        self.book.remove(pos)

    # -- flatten (09:30, kill-file, shutdown) ---------------------------------
    def flatten_all(self, reason: str) -> None:
        """Exit every open position and cancel every working entry.

        Safe to call repeatedly: _exit is a no-op for a position already exiting,
        and an entry whose cancel is already away is left alone.

        The pending entries are NOT dropped here. This used to cancel and pop them
        in the same breath, which meant a partially-filled entry at the 09:35
        flatten -- the one moment with no reconcile pass left to catch it -- was
        abandoned with real shares held and nothing tracking them. The cancel is a
        request; poll_fills adopts whatever actually filled once the broker
        answers, and _drain_exits keeps pumping until it has."""
        for pos in list(self.book.positions.values()):
            self._exit(pos, reason=reason)
        for sym, p in list(self._pending.items()):
            if p["cancel_sent"]:
                continue
            p["cancel_sent"] = True
            self.broker.cancel(p["order"])
            self.rec.event("entry_cancelled_at_flatten", ticker=sym, reason=reason,
                           shares=p["shares"], filled_so_far=_filled_qty(p["order"], 0))

    def audit_against_broker(self, ib) -> dict:
        """End-of-session sweep: every execution IBKR recorded today, reconciled
        against what the bot thinks it did.

        The commission-report stream already gives this live, so why sweep? Because
        the stream only reports what the bot was *listening for*. This asks the
        broker to enumerate the day from its own records, so it catches the cases
        the live path structurally cannot: an execution that arrived while the bot
        was reconnecting, a fill on an order the bot had already written off, a
        handler that raised. WETO was invisible for 88 seconds precisely because
        the bot was reporting its own model back to itself.

        Read-only, once, after the flatten -- no hot-path cost. Any failure is
        recorded and swallowed: an audit that takes the session down with it would
        be worse than no audit.

        `model_realized` is the bot's number, `broker_realized` IBKR's, and `delta`
        is what the backtest cannot see (commission, and any execution the model
        got wrong). Investigate a delta that is not roughly the commission total."""
        out = {"executions": 0, "shares": 0, "commission": 0.0,
               "broker_realized": 0.0, "model_realized": self.guard.realized_pnl,
               "symbols": {}}
        try:
            fills = list(ib.fills())
        except Exception as e:
            self.rec.event("broker_audit_error", error=repr(e))
            return out
        for f in fills:
            ex, rep = getattr(f, "execution", None), getattr(f, "commissionReport", None)
            sym = getattr(getattr(f, "contract", None), "symbol", None) or "?"
            shares = _num(getattr(ex, "shares", None)) or 0.0
            commission = _num(getattr(rep, "commission", None)) or 0.0
            realized = _num(getattr(rep, "realizedPNL", None)) or 0.0
            out["executions"] += 1
            out["shares"] += int(shares)
            out["commission"] = round(out["commission"] + commission, 4)
            out["broker_realized"] = round(out["broker_realized"] + realized, 4)
            s = out["symbols"].setdefault(sym, {"executions": 0, "shares": 0,
                                                "commission": 0.0, "realized": 0.0})
            s["executions"] += 1
            s["shares"] += int(shares)
            s["commission"] = round(s["commission"] + commission, 4)
            s["realized"] = round(s["realized"] + realized, 4)
        out["delta"] = round(out["broker_realized"] - out["model_realized"], 4)
        self.rec.event("broker_audit", **out)
        return out

    def has_work_in_flight(self) -> bool:
        """Anything the broker still owes us an answer on: an exit submitted but
        not resolved, or a cancelled entry that may yet report a partial fill."""
        return bool(self._exiting or self._pending or self._banking)


# --- order-status helpers (tolerant of the fake broker used in tests) ---------

def _status(trade) -> str:
    try:
        return trade.orderStatus.status
    except Exception:
        return getattr(trade, "status", "Submitted")


def _num(v) -> float | None:
    """A usable float, or None for absent/unset. IBKR fills unknown doubles with
    UNSET_DOUBLE rather than leaving them null."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f >= UNSET_DOUBLE or f != f:          # sentinel, or NaN
        return None
    return f


def _stream_cursor(stream, fallback: int) -> int:
    """The stream's current tick index. `cursor` is a PROPERTY on TickStream, so
    this reads it as one -- an earlier version called it, and because the helper
    caught every exception the resulting TypeError was swallowed into `fallback`,
    leaving the seed below a silent no-op that the whole suite still passed.

    Tolerant only of a stream that has no cursor at all (the simplified stand-ins
    in tests drive first_touch directly and carry no buffer); a cursor that exists
    but is unreadable is a real fault and is not papered over here."""
    c = getattr(stream, "cursor", None)
    if c is None:
        return int(fallback)
    return int(c)


def _filled_qty(trade, fallback: int) -> int:
    """Shares the broker says actually filled. `fallback` is used only when the
    field is unreadable (the fake broker in tests doesn't model it).

    Read this, never the shares we ASKED for. A cancelled order that filled part
    way is the entry-side twin of the WETO phantom: real shares, held, with no
    position in the book and no synthetic stop watching them."""
    try:
        q = trade.orderStatus.filled
        if q is not None:
            return int(q)
    except Exception:
        pass
    return int(fallback)


def _avg_fill(trade, fallback: float) -> float:
    try:
        f = float(trade.orderStatus.avgFillPrice)
        if f > 0:
            return f
    except Exception:
        pass
    return float(fallback)
