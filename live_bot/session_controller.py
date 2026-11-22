"""Session controller (PORTING_TO_LIVE.md §6/§7): blackout gate -> connect
(read-only) -> discovery/confirm/stream/evaluate loop -> session end. The
priority ladder is trivial in the observer (no positions exist): scanner polls
are the lowest rung and everything else is per-symbol evaluation."""
from __future__ import annotations

import time as systime
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from .broker import Broker
from .coverage import CoverageManager
from .discovery import (
    FloatCache, close_scanner, daily_volume_baseline, open_scanner, read_scanner,
)
from .ib_errors import install_error_triage
from .ibcompat import IB, IB_LIB, Stock
from .armed_watch import LiveArmedWatch
from .live_data import ET, SymbolFeed
from .order_manager import OrderManager
from .position_state import PositionBook
from .recorder import Recorder
from .risk_guard import RiskGuard
from .slot_manager import SlotManager
from .signal_path import attach_signal_core
from .signal_runtime import SignalRuntime


# The strategy set this bot implements. Single-strategy since 2026-08-16; the
# wedge was removed from the live path, and _require_dip_only refuses to start if
# the parity anchor ever selects anything else.
STRATEGIES = ("dip",)


def _hhmm(s: str) -> dtime:
    h, m = s.split(":")
    return dtime(int(h), int(m))


def _secs_until(now_et: datetime, target: dtime) -> float:
    """Seconds from now until `target` clock time TODAY (>=1, never negative)."""
    tgt = now_et.replace(hour=target.hour, minute=target.minute, second=0, microsecond=0)
    return max((tgt - now_et).total_seconds(), 1.0)


def _next_window_start(now_et: datetime, start_t: dtime) -> datetime:
    """The next session start instant, in ET -- today if it hasn't passed, else
    tomorrow. Computed on the ET clock so DST shifts are handled by the zone,
    never by arithmetic on a naive time."""
    today = now_et.replace(hour=start_t.hour, minute=start_t.minute, second=0, microsecond=0)
    return today if now_et < today else today + timedelta(days=1)


def _require_dip_only(sc, cfg) -> None:
    """This bot trades ONE strategy: dip_red_break. Refuse to start otherwise.

    The strategy set still lives in the backtester's config.yaml -- the shared
    parity anchor -- and signal_core.active_strategies() is still its one truth
    table. What changed on 2026-08-16 is that the bot no longer has a wedge
    implementation to honour it with: no LiveWedgeWatch, no wedge engine pass, no
    wedge sizing caps.

    That makes re-enabling the wedge in the anchor a silent Class-0 divergence --
    the backtest would trade two engines while the live book traded one, and the
    parity numbers the whole project rests on would be comparing different
    strategies. There is no safe way to continue, so this stops the session
    before a single order is placed, exactly as the pin and paper-account guards
    do. Re-adding the wedge means restoring the bot-side implementation, not
    deleting this check."""
    active = tuple(sc.active_strategies(cfg.wedge))
    if active != ("dip",):
        raise SystemExit(
            f"STRATEGY MISMATCH: the anchor selects {active} but this bot implements "
            "('dip',) only -- the wedge was removed from the live path on 2026-08-16.\n"
            "Refusing to run: the live book would trade a different strategy set than "
            "the backtest it is validated against.\n"
            "Set wedge.enabled: false in the backtester's config.yaml, or restore the "
            "bot-side wedge implementation.")


class Observer:
    def __init__(self, config_path: Path):
        self.root = config_path.parent.resolve()
        self.bcfg = yaml.safe_load(config_path.read_text())
        self.sc, self.signal_sha, self.signal_dirty = attach_signal_core(
            self.bcfg["signal"]["backtester_path"], self.bcfg["signal"]["pin"], self.root)
        # THE strategy config: the backtester repo's config.yaml, via signal_core.
        bt_root = (self.root / self.bcfg["signal"]["backtester_path"]).resolve()
        self.cfg = self.sc.load_config(str(bt_root / "config.yaml"))
        _require_dip_only(self.sc, self.cfg)
        # The ET calendar date, never the host's local date: this value names the
        # recording, and is the `day` handed to the engines. On a non-ET host
        # (this project is developed from the UK) local and ET dates diverge
        # outside the window, and a wrong day would mis-key everything.
        self.today = datetime.now(ZoneInfo(ET)).date()
        # TRADING master switch. When off this is a pure read-only observer;
        # readonly is DERIVED from it so the two can never contradict.
        self.trading = bool((self.bcfg.get("trading", {}) or {}).get("enabled", False))
        self.readonly = not self.trading
        self.rec = Recorder(self.root / self.bcfg["recording"]["dir"], self.today.isoformat())
        # `strategies` replaces the old wedge_enabled/wedge_concurrent pair: it
        # states what actually ran rather than the two flags a reader had to
        # re-derive it from. session_view still understands the old pair so
        # recordings from before 2026-08-16 keep rendering.
        self.rec.event("session_meta", signal_sha=self.signal_sha, ib_lib=IB_LIB,
                       signal_dirty=self.signal_dirty, strategies=list(STRATEGIES),
                       pin=self.bcfg["signal"]["pin"],
                       readonly=self.readonly, trading=self.trading)
        self.floats = FloatCache(bt_root / "data" / "reference_floats.csv",
                                 self.root / self.bcfg["state"]["dir"])
        # Live trigger-watch (Phase B): pre-arms dip buy parameters and fires the
        # entry on the tick cross. Only in trading mode -- an observer records
        # decisions, it doesn't need to race the trigger.
        self.watch = LiveArmedWatch(self.sc, self.cfg) if self.trading else None
        self.runtime = SignalRuntime(self.sc, self.cfg, self.rec, watch=self.watch)
        self.state_dir = self.root / self.bcfg["state"]["dir"]
        self.guard = RiskGuard(self.bcfg, self.state_dir)
        self.book = PositionBook(self.state_dir)
        self.orders = None            # OrderManager, created after connect (needs account)
        self.flattened = False
        self.ib = IB()
        # Triage IBKR's error stream: benign status/scanner-cancel codes go to the
        # recording quietly; a real error still reaches the console. Without this
        # the scanner-cancel (162) prints on every poll and buries real errors.
        install_error_triage(self.ib, self.rec)
        self.feeds: dict[str, SymbolFeed] = {}   # Tier B: the STREAMED symbols
        self.baselines: dict[str, float | None] = {}   # cached across demote/re-promote
        self.scanner_first_seen: dict[str, str] = {}
        self.scan_data = None            # the persistent scanner subscription
        self._snap_contracts: dict[str, object] = {}   # qualified contracts for snapshots
        # Tier C/B coverage: the wide monitor + promote/demote decision core, so
        # the mover universe is never lost to a flat cap. max_streamed is the
        # RealTimeBars line budget; the monitor beneath it is unbounded and never
        # forgets a symbol the scanner surfaced.
        d = self.bcfg["discovery"]
        self.coverage = CoverageManager(
            max_streamed=int(d.get("max_streamed", d.get("max_tracked", 20))),
            max_promote_per_poll=int(d.get("max_promote_per_poll", 10)),
            max_demote_per_poll=int(d.get("max_demote_per_poll", 3)))
        self._max_snapshots = int(d.get("max_snapshots", 2 * self.coverage.max_streamed))

    # -- lifecycle ----------------------------------------------------------
    def connect(self) -> None:
        g = self.bcfg["gateway"]
        # Retry the initial connect: after a hard restart (NSSM/Task Scheduler
        # relaunch, or a killed process) TWS/Gateway can still hold the previous
        # session's clientId for a few seconds (error 326, "client id already in
        # use"), and Gateway may not have finished coming up. Backoff rather than
        # dying on the first attempt -- a bot that won't restart cleanly is
        # useless on a box that restarts it.
        last = None
        for i in range(12):
            try:
                self.ib.connect(g["host"], int(g["port"]), clientId=int(g["client_id"]),
                                readonly=self.readonly, timeout=20)
                self.rec.event("connected", host=g["host"], port=g["port"],
                               attempt=i + 1, readonly=self.readonly)
                if self.trading:
                    self._arm_trading()
                return
            except Exception as e:
                last = e
                self.rec.event("connect_retry", attempt=i + 1, error=repr(e))
                systime.sleep(min(5 * (i + 1), 30))
        raise ConnectionError(f"could not connect to {g['host']}:{g['port']} after 12 attempts: {last!r}")

    def _arm_trading(self) -> None:
        """Stand up the order layer after a writable connect: bind the account
        (paper-guard), build the order manager, and RECONCILE the durable book
        against what the broker actually holds -- crash adoption. Any mismatch is
        surfaced loudly; near a client-side synthetic stop it's the highest-
        severity condition there is."""
        accounts = self.ib.managedAccounts() or []
        account = accounts[0] if accounts else ""
        self.guard.bind_account(account)          # raises SystemExit on a non-paper account
        self.broker = Broker(self.ib)
        # Tick-by-tick slot manager: the 'zoom in on priority stocks' engine.
        # Phase A promotes OPEN POSITIONS to tick-precise stop/target watching.
        # On a reconnect the old streams reference the dead socket, so drop them;
        # the next loop re-promotes the still-open positions on the new socket.
        if getattr(self, "slots", None) is not None:
            self.slots.streams.clear()
        tcfg = self.bcfg.get("trading", {}) or {}
        self.slots = SlotManager(self.ib, self.broker, self.rec,
                                 max_slots=int(tcfg.get("max_tick_slots", 3)), bar_seconds=10)
        self.orders = OrderManager(self.broker, self.sc, self.cfg, self.bcfg,
                                   self.book, self.guard, self.rec, account, slots=self.slots)
        # Fills arrive as EVENTS, not on the next poll. Re-attached on every
        # re-arm because a reconnect builds a new socket and drops the old
        # subscriptions; the loop's polls stay as the backstop either way.
        self.orders.attach_events(self.ib)
        # ...and route every tick straight into the risk layer, so a stop resolves
        # on the crossing print rather than on the next pass of the ~2s loop.
        self.slots.on_ticks = self.orders.on_ticks
        for st in self.slots.streams.values():
            st.on_ticks = self.orders.on_ticks
        self.ib.sleep(1)                          # let positions() populate
        alarms = self.book.reconcile(self.broker.positions(), self.rec)
        for a in alarms:
            print(f"RECONCILE: {a}", flush=True)
        self.rec.event("trading_armed", account=account, adopted=self.book.open_count())
        print(f"[trading ARMED] account={account} (paper)  adopted {self.book.open_count()} open position(s)", flush=True)

    def _reconnect(self, attempts: int = 30) -> bool:
        """Backoff reconnect during a session (Gateway restart, network blip).
        A dropped link with a position OPEN means the synthetic stop is
        unenforced until we're back -- so reconnecting fast is safety-critical,
        and on return we re-arm trading (which reconciles the book against the
        broker) rather than assuming our view is still correct."""
        g = self.bcfg["gateway"]
        for i in range(attempts):
            delay = min(5 * (i + 1), 60)
            systime.sleep(delay)
            try:
                self.ib.connect(g["host"], int(g["port"]), clientId=int(g["client_id"]),
                                readonly=self.readonly, timeout=20)
                self.rec.event("reconnected", attempt=i + 1)
                if self.trading:
                    self._arm_trading()           # re-bind + reconcile after the gap
                # The scanner subscription died with the socket; drop the handle
                # so the next poll re-opens it.
                self.scan_data = None
                # Streams die with the socket: re-subscribe everything tracked.
                for feed in self.feeds.values():
                    try:
                        feed.start_stream()
                    except Exception:
                        pass
                return True
            except Exception:
                continue
        return False

    def blackout_check(self) -> str | None:
        reason = self.sc.blackout_reason(self.today, self.cfg.blackout)
        if reason:
            self.rec.event("blackout", reason=reason)
        return reason

    # -- discovery ----------------------------------------------------------
    def poll_scanner(self) -> None:
        d = self.bcfg["discovery"]
        try:
            # Open the persistent subscription on first poll; thereafter just read
            # its live contents -- no per-poll subscribe/cancel, so no 162 flood.
            if self.scan_data is None:
                self.scan_data = open_scanner(self.ib, d)
                self.ib.sleep(2)         # let the first snapshot populate
            ranked = read_scanner(self.scan_data)   # symbols in %-gain rank order
        except Exception as e:
            self.rec.event("scanner_error", error=repr(e))
            return
        for s in ranked:
            if s not in self.scanner_first_seen:
                self.scanner_first_seen[s] = datetime.now(ZoneInfo(ET)).isoformat(timespec="seconds")
                self.rec.event("scanner_seen", symbol=s)
        self._reconcile_coverage(ranked)

    # -- Tier C/B coverage ---------------------------------------------------
    def _reconcile_coverage(self, ranked: list[str]) -> None:
        """Fold the scanner read into the monitor, take cheap snapshots for the
        strategy early-warning, then promote/demote streams. Positions and armed
        setups are pinned (never demoted); a demoted symbol stays in the monitor
        and can re-promote later. This is what replaces the flat-cap confirm loop:
        the streamed set always tracks the CURRENT top movers instead of freezing
        on whoever appeared first."""
        self.coverage.observe_scanner(ranked)
        self._snapshot_monitor()
        pinned = self._pinned_symbols()
        promote, demote = self.coverage.plan(pinned)
        for s in demote:
            self.demote(s)
            self.coverage.mark_demoted(s)
        for s in promote:
            reason = self.coverage.monitor[s].warning if s in self.coverage.monitor else None
            self.confirm(s, reason=reason)
            self.coverage.mark_promoted(s)

    def _pinned_symbols(self) -> set[str]:
        """Symbols that MUST keep their stream: open positions (their client-side
        synthetic stop needs the live feed) and armed setups (we're watching the
        trigger). Never eligible for demotion."""
        pinned = {p.ticker for p in self.book.positions.values()}
        if self.watch is not None:
            pinned |= set(self.watch.armed)
        return pinned

    def _snapshot_monitor(self) -> None:
        """Cheap snapshot quotes for the top monitored-but-not-streamed symbols,
        feeding the dip early-warning. Best-effort: any failure just leaves
        the early-warning cold and promotion falls back to %-gain rank -- still
        lossless in awareness, only a touch more latency."""
        targets = self.coverage.snapshot_targets(self._max_snapshots)
        if not targets:
            return
        try:
            contracts = [self._snap_contract(s) for s in targets]
            tickers = self.ib.reqTickers(*contracts)
        except Exception as e:
            self.rec.event("snapshot_error", error=repr(e))
            return
        for s, t in zip(targets, tickers):
            try:
                px = t.marketPrice()
                if px != px or px is None or px <= 0:   # NaN / missing
                    px = getattr(t, "last", None)
                if px and px > 0:
                    self.coverage.ingest_snapshot(s, float(px))
            except Exception:
                continue

    def _snap_contract(self, symbol: str):
        c = self._snap_contracts.get(symbol)
        if c is None:
            c = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(c)
            self._snap_contracts[symbol] = c
        return c

    def confirm(self, symbol: str, reason: str | None = None) -> None:
        """PROMOTE a symbol to a streamed Tier-B feed: backfill the whole session-
        so-far (so a late promotion reconstructs the exact HOD/coil the backtest
        would have seen) then start the RealTimeBars stream. The daily-volume
        baseline is cached, so a re-promotion after a demote doesn't re-fetch it."""
        base = self.baselines.get(symbol)
        if base is None:
            base = daily_volume_baseline(self.ib, symbol, self.cfg.strategy, self.today)
            self.baselines[symbol] = base
        fl = self.floats.get(symbol)
        feed = SymbolFeed(self.ib, symbol, _hhmm(self.bcfg["session"]["start_et"]), self.rec,
                          session_date=self.today)
        n = feed.backfill()
        feed.start_stream()
        self.feeds[symbol] = feed
        self.rec.event("confirm", symbol=symbol, daily_avg_volume=base,
                       float_shares=fl, backfill_bars=n, reason=reason)

    def demote(self, symbol: str) -> None:
        """DEMOTE a streamed symbol back to the cheap monitor: stop its stream and
        drop its feed so it's no longer evaluated, but keep it in the coverage
        monitor (and its cached baseline) so it can re-promote instantly. Never
        called on a pinned symbol -- the coverage plan excludes those.

        The frame is recorded BEFORE the feed is released: dropping it silently
        would delete the only copy of what that symbol's bars looked like."""
        feed = self.feeds.pop(symbol, None)
        if feed is not None:
            feed.stop_stream()
            self._record_frame(symbol, feed)
        self.rec.event("demoted", symbol=symbol)

    def _record_frame(self, symbol: str, feed) -> None:
        """Dump a feed's session-so-far into the recording. Called from EVERY path
        that releases a feed -- demote as well as session end -- because the parity
        replay rebuilds from the recorded frame and can only see what was written.

        Demote used to drop the feed without recording, which was invisible while
        nothing traded (the bars just vanished) but is a live trap: pinning covers
        open positions and armed setups only, so a symbol that fires, closes and
        disarms becomes demotion-eligible with its trade already in the recording.
        Its frame would then never be written, the replay could not reproduce the
        trade, and parity_diff would report a phantom Class-0 `missing_on_replay`
        -- on the first session that actually traded, which is the worst possible
        time to be debugging the parity harness.

        A symbol may now be dumped more than once (demote, then again at session
        end after a re-promotion). That is intentional: re-promotion backfills to
        the session start so the later dump is normally a superset, and the replay
        unions them on dt_et rather than trusting either one to be complete."""
        f = feed.frame()
        self.rec.event("frame", symbol=symbol,
                       bars=[[str(r.dt_et), r.open, r.high, r.low, r.close, r.volume]
                             for r in f.itertuples()])

    # -- main loop ----------------------------------------------------------
    def run(self, once: bool = False, wait_for_window: bool = False) -> None:
        """Observe ONE session. With wait_for_window, idle (no connection, no
        IBKR requests) until the session window opens rather than scanning a
        dead premarket -- `serve` uses this; a bare `run` starts immediately so
        an ad-hoc mid-session start still works."""
        if self.blackout_check():
            print("Blackout day -- not observing.")
            return
        start_t = _hhmm(self.bcfg["session"]["start_et"])
        end_t = _hhmm(self.bcfg["session"]["end_et"])
        if wait_for_window:
            while True:
                now_et = datetime.now(ZoneInfo(ET))
                if now_et.time() >= start_t:
                    break
                if now_et.time() >= end_t:
                    return          # window already closed for today
                systime.sleep(min(30, _secs_until(now_et, start_t)))
        now_et = datetime.now(ZoneInfo(ET))
        if now_et.time() >= end_t:
            print(f"Session window {self.bcfg['session']['start_et']}-{self.bcfg['session']['end_et']} ET "
                  f"has already closed today ({now_et:%H:%M} ET). Use `serve` to wait for the next one.")
            return
        self.connect()
        poll_s = int(self.bcfg["discovery"]["poll_seconds"])
        last_poll = 0.0
        acted = 0                     # intended trades already routed to the order manager
        killed = False
        try:
            while True:
                now_et = datetime.now(ZoneInfo(ET))
                if now_et.time() >= end_t:
                    self.rec.event("session_end", reason="end_of_window")
                    break
                # Supervised reconnect. In TRADING mode a dropped link means the
                # synthetic stop on any open position is unenforced until we're
                # back -- reconnecting fast is safety-critical, and _reconnect
                # re-arms + reconciles.
                if not self.ib.isConnected():
                    self.rec.event("disconnected")
                    if not self._reconnect():
                        self.rec.event("session_end", reason="reconnect_failed")
                        break
                if systime.monotonic() - last_poll >= poll_s:
                    self.poll_scanner()
                    last_poll = systime.monotonic()
                for sym, feed in self.feeds.items():
                    if feed.new_data:
                        feed.new_data = False
                        self.runtime.evaluate(sym, self.today, feed.frame(),
                                              self.baselines.get(sym), self.floats.get(sym))

                # --- ORDER LAYER (trading mode only) -------------------------
                if self.trading and self.orders is not None:
                    # Kill-file: the operator's manual stop. Flatten everything,
                    # place no more entries, but keep managing exits to completion.
                    if not killed and self.guard.kill_requested():
                        killed = True
                        self.rec.event("kill_file", path=str(self.guard.kill_file))
                        print("KILL FILE present -- flattening and halting new entries.", flush=True)
                        self.orders.flatten_all(reason="kill_file")
                    # Zoom in: hand the scarce tick slots to OPEN POSITIONS first
                    # (their client-side stop is the money-losing path) then to
                    # ARMED setups approaching their trigger (so we can watch the
                    # cross tick-by-tick). Priority order: positions, then armed.
                    open_syms = [p.ticker for p in self.book.positions.values()]
                    armed_syms = [s for s in (self.watch.armed if self.watch else {})
                                  if s not in open_syms]
                    self.slots.sync(open_syms + armed_syms)

                    # FIRE: for each armed setup with a tick slot, check whether the
                    # forming bar has crossed the trigger; on a fire, place the entry
                    # NOW (sub-second) rather than waiting for the bar-detected path.
                    if not killed and self.watch is not None:
                        for sym in list(self.watch.armed):
                            stream = self.slots.get(sym)
                            feed = self.feeds.get(sym)
                            if stream is None or feed is None:
                                continue
                            forming = stream.forming_bar()
                            if forming is None:
                                continue
                            dec = self.watch.check_cross(sym, feed.frame(), forming,
                                                         self.baselines.get(sym), stream)
                            if dec is not None and dec.kind == "fire":
                                trade = self._fire_to_trade(sym, dec)
                                self.rec.event("tick_fire", ticker=sym, trigger=trade["trigger_price"],
                                               fill=dec.fill, stop=dec.stop, target=dec.target)
                                self.orders.enter(trade)

                    # Route any NEW bar-detected intended trades too. The order
                    # manager's idempotency means whichever path fired first (the
                    # tick-watch, usually) wins and this is a no-op for that setup.
                    if not killed:
                        for tr in self.runtime.intended[acted:]:
                            self.orders.enter(tr)
                    acted = len(self.runtime.intended)
                    # Advance order lifecycle: fills -> positions, stop/target watch, exit fills.
                    self.orders.poll_fills()
                    self.orders.manage(self.feeds)
                    self.orders.poll_exits()
                    self.orders.poll_banks()
                    # Then re-check the book against what the broker ACTUALLY
                    # holds. The bot's model of its own risk can drift from the
                    # account (a resting target filling is the common way), and a
                    # drifted book is how a flatten ends up selling shares that
                    # aren't there. Rate-limited internally; in-flight symbols are
                    # skipped so a fresh fill is never mistaken for a vanished one.
                    self.orders.reconcile_open()

                if once:
                    self.rec.event("session_end", reason="--once")
                    break
                self.ib.sleep(2)   # yields to the IB event loop (RealTimeBars callbacks)
        finally:
            # exit_at_regular_open: flatten any open position at the close of the
            # window before disconnecting -- the strategy never carries premarket
            # positions into the regular session.
            if self.trading and self.orders is not None and not self.flattened:
                self.orders.flatten_all(reason="session_end")
                self._drain_exits()
                self.flattened = True
                # Ask the BROKER what the day actually was, once everything has
                # settled -- the model's P&L reported back to itself is exactly
                # what made WETO invisible for 88 seconds.
                if self.ib.isConnected():
                    self._report_audit(self.orders.audit_against_broker(self.ib))
            if getattr(self, "slots", None) is not None:
                self.slots.release_all()
            close_scanner(self.ib, self.scan_data)   # the session's ONE 162
            for sym, feed in self.feeds.items():
                feed.stop_stream()
                # Full session frame into the recording: the parity replay
                # rebuilds from THIS (the recorded feed), never a refetch.
                self._record_frame(sym, feed)
            self.rec.event("summary", tracked=len(self.feeds),
                           intended_trades=len(self.runtime.intended),
                           scanner_seen=len(self.scanner_first_seen),
                           trading=self.trading,
                           realized_pnl=(self.guard.realized_pnl if self.trading else None),
                           broker_realized_pnl=(self.orders.broker_realized
                                                if (self.trading and self.orders) else None),
                           commission=(self.orders.commission_total
                                       if (self.trading and self.orders) else None),
                           closed_trades=(self.guard.closed_trades if self.trading else None),
                           open_at_end=(self.book.open_count() if self.trading else None))
            self.rec.close()
            try:
                self.ib.disconnect()
            except Exception:
                pass
            # End-of-session email summary (optional; off unless email: configured).
            # Runs ONCE here, after the recording is closed -- zero cost during the
            # session -- and never raises, so a mail problem can't affect trading or
            # the daemon. The recorder is already closed, so it logs to the console.
            try:
                from .notifier import send_session_summary
                send_session_summary(self.bcfg, self.rec.path, root=self.root,
                                     logger=lambda k, **p: print(f"[email] {k} {p}", flush=True))
            except Exception:
                pass
        mode = "TRADING" if self.trading else "observer"
        tail = (f", realized ${self.guard.realized_pnl:+.2f} over {self.guard.closed_trades} closed"
                if self.trading else "")
        print(f"{mode} session done: {len(self.feeds)} tracked, "
              f"{len(self.runtime.intended)} signals{tail} -> {self.rec.path}")

    def _fire_to_trade(self, sym: str, dec) -> dict:
        """A fired trigger-watch decision -> the intended-trade dict the order
        manager enters. Dip-only (the live trigger-watch is the dip strategy's),
        long. trigger_price comes from the armed setup so the order manager
        places its marketable limit at trigger + slippage, exactly as the
        bar-detected path would."""
        a = self.watch.armed.get(sym)
        return {
            "ticker": sym, "date": self.today.isoformat(), "entry_mode": "dip_red_break",
            "direction": "long", "setup_index": 1,
            "entry_price": dec.fill, "stop_price": dec.stop, "target_price": dec.target,
            "trigger_price": a.trigger if a is not None else dec.fill,
            "volume_at_entry": dec.volume_at_entry,
        }

    def _report_audit(self, a: dict) -> None:
        """Print the broker-vs-model reconciliation. A delta that is roughly the
        commission total is expected (the model is gross of fees); anything else
        means the bot's picture of its own trading is wrong somewhere."""
        if not a.get("executions"):
            return
        expected = -a.get("commission", 0.0)
        drift = a.get("delta", 0.0) - expected
        print(f"BROKER AUDIT: {a['executions']} execution(s), {a['shares']} shares, "
              f"commission {a['commission']:.2f}", flush=True)
        print(f"  model realized {a['model_realized']:+.2f}   "
              f"broker realized {a['broker_realized']:+.2f}   delta {a['delta']:+.2f}", flush=True)
        if abs(drift) > 1.0:
            self.rec.event("broker_audit_drift", drift=round(drift, 4), **a)
            print(f"  WARNING: {drift:+.2f} beyond commission -- the bot's model of its own "
                  f"trading disagrees with the account. Investigate before the next session.",
                  flush=True)

    def _drain_exits(self, max_wait_s: int = 20) -> None:
        """After a flatten, spin the event loop briefly so orders resolve and
        settle (realized P&L, book cleared) before we disconnect.

        Pumps poll_fills as well as poll_exits, and re-flattens: a cancelled entry
        can still report a partial fill after the flatten, and that fill is a real
        position that must be adopted AND then exited. Draining only the exits
        left those shares held overnight with the session already closed."""
        if self.orders is None:
            return
        deadline = systime.monotonic() + max_wait_s
        while (self.book.open_count() > 0 or self.orders.has_work_in_flight())                 and systime.monotonic() < deadline and self.ib.isConnected():
            self.orders.poll_fills()          # adopt anything that filled late
            self.orders.poll_banks()          # settle any partial still in flight
            self.orders.flatten_all(reason="session_end")   # exit it (no-op if already going)
            self.orders.poll_exits()
            self.ib.sleep(1)
        if self.book.open_count() > 0:
            self.rec.event("flatten_incomplete", still_open=self.book.open_count())
            print(f"WARNING: {self.book.open_count()} position(s) still open after flatten drain -- "
                  f"check the account manually.", flush=True)


def serve(config_path: Path) -> None:
    """DAEMON MODE -- the way the VPS actually runs this.

    Stays up indefinitely: sleeps outside the premarket window (no connection,
    no IBKR requests), wakes for each session, records it, then sleeps until the
    next one. Blackout days are skipped without connecting. A crash inside one
    session is logged and the loop continues to the next day rather than taking
    the process down -- an observer that dies silently on a Tuesday and is
    noticed on Friday has lost four sessions of parity evidence.

    A fresh Observer is built per session so each day gets its own recording,
    its own float cache read, and re-verifies the signal pin (a mid-week
    backtester commit must not silently change what the bot evaluates).
    """
    bcfg = yaml.safe_load(config_path.read_text())
    start_t = _hhmm(bcfg["session"]["start_et"])
    end_t = _hhmm(bcfg["session"]["end_et"])
    # Reflect the ACTUAL mode -- trading (places paper orders) vs observer -- so the
    # daemon banner can't claim "read-only" while it's arming orders.
    trading = bool((bcfg.get("trading", {}) or {}).get("enabled", False))
    mode = "TRADING (paper-account-guarded)" if trading else "read-only observer"
    print(f"live_bot serve: premarket window {bcfg['session']['start_et']}-"
          f"{bcfg['session']['end_et']} ET, {mode}. Ctrl-C to stop.")
    # The ET date whose session has already been handled. Without this, ANY
    # early return from run() -- a blackout day, a failed reconnect, a crash --
    # drops back into the loop still inside the window, re-enters, and repeats
    # every 60s for the rest of the window (measured: 335 Observer
    # constructions on one blackout day, each appending session_meta to the same
    # recording). One session per ET date, then wait for the next.
    done_for: date | None = None

    def _due(now_et: datetime) -> bool:
        """Is a session owed right now? Inside the window AND not already run
        for this ET date. The done_for half is what stops an early return from
        re-entering for the rest of the window."""
        return start_t <= now_et.time() < end_t and now_et.date() != done_for

    while True:
        try:
            now_et = datetime.now(ZoneInfo(ET))
            if not _due(now_et):
                nxt = _next_window_start(now_et, start_t)
                # ASCII only in console output: the Windows console codepage
                # mangles non-ASCII, and this line is what an operator reads on
                # an RDP session to confirm the daemon is alive.
                print(f"[{now_et:%Y-%m-%d %H:%M} ET] idle - next session {nxt:%Y-%m-%d %H:%M} ET "
                      f"(in {(nxt - now_et).total_seconds() / 3600:.1f}h)", flush=True)
                # Wake condition is _due(), NOT merely "inside the window", so a
                # day already handled (or skipped) stays asleep until tomorrow.
                # Short chunks keep Ctrl-C responsive and re-evaluate the clock
                # (DST, suspend/resume) rather than sleeping through a jump.
                while not _due(datetime.now(ZoneInfo(ET))):
                    systime.sleep(60)
            now_et = datetime.now(ZoneInfo(ET))
            # Claim the day BEFORE running it: a crash mid-session must back off
            # to tomorrow, not retry a broken session in a tight loop.
            done_for = now_et.date()
            print(f"[{now_et:%Y-%m-%d %H:%M} ET] session start", flush=True)
            Observer(config_path).run(wait_for_window=False)
            print(f"[{datetime.now(ZoneInfo(ET)):%Y-%m-%d %H:%M} ET] session complete", flush=True)
        except KeyboardInterrupt:
            print("\nlive_bot serve: stopped by user.")
            return
        except Exception as e:
            print(f"[{datetime.now(ZoneInfo(ET)):%Y-%m-%d %H:%M} ET] SESSION ERROR: {e!r}", flush=True)
            systime.sleep(60)
