"""Order-layer safety tests -- the whole point of this file is that order code
that can't be live-tested until Monday must be trustworthy NOW. Everything runs
against a fake broker; the guards and lifecycle are exercised end to end.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot.order_manager import OrderManager
from live_bot.position_state import Position, PositionBook
from live_bot.risk_guard import RiskGuard


# --- fakes --------------------------------------------------------------------

class _Rec:
    def __init__(self):
        self.events = []

    def event(self, kind, **p):
        self.events.append((kind, p))


class _FakeOrderTrade:
    def __init__(self, status="Submitted", fill=0.0):
        self.order = SimpleNamespace()
        self.orderStatus = SimpleNamespace(status=status, avgFillPrice=fill)


class _FakeBroker:
    """Records every order action; lets a test drive fills deterministically."""
    def __init__(self, account_vals=None):
        self.submitted = []
        self.cancelled = []
        self._pos = {}
        self._acct = account_vals or {"NetLiquidation": 5000.0, "BuyingPower": 8000.0}

    def marketable_limit(self, ticker, action, qty, limit):
        t = _FakeOrderTrade(status="Filled", fill=limit)   # fills at the cap by default
        self.submitted.append(("marketable", ticker, action, qty, limit, t))
        return t

    def resting_limit(self, ticker, action, qty, limit):
        t = _FakeOrderTrade(status="Submitted")            # rests until a test fills it
        self.submitted.append(("resting", ticker, action, qty, limit, t))
        return t

    def cancel(self, trade):
        self.cancelled.append(trade)

    def positions(self):
        return dict(self._pos)

    def account_value(self, tag, account=None):
        return self._acct.get(tag)


class _FakeFeed:
    def __init__(self, df):
        self._df = df

    def frame(self):
        return self._df


def _cfg():
    """Minimal stand-in for the backtester AppConfig fields the order manager uses."""
    return SimpleNamespace(
        strategy=SimpleNamespace(entry_limit_slippage_cents=0.20, exit_mode="standard",
                                 trail_partial_at_target=0.5, fatgreen_body_min=0.7),
        risk=SimpleNamespace(starting_equity=5000.0, position_pct_of_equity=76.0,
                             risk_pct_of_equity=1.6, max_pct_of_volume_at_entry=3.0,
                             shares=100, max_leverage=1.6),
        wedge=SimpleNamespace(max_pct_of_volume_at_entry=1.0, exit_mode="bracket",
                              trail_partial_at_target=0.5,
                              max_entry_pct_of_lookback_volume=None),
    )


class _SC:
    """signal_core stand-in exposing capped_shares (risk cap + the wedge's
    lookback-window cap, mirroring _capped_shares' min-of-caps shape)."""
    @staticmethod
    def capped_shares(equity, entry, stop, vol, risk, vol_pct=None,
                      lookback_volume=None, lookback_pct=None):
        caps = [("risk", int((equity * risk.risk_pct_of_equity / 100.0) // abs(entry - stop)))]
        if lookback_pct is not None and lookback_volume:
            caps.append(("lookback_vol", int(lookback_volume * lookback_pct / 100.0)))
        return min(caps, key=lambda c: c[1])


def _bcfg(**trading):
    base = {"require_paper_account": True, "sizing": "config", "fixed_notional": 200,
            "max_open_positions": None, "kill_file": "STOP"}
    base.update(trading)
    return {"trading": base}


def _om(tmp_path, broker=None, bcfg=None, account="DU123"):
    broker = broker or _FakeBroker()
    guard = RiskGuard(bcfg or _bcfg(), tmp_path / "state")
    book = PositionBook(tmp_path / "state")
    om = OrderManager(broker, _SC(), _cfg(), bcfg or _bcfg(), book, guard, _Rec(), account)
    return om, broker, guard, book


def _trade(ticker="VIVK", mode="dip_red_break", direction="long",
           entry=2.99, stop=2.92, target=3.06, trigger=2.99):
    return {"ticker": ticker, "date": "2026-07-27", "entry_mode": mode,
            "direction": direction, "setup_index": 1, "entry_price": entry,
            "stop_price": stop, "target_price": target, "trigger_price": trigger,
            "volume_at_entry": 500000.0}


# --- paper-account guard (the one that stands between paper and real money) ----

def test_paper_guard_refuses_a_live_account(tmp_path):
    guard = RiskGuard(_bcfg(), tmp_path / "state")
    with pytest.raises(SystemExit):
        guard.bind_account("U1234567")          # live prefix
    guard.bind_account("DU7654321")              # paper -> ok


def test_paper_guard_can_be_disabled_only_deliberately(tmp_path):
    guard = RiskGuard(_bcfg(require_paper_account=False), tmp_path / "state")
    guard.bind_account("U1234567")              # allowed ONLY because explicitly off


# --- idempotency: a setup acted on once never re-submits -----------------------

def test_entry_is_idempotent_per_setup(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    tr = _trade()
    om.enter(tr)
    om.enter(tr)                                # same setup again (re-eval / reconnect)
    om.enter(tr)
    assert sum(1 for s in broker.submitted if s[0] == "marketable") == 1


def test_no_second_entry_while_already_in_that_name(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade())
    om.poll_fills()                             # -> open position VIVK
    om.enter(_trade(mode="wedge_break"))        # different setup, same ticker
    entries = [s for s in broker.submitted if s[0] == "marketable"]
    assert len(entries) == 1                    # still just the one VIVK entry


# --- sizing: config path uses broker equity, clamps to BuyingPower -------------

def test_config_sizing_clamps_to_buying_power(tmp_path):
    broker = _FakeBroker({"NetLiquidation": 100000.0, "BuyingPower": 300.0})
    om, broker, guard, book = _om(tmp_path, broker=broker)
    om.enter(_trade(entry=3.00))
    _, _, action, qty, limit, _ = next(s for s in broker.submitted if s[0] == "marketable")
    assert qty == 100                           # 300 buying power / 3.00 = 100, not the risk-cap size


def test_fixed_sizing_uses_notional(tmp_path):
    om, broker, guard, book = _om(tmp_path, bcfg=_bcfg(sizing="fixed", fixed_notional=200))
    om.enter(_trade(entry=2.99))
    _, _, _, qty, _, _ = next(s for s in broker.submitted if s[0] == "marketable")
    assert qty == 66                            # floor(200/2.99)


# --- entry limit = trigger +/- slippage (the fill-cap protection) --------------

def test_entry_limit_is_trigger_plus_slippage_for_long(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade(trigger=2.99))
    _, _, action, _, limit, _ = next(s for s in broker.submitted if s[0] == "marketable")
    assert action == "BUY" and limit == pytest.approx(3.19)   # 2.99 + 0.20


def test_entry_limit_is_trigger_minus_slippage_for_short(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade(direction="short", trigger=3.50, stop=3.60, target=3.20))
    _, _, action, _, limit, _ = next(s for s in broker.submitted if s[0] == "marketable")
    assert action == "SELL" and limit == pytest.approx(3.30)  # 3.50 - 0.20


# --- fill -> position opens with a resting target + armed synthetic stop --------

def test_fill_opens_position_and_places_resting_target(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade())
    om.poll_fills()
    assert book.open_count() == 1
    pos = next(iter(book.positions.values()))
    assert pos.entry_price == pytest.approx(3.19) and pos.stop_price == pytest.approx(2.92)
    assert any(s[0] == "resting" for s in broker.submitted)   # target resting at the exchange


# --- the CLIENT-SIDE SYNTHETIC STOP: bar low breaches -> marketable exit --------

def test_synthetic_stop_fires_on_bar_low_breach(tmp_path):
    import pandas as pd
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade())
    om.poll_fills()
    # a bar whose LOW pierces the 2.92 stop (even if it closes above)
    bar = pd.DataFrame([{"dt_et": pd.Timestamp("2026-07-27 08:10:00"), "open": 3.0,
                         "high": 3.05, "low": 2.90, "close": 2.99, "volume": 1000}])
    om.manage({"VIVK": _FakeFeed(bar)})
    exits = [s for s in broker.submitted if s[0] == "marketable" and s[2] == "SELL"]
    assert len(exits) == 1                       # the stop exit was submitted
    om.poll_exits()
    assert book.open_count() == 0                # and the position settled/closed
    assert guard.closed_trades == 1


def test_no_stop_fire_when_bar_holds_above_stop(tmp_path):
    import pandas as pd
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade()); om.poll_fills()
    bar = pd.DataFrame([{"dt_et": pd.Timestamp("2026-07-27 08:10:00"), "open": 3.0,
                         "high": 3.10, "low": 2.95, "close": 3.05, "volume": 1000}])
    om.manage({"VIVK": _FakeFeed(bar)})
    assert book.open_count() == 1                # 2.95 low never touched the 2.92 stop


# --- flatten clears everything -------------------------------------------------

def test_flatten_all_exits_positions_and_cancels_pending(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade()); om.poll_fills()          # one open position
    om.enter(_trade(ticker="EXYN", mode="wedge_break", entry=3.22, stop=3.17,
                    target=3.53, trigger=3.22))  # pending (fills on poll)
    om.flatten_all(reason="test")
    om.poll_exits()
    assert book.open_count() == 0


# --- reconciliation / crash adoption -------------------------------------------

def test_reconcile_flags_unmanaged_broker_position(tmp_path):
    book = PositionBook(tmp_path / "state")
    rec = _Rec()
    alarms = book.reconcile({"ZZZZ": 500}, rec)   # broker holds something the book doesn't
    assert any("unmanaged" in a for a in alarms)


def test_reconcile_drops_book_position_broker_no_longer_holds(tmp_path):
    book = PositionBook(tmp_path / "state")
    book.add(Position(ticker="VIVK", entry_mode="dip_red_break", direction="long",
                      date="2026-07-27", shares=100, entry_price=3.0, stop_price=2.9,
                      target_price=3.1, remaining=100))
    alarms = book.reconcile({}, _Rec())           # broker holds nothing
    assert book.open_count() == 0 and any("closed while bot was down" in a for a in alarms)


def test_position_book_persists_across_instances(tmp_path):
    b1 = PositionBook(tmp_path / "state")
    b1.add(Position(ticker="VIVK", entry_mode="dip_red_break", direction="long",
                    date="2026-07-27", shares=100, entry_price=3.0, stop_price=2.9,
                    target_price=3.1, remaining=100))
    b2 = PositionBook(tmp_path / "state")         # a fresh process (crash + restart)
    assert b2.open_count() == 1 and "VIVK::dip_red_break" in b2.positions


# --- kill file -----------------------------------------------------------------

def test_kill_file_detected(tmp_path):
    guard = RiskGuard(_bcfg(), tmp_path / "state")
    assert not guard.kill_requested()
    guard.kill_file.write_text("stop")
    assert guard.kill_requested()


# --- tick-precise exit: a slotted position resolves on the crossing PRINT ------

class _FakeSlots:
    """A SlotManager stand-in exposing a single symbol's TickStream."""
    def __init__(self, symbol, stream):
        self._m = {symbol.upper(): stream}

    def get(self, symbol):
        return self._m.get(symbol.upper())


def test_slotted_position_exits_on_the_crossing_tick_not_the_bar(tmp_path):
    """The whole point of Phase A: with a tick slot, the stop fires on the exact
    crossing print. Here the bar's low would ALSO show a breach, but the exit is
    driven by the tick stream (source='tick', at the crossing price), and it
    resolves in first-touch tick order."""
    import pandas as pd
    from live_bot.tick_stream import TickStream

    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade())        # VIVK long, entry cap 3.19, stop 2.92, target 3.06
    om.poll_fills()
    pos = next(iter(book.positions.values()))

    stream = TickStream(None, None, "VIVK", 10)
    for dt, px in [("2026-07-27 08:10:00", 3.00), ("2026-07-27 08:10:01", 2.95),
                   ("2026-07-27 08:10:02", 2.90)]:      # stop 2.92 crossed at 2.90
        stream.on_tick(pd.Timestamp(dt, tz="America/New_York"), px, 100)
    om.slots = _FakeSlots("VIVK", stream)

    om.manage({})             # no bar frame at all -> tick path is the only authority
    ev = dict(next(e[1] for e in om.rec.events if e[0] == "exit_submitted"))
    assert ev["reason"] == "stop" and ev["source"] == "tick" and ev["tick_price"] == 2.90
    om.poll_exits()
    assert book.open_count() == 0


def test_slotted_position_does_not_exit_before_any_tick_crosses(tmp_path):
    import pandas as pd
    from live_bot.tick_stream import TickStream
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade()); om.poll_fills()
    stream = TickStream(None, None, "VIVK", 10)
    for dt, px in [("2026-07-27 08:10:00", 3.00), ("2026-07-27 08:10:01", 2.98)]:  # above stop
        stream.on_tick(pd.Timestamp(dt, tz="America/New_York"), px, 100)
    om.slots = _FakeSlots("VIVK", stream)
    om.manage({})
    assert book.open_count() == 1        # 2.98 never touched the 2.92 stop
