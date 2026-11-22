"""Scenario B -- concurrent-leverage parity. The live funding cap must bound
total open+pending exposure to equity * max_leverage, mirroring the backtester's
apply_position_sizing `committed` walk, so a concurrent live book sizes exactly
as the backtest. A second entry fired in the SAME loop must already see the
first's reservation (the same-loop over-commit fix), and every open must record
leverage telemetry. Real signal_core for capped_shares; fake broker, no TWS.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "hindsight" / "ibkr_premarket_backtester"))

import signal_core as sc_real

from live_bot.order_manager import OrderManager
from live_bot.position_state import Position, PositionBook
from live_bot.risk_guard import RiskGuard

from test_order_layer import _FakeBroker, _Rec, _bcfg, _cfg


def _om(tmp_path, net_liq=5000.0, buying_power=1e9):
    broker = _FakeBroker({"NetLiquidation": net_liq, "BuyingPower": buying_power})
    guard = RiskGuard(_bcfg(), tmp_path / "state")
    book = PositionBook(tmp_path / "state")
    om = OrderManager(broker, sc_real, _cfg(), _bcfg(), book, guard, _Rec(), "DU1")
    return om, broker, guard, book


def _dip(entry=3.00, stop=2.97, ticker="VIVK"):
    return {"ticker": ticker, "date": "2026-07-27", "entry_mode": "dip_red_break",
            "direction": "long", "setup_index": 1, "entry_price": entry,
            "stop_price": stop, "target_price": entry + 0.20, "trigger_price": entry,
            "volume_at_entry": 5_000_000.0}


def _hold(book, ticker, shares, entry):
    """Pretend a position is already open (consumes committed notional)."""
    pos = Position(ticker=ticker, entry_mode="wedge_break", direction="long",
                   date="2026-07-27", shares=shares, entry_price=entry,
                   stop_price=entry - 0.5, target_price=entry + 1, remaining=shares)
    book.positions[pos.key()] = pos


def test_funding_cap_bounds_exposure_to_equity_times_max_leverage(tmp_path):
    # equity 5000, max_leverage 1.6 -> total cap 8000. Already holding $7000.
    om, _b, _g, book = _om(tmp_path)
    _hold(book, "BIG", shares=2000, entry=3.50)          # committed = 7000
    shares, basis = om._size(_dip(entry=3.00, stop=2.97)) # capped_shares would give 1266 (notional)
    assert basis == "funding"
    assert shares == 333                                  # headroom 1000 // 3.00


def test_same_loop_second_entry_sees_the_first_reservation(tmp_path):
    # No open positions, but a pending entry reserving ~7000 -> the next size is
    # funding-capped by that reservation (the same-loop over-commit fix).
    om, _b, _g, _book = _om(tmp_path)
    om._pending["BIG"] = {"shares": 2000, "limit": 3.50}  # reservation = 7000
    shares, basis = om._size(_dip(entry=3.00, stop=2.97))
    assert basis == "funding" and shares == 333


def test_single_trade_within_leverage_is_not_funding_capped(tmp_path):
    om, _b, _g, _book = _om(tmp_path)
    shares, basis = om._size(_dip(entry=3.00, stop=2.70))  # wide stop -> risk cap, small notional
    assert basis != "funding"


def test_committed_releases_when_a_position_closes(tmp_path):
    om, _b, _g, book = _om(tmp_path)
    _hold(book, "BIG", shares=2000, entry=3.50)
    assert om._committed_notional() == 7000.0
    book.positions.clear()                                # position closed
    assert om._committed_notional() == 0.0
    shares, basis = om._size(_dip(entry=3.00, stop=2.97))
    assert basis == "notional" and shares == 1266         # full size again, no funding cap


def test_size_to_risk_flows_through_live_sizing(tmp_path):
    # The flag lives on the backtester config the bot reads; capped_shares honors
    # it, so a tight-stop trade sizes to the full risk target (margin-funded) live
    # instead of being clipped by the cash cap.
    om, _b, _g, _book = _om(tmp_path)          # NetLiq 5000, BuyingPower huge
    om.cfg.risk.size_to_risk = True
    on, _bon = om._size(_dip(entry=3.00, stop=2.97))
    om.cfg.risk.size_to_risk = False
    off, _boff = om._size(_dip(entry=3.00, stop=2.97))
    assert on > off                             # margin-funded to fuller risk (2666 vs 1266)


def test_open_records_leverage_telemetry(tmp_path):
    om, _b, guard, _book = _om(tmp_path)
    om.enter(_dip(entry=3.00, stop=2.70))
    om.poll_fills()
    opened = [p for k, p in om.rec.events if k == "position_opened"]
    assert opened, "a position_opened event was recorded"
    ev = opened[-1]
    assert "leverage_at_entry" in ev and "margin_used" in ev
    assert ev["open_positions_at_entry"] == 0             # first position
    assert ev["leverage_at_entry"] is not None
