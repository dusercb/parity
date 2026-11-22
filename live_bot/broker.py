"""Thin order-submission wrapper over ib_insync/ib_async.

Isolated behind a tiny interface so the order_manager is unit-testable with a
fake broker (no TWS): every method here is a single, auditable broker action.
Only THREE order shapes exist, matching the backtester's fill model:

  * marketable limit  -- entries and software-triggered exits (stop hit, cover,
    09:30 flatten). A limit priced through the market so it fills like a market
    order but can NEVER fill worse than the cap -- the same protection the
    backtest's entry_limit_slippage_cents models. outsideRth=True for premarket.
  * resting limit      -- the profit target, sitting at the exchange.
  * position readback  -- what the broker actually holds, for reconciliation.

There are deliberately NO native stop orders: IBKR does not honour stop orders
in premarket, so the stop is simulated client-side by the order_manager, which
submits a marketable-limit exit when its monitored level is breached.
"""
from __future__ import annotations

from .ibcompat import Stock

try:
    from ib_async import LimitOrder, MarketOrder  # noqa: F401
except ImportError:
    from ib_insync import LimitOrder, MarketOrder  # noqa: F401


class Broker:
    def __init__(self, ib):
        self.ib = ib
        self._contracts: dict[str, object] = {}

    def contract(self, ticker: str):
        c = self._contracts.get(ticker.upper())
        if c is None:
            c = Stock(ticker.upper(), "SMART", "USD")
            self.ib.qualifyContracts(c)
            self._contracts[ticker.upper()] = c
        return c

    def marketable_limit(self, ticker: str, action: str, qty: int, limit_price: float):
        """A buy/sell limit priced at limit_price, outsideRth, that behaves like a
        market order up to the cap. action = 'BUY' | 'SELL'."""
        order = LimitOrder(action, int(qty), round(float(limit_price), 2),
                           outsideRth=True, tif="DAY")
        return self.ib.placeOrder(self.contract(ticker), order)

    def resting_limit(self, ticker: str, action: str, qty: int, limit_price: float):
        """A profit-target limit resting at the exchange, outsideRth."""
        order = LimitOrder(action, int(qty), round(float(limit_price), 2),
                           outsideRth=True, tif="DAY")
        return self.ib.placeOrder(self.contract(ticker), order)

    def cancel(self, trade) -> None:
        try:
            self.ib.cancelOrder(trade.order)
        except Exception:
            pass

    def positions(self) -> dict[str, int]:
        """symbol -> signed share quantity currently held (long +, short -)."""
        out: dict[str, int] = {}
        try:
            for p in self.ib.positions():
                sym = getattr(p.contract, "symbol", None)
                if sym:
                    out[sym.upper()] = out.get(sym.upper(), 0) + int(p.position)
        except Exception:
            pass
        return out

    def account_value(self, tag: str, account: str | None = None) -> float | None:
        """A single account-summary figure (e.g. 'BuyingPower', 'NetLiquidation')."""
        try:
            for v in self.ib.accountValues(account) if account else self.ib.accountValues():
                if v.tag == tag and v.currency in ("USD", "BASE", ""):
                    return float(v.value)
        except Exception:
            pass
        return None
