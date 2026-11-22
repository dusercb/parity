"""What the bot THINKS it traded vs what the broker actually did.

Three kinds of mismatch, all of which the bot used to be blind to:

  * FILL QUANTITY. Nothing read orderStatus.filled -- only avgFillPrice. A
    cancelled entry that filled part way was recorded "entry_unfilled" and
    dropped, leaving real shares held with no book entry and no synthetic stop:
    the WETO phantom on the entry side. The entry_ttl_s cancel added on
    2026-08-15 made that path routine rather than rare, and the 09:35 flatten hit
    it at the one moment with no reconcile pass left to alarm.

  * PARTIAL EXITS. An exit reaching a terminal state part-filled left the key in
    _exiting forever -- _exit refuses to re-submit, tick resolution skips it, and
    reconcile masks it as busy. Silent and indefinite.

  * P&L. _settle computes (exit - entry) x shares, gross of commission, from
    prices the bot observed. That is a MODEL. The broker's own realised figure
    now arrives on commissionReportEvent and is reconciled against it.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot.order_manager import UNSET_DOUBLE
from test_order_layer import _FakeBroker, _FakeOrderTrade, _om, _trade


def _order(status="Submitted", filled=0, avg=0.0):
    t = _FakeOrderTrade(status=status, fill=avg)
    t.orderStatus.filled = filled
    return t


class _PartialBroker(_FakeBroker):
    """Entries rest unfilled until a test drives them."""
    def marketable_limit(self, ticker, action, qty, limit):
        t = _order(status="Submitted")
        self.submitted.append(("marketable", ticker, action, qty, limit, t))
        return t


class _Stream:
    def __init__(self, px):
        self._px = px

    def last_price(self):
        return self._px


class _Slots:
    def __init__(self, m):
        self._m = m

    def get(self, sym):
        return self._m.get(sym.upper())


def _events(om, kind):
    return [p for k, p in om.rec.events if k == kind]


def _enter(om, ticker="VIVK", direction="long", trigger=2.99):
    om.enter(_trade(ticker=ticker, direction=direction, trigger=trigger))
    return om._pending[ticker.upper()]["order"]


# --- entry side: a cancel is not proof that nothing filled --------------------

def test_a_partially_filled_cancelled_entry_becomes_a_real_position(tmp_path):
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    order = _enter(om)
    order.orderStatus.status = "Cancelled"
    order.orderStatus.filled = 400
    order.orderStatus.avgFillPrice = 2.99
    om.poll_fills()
    pos = book.positions.get("VIVK::dip_red_break")
    assert pos is not None, "shares the broker filled must never be dropped"
    assert pos.shares == 400 and pos.remaining == 400, "sized off the BROKER's fill"
    assert _events(om, "entry_partial_fill")


def test_a_genuinely_unfilled_cancel_is_still_just_unfilled(tmp_path):
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    order = _enter(om)
    order.orderStatus.status = "Cancelled"
    order.orderStatus.filled = 0
    om.poll_fills()
    assert not book.positions
    assert _events(om, "entry_unfilled") and not _events(om, "entry_partial_fill")


def test_a_full_fill_is_sized_off_the_broker_not_the_request(tmp_path):
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    order = _enter(om)
    order.orderStatus.status = "Filled"
    order.orderStatus.filled = 900          # broker filled fewer than we asked
    order.orderStatus.avgFillPrice = 2.99
    om.poll_fills()
    assert book.positions["VIVK::dip_red_break"].shares == 900


def test_a_bracket_target_is_sized_off_the_adopted_fill(tmp_path):
    """A position adopted at half size must not rest a full-size target -- that
    would sell shares it never owned and open an unintended short. (Bracket mode;
    candle_trail rests nothing at all.)"""
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    order = _enter(om)
    order.orderStatus.status = "Cancelled"
    order.orderStatus.filled = 400
    order.orderStatus.avgFillPrice = 2.99
    om.poll_fills()
    resting = [s for s in broker.submitted if s[0] == "resting"]
    assert resting and resting[0][3] == 400


# --- price through the limit --------------------------------------------------

def test_price_through_the_limit_cancels_the_remainder(tmp_path):
    """A marketable limit cannot fill once price is past its cap; holding it just
    reserves sizing while the break runs away."""
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    om.entry_ttl_s = 999                     # prove it is the PRICE, not the clock
    _enter(om)
    limit = om._pending["VIVK"]["limit"]
    om.slots = _Slots({"VIVK": _Stream(limit + 0.05)})
    om.poll_fills()
    assert broker.cancelled
    ev = _events(om, "entry_stale_cancel")
    assert ev and ev[0]["reason"] == "price_through_limit"


def test_price_still_inside_the_limit_is_left_working(tmp_path):
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    om.entry_ttl_s = 999
    _enter(om)
    limit = om._pending["VIVK"]["limit"]
    om.slots = _Slots({"VIVK": _Stream(limit - 0.01)})
    om.poll_fills()
    assert not broker.cancelled and "VIVK" in om._pending


def test_a_short_entry_uses_the_other_side_of_the_limit(tmp_path):
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    om.entry_ttl_s = 999
    _enter(om, ticker="SXTC", direction="short", trigger=4.65)
    limit = om._pending["SXTC"]["limit"]
    om.slots = _Slots({"SXTC": _Stream(limit + 0.05)})   # ABOVE = still fillable short
    om.poll_fills()
    assert not broker.cancelled
    om.slots = _Slots({"SXTC": _Stream(limit - 0.05)})   # through it
    om.poll_fills()
    assert broker.cancelled


def test_a_price_cancel_keeps_whatever_already_filled(tmp_path):
    """The occasional-partial case: cancel the remainder, hold what filled as a
    normal position."""
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    om.entry_ttl_s = 999
    order = _enter(om)
    limit = om._pending["VIVK"]["limit"]
    order.orderStatus.filled = 150                       # some of it got done
    om.slots = _Slots({"VIVK": _Stream(limit + 0.10)})
    om.poll_fills()                                      # cancel goes out
    order.orderStatus.status = "Cancelled"
    order.orderStatus.avgFillPrice = limit
    om.poll_fills()                                      # broker confirms
    pos = book.positions.get("VIVK::dip_red_break")
    assert pos is not None and pos.remaining == 150


def test_no_tick_slot_means_the_ttl_is_still_the_backstop(tmp_path):
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    om.slots = None
    om.entry_ttl_s = 0
    _enter(om)
    om.poll_fills()
    ev = _events(om, "entry_stale_cancel")
    assert ev and ev[0]["reason"] == "ttl"


# --- exit side ----------------------------------------------------------------

def test_a_partially_filled_exit_reduces_rather_than_closes(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade())
    om.poll_fills()
    pos = book.positions["VIVK::dip_red_break"]
    whole = pos.remaining
    om._exit(pos, reason="stop")
    ex = om._exiting[pos.key()][1]
    ex.orderStatus.status = "Cancelled"
    ex.orderStatus.filled = whole // 3
    ex.orderStatus.avgFillPrice = 2.92
    om.poll_exits()
    assert pos.key() in book.positions, "the unsold remainder is still ours"
    assert pos.remaining == whole - whole // 3
    assert _events(om, "exit_partial_fill")


def test_a_stranded_exit_is_released_so_it_can_be_retried(tmp_path):
    """The remainder must become resolvable again -- previously the key stayed in
    _exiting forever and nothing ever looked at the position again."""
    om, broker, guard, book = _om(tmp_path)
    om.enter(_trade())
    om.poll_fills()
    pos = book.positions["VIVK::dip_red_break"]
    om._exit(pos, reason="stop")
    ex = om._exiting[pos.key()][1]
    ex.orderStatus.status, ex.orderStatus.filled = "Cancelled", 0
    om.poll_exits()
    assert pos.key() not in om._exiting
    om._exit(pos, reason="stop")                 # retry now possible
    assert pos.key() in om._exiting


def test_a_candle_trail_partial_banks_only_what_the_broker_sold(tmp_path):
    """The transition -- target dropped, runner to breakeven -- only makes sense
    BECAUSE half the position was sold, so it must follow the fill and not the
    touch. A touch that never fills used to book P&L and move the runner anyway,
    after which reconcile healed `remaining` back from broker truth and the same
    shares were paid for twice."""
    om, broker, guard, book = _om(tmp_path)
    om.cfg.strategy.exit_mode = "candle_trail_1min"
    om.cfg.strategy.trail_partial_at_target = 0.5
    om.enter(_trade())
    om.poll_fills()
    pos = book.positions["VIVK::dip_red_break"]
    whole, partial = pos.remaining, om._target_shares[pos.key()]
    om._bank_partial(pos, 3.11)                    # the touch
    assert pos.remaining == whole, "nothing moves before the broker confirms"
    assert not pos.trail_active
    assert guard.closed_trades == 0
    om.poll_banks()                                # the fill
    assert pos.key() in book.positions, "the runner is still held"
    assert pos.remaining == whole - partial
    assert pos.trail_active


def test_a_partial_that_never_fills_leaves_the_position_untouched(tmp_path):
    """If nothing sold, the position is exactly as it was: full size, target still
    live, runner not at breakeven, no P&L booked."""
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    om.cfg.strategy.exit_mode = "candle_trail_1min"
    om.cfg.strategy.trail_partial_at_target = 0.5
    order = _enter(om)
    order.orderStatus.status, order.orderStatus.filled = "Filled", 1000
    order.orderStatus.avgFillPrice = 2.99
    om.poll_fills()
    pos = book.positions["VIVK::dip_red_break"]
    om._bank_partial(pos, 3.11)
    bank = om._banking[pos.key()][0]
    bank.orderStatus.status, bank.orderStatus.filled = "Cancelled", 0
    om.poll_banks()
    assert pos.remaining == 1000 and not pos.trail_active
    assert guard.realized_pnl == 0.0
    assert _events(om, "partial_unfilled")


def test_a_stop_during_a_bank_never_sells_the_same_shares_twice(tmp_path):
    """A bank order is a sell on the same side. Counting its shares again in the
    stop exit would fill past flat and open a position the other way."""
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    om.cfg.strategy.exit_mode = "candle_trail_1min"
    om.cfg.strategy.trail_partial_at_target = 0.5
    order = _enter(om)
    order.orderStatus.status, order.orderStatus.filled = "Filled", 1000
    order.orderStatus.avgFillPrice = 2.99
    om.poll_fills()
    pos = book.positions["VIVK::dip_red_break"]
    om._bank_partial(pos, 3.11)
    committed = om._banking[pos.key()][1]
    om._exit(pos, reason="stop")
    ex = [p for k, p in om.rec.events if k == "exit_submitted"][0]
    assert ex["shares"] == pos.remaining - committed
    assert ex["shares"] + committed == pos.remaining, "exactly flat, never past it"


# --- flatten ------------------------------------------------------------------

def test_flatten_does_not_discard_a_pending_entry(tmp_path):
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    order = _enter(om)
    om.flatten_all(reason="session_end")
    assert broker.cancelled
    assert "VIVK" in om._pending, "the cancel is a request, not an outcome"
    assert om.has_work_in_flight()
    order.orderStatus.status = "Cancelled"
    order.orderStatus.filled = 250
    order.orderStatus.avgFillPrice = 2.99
    om.poll_fills()
    assert book.positions["VIVK::dip_red_break"].remaining == 250


def test_flatten_is_idempotent(tmp_path):
    om, broker, guard, book = _om(tmp_path, broker=_PartialBroker())
    _enter(om)
    om.flatten_all(reason="session_end")
    om.flatten_all(reason="session_end")
    om.flatten_all(reason="session_end")
    assert len(broker.cancelled) == 1


# --- broker-truth P&L ---------------------------------------------------------

def _fill(sym="VIVK", shares=100, price=3.0, commission=1.25, realized=None):
    return SimpleNamespace(
        contract=SimpleNamespace(symbol=sym),
        execution=SimpleNamespace(execId="e1", shares=shares, price=price),
        commissionReport=SimpleNamespace(
            commission=commission,
            realizedPNL=UNSET_DOUBLE if realized is None else realized))


def test_commission_and_realized_pnl_accumulate_from_the_broker(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    f = _fill(commission=1.25, realized=42.5)
    om._record_commission(f, f.commissionReport)
    assert om.commission_total == 1.25
    assert om.broker_realized == 42.5
    assert _events(om, "commission_report")


def test_the_unset_sentinel_is_never_summed_as_pnl(tmp_path):
    """Opening executions carry IBKR's UNSET double in realizedPNL. Adding it
    would report a session P&L of ~1.8e308."""
    om, broker, guard, book = _om(tmp_path)
    f = _fill(commission=1.0, realized=None)          # -> UNSET
    om._record_commission(f, f.commissionReport)
    assert om.broker_realized == 0.0
    assert om.commission_total == 1.0
    assert _events(om, "commission_report")[0]["realized_pnl"] is None


def test_the_audit_reconciles_the_model_against_the_broker(tmp_path):
    om, broker, guard, book = _om(tmp_path)
    guard.record_close(100.0)                          # the model says +100
    ib = SimpleNamespace(fills=lambda: [
        _fill(shares=100, commission=1.0, realized=None),      # opening
        _fill(shares=100, commission=1.0, realized=98.0),      # closing
    ])
    a = om.audit_against_broker(ib)
    assert a["executions"] == 2 and a["shares"] == 200
    assert a["commission"] == 2.0
    assert a["broker_realized"] == 98.0
    assert a["model_realized"] == 100.0
    assert a["delta"] == -2.0                          # exactly the commission
    assert a["symbols"]["VIVK"]["executions"] == 2


def test_an_audit_failure_never_takes_the_session_down(tmp_path):
    om, broker, guard, book = _om(tmp_path)

    def boom():
        raise RuntimeError("no connection")

    a = om.audit_against_broker(SimpleNamespace(fills=boom))
    assert a["executions"] == 0
    assert _events(om, "broker_audit_error")
