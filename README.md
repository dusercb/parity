# Parity

[![CI](https://github.com/dusercb/parity/actions/workflows/ci.yml/badge.svg)](https://github.com/dusercb/parity/actions/workflows/ci.yml)

A live trading bot for Interactive Brokers that has to make exactly the decisions its backtest would.

The bot doesn't re-implement its strategy. It imports the decision code of the
[Hindsight](https://github.com/dusercb/hindsight) backtester (`signal_core`), can pin it to an exact git
commit, and stamps that commit into every session it records. Every session is recorded as an
append-only event log, and a recording can be replayed through the backtester to compare the two
decision by decision. A different gate verdict, trigger, stop or share count is a bug, not a tolerance.

## How it works

- **Discovery.** IBKR's market scanner finds premarket movers. Every mover is monitored, confirmed
  movers are streamed on 5-second bars, and IBKR's three tick-by-tick streams are reserved for open
  positions and setups that are armed to trigger.
- **Decisions.** Completed bars feed the shared `signal_core` state machine. Once a setup arms, its
  trigger is checked on every tick, so entries fire at tick speed through the same gates the backtest
  uses.
- **Orders.** Entries are marketable limit orders priced off the quote. Bracket targets rest at the
  exchange. Stops are synthetic, because IBKR honours no stop orders premarket: they're watched on
  every tick and fired as marketable limits.
- **Recording.** Scanner snapshots, bars, gate decisions, orders and fills all go to a JSONL event log,
  and `replay/parity_diff.py` replays a recording through the backtester and reports any divergence.

## Safety

- **Paper-account guard.** With `require_paper_account: true`, the bot refuses to arm unless the
  connected account is a paper account.
- **Kill file.** `python -m live_bot flatten` writes a `STOP` file. On its next loop the running bot
  flattens every position and stops entering.
- **Crash adoption.** Open positions are persisted. After a restart or reconnect the bot reconciles them
  with the broker's positions and adopts them, raising an alarm on any mismatch.
- **Broker truth.** The bot keeps its own model P&L alongside the broker's realised P&L, and at session
  end audits IBKR's execution records against its own ledger.
- **Refuses rather than guesses.** It won't start on a commit-pin or strategy mismatch, and it reads
  fill quantities from the broker rather than assuming them.

## A bug worth telling

In one paper session the bot booked a winning exit the moment price printed through its target, and
dropped the position from its book. The broker didn't actually fill the resting order for another 88
seconds. For those 88 seconds a live position sat outside the book, with no synthetic stop and no crash
adoption. The fix: the exit is still *decided* on the first touch, so parity with the backtest is
unchanged, but the bookkeeping is only *settled* on the broker's fill. See `_arm_target_confirm` in
[`live_bot/order_manager.py`](live_bot/order_manager.py).

## Running it

Requires Python 3.13 and TWS or IB Gateway with API access, with this repository checked out next to
Hindsight:

```
parent-folder/
  hindsight/
  parity/
```

```bash
pip install -r ../hindsight/ibkr_premarket_backtester/requirements.txt -r requirements.txt
python -m live_bot run --once     # a single poll-and-evaluate pass
python -m live_bot run            # trade today's premarket session, then exit
python -m live_bot serve          # stay up across days, sleeping between sessions
python -m live_bot status         # one line per recorded session
python -m live_bot flatten        # flatten everything and halt
```

`bot_config.yaml` contains example settings. With `trading.enabled: false` the bot connects read-only
and only records what it would have done.

## Tests

```bash
python -m pytest
```

The suite runs offline against fake broker objects. CI runs it on every push, with Hindsight checked
out alongside.

## More detail

[docs/design.md](docs/design.md) covers the strategy scope, broker truth versus the bot's model, how
partial exits transact, why fill quantities are read rather than assumed, and the observer and trading
modes.

Python · ib_async · asyncio · pandas · JSONL event sourcing
