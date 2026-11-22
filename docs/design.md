# Design notes

The reasoning behind the parts of the bot that are easy to get wrong. For an overview, see the [README](../README.md).

## Strategy scope

**This bot trades ONE strategy: `dip_red_break`** — the long-only
HOD-first-pullback dip. The wedge was switched off on 2026-08-15 (live results
below the sweep's expectation) and **removed from the live path entirely** on
2026-08-16.

Removed means removed: there is no `LiveWedgeWatch`, no wedge engine pass, no
wedge sizing caps, no coil early-warning. What that bought, all of it paid per
symbol per poll before:

| Removed | Cost it was paying |
|---|---|
| second engine pass (`backtest_wedge`) | a full re-run over the whole session frame, per symbol, per poll |
| `complete_tf_frame` | a resample to trim the frame to whole wedge candles (the dip scans native 10s bars, already complete) |
| wedge tick-fire loop | a `forming_candle` resample **plus** a `feed.frame()` rebuild per armed symbol, inside the ~2s loop |
| coil early-warning | a second geometry pass over every monitored symbol's samples, every poll |
| wedge-armed symbols | contention for the 3 tick-by-tick slots, which now go to dip positions and armed setups only |

### The strategy set is still the anchor's to state

`signal_core.active_strategies()` (`routing.py`) remains the one truth table,
read by the backtester, the parity replay and this bot. There is deliberately
**no** strategy toggle in `bot_config.yaml` — a live-only override would be a
Class-0 divergence, the live book trading a different strategy set than the
backtest that validated it.

What changed is that the bot can no longer *honour* a wedge selection. So
`Observer.__init__` calls `_require_dip_only()` and **refuses to start** with
`STRATEGY MISMATCH` if the anchor ever selects anything but `("dip",)`, the same
way the signal pin and the paper-account guard refuse. Re-enabling the wedge
live means restoring the bot-side implementation, not deleting that check.
`wedge.enabled: true` still works fine for *backtests*.

### Historical recordings stay readable

Sessions before 2026-08-16 traded the wedge. `session_strategies()` reads the
strategy set out of each recording's own `session_meta` — the new `strategies`
field, or the old `wedge_enabled`/`wedge_concurrent` pair — so `parity_diff`
replays a session with the engines *it* ran, not today's config, and
`session_view` and the digest email still render those sessions correctly.

### Sizing note

`risk.max_leverage` is the funding model: per-trade risk is sized off realized
equity, and an entry keeps its full risk-derived size by drawing on margin up to
the cap (the live bot clamps to broker-reported `BuyingPower`).
Per-entry `margin_used` / `leverage_at_entry` telemetry shows what live borrowed.
The wedge's two extra volume caps went with it; the dip takes
`risk.max_pct_of_volume_at_entry` through `capped_shares`, exactly as the
backtest applies it to a dip trade.

## Broker truth vs the bot's model

The bot keeps TWO P&L numbers and reconciles them, because they answer different
questions and conflating them is how execution problems stay invisible.

* **model** (`realized_pnl`) — `(exit - entry) x shares`, gross of commission,
  from fill prices the bot observed. This is what the backtest computes, so it is
  the number parity is measured against. It is *not* what the account made.
* **broker** (`broker_realized_pnl`) — IBKR's own realised figure, arriving on
  `commissionReportEvent` alongside the commission for each execution. Pushed, not
  polled, so it costs nothing on the hot path.

At session end, after the flatten has drained, `audit_against_broker()` asks
`ib.fills()` for every execution IBKR recorded that day and records a
`broker_audit` event with both totals and their `delta`. A delta of roughly
`-commission` is expected. Anything beyond that logs `broker_audit_drift` and
raises an `[ALARM]` on the digest email: the bot's picture of its own trading
disagrees with the account, which is exactly the condition that made WETO
invisible for 88 seconds on 2026-08-14.

The live stream and the end-of-session sweep are deliberately redundant. The
stream only sees what the bot was listening for; the sweep asks the broker to
enumerate the day from its own records, so it still catches an execution that
landed during a reconnect, a fill on an order the bot had written off, or a
handler that raised.

### The candle_trail partial transacts, it does not assume

The backtest banks `trail_partial_at_target` the moment price *touches* the
target, then drops the target and jumps the remainder's stop to breakeven. That
transition only makes sense **because half the position was sold** — so live, the
sale has to be real before the transition applies.

A resting limit cannot promise that. The backtest banks on a touch; a resting
order only fills if the queue reaches it, and a wick that tags the target and
reverses is exactly the case the trail exists to handle. So `candle_trail` rests
**nothing**. The touch sends a marketable limit for the partial — a limit priced
through the target by `exit_slippage_cents`, `outsideRth`, the same instrument
the synthetic stop uses, since IBKR has no premarket market orders — and the
transition applies in `poll_banks` when the broker confirms the fill.

The trade-off is deliberate: you pay the spread on the partial instead of
possibly earning it, in exchange for the book and the broker moving together. If
the order does not fill, the position is exactly as it was — full size, target
still live, runner not at breakeven, no P&L booked — and the next touch retries.

While a bank is in flight the runner is **not** suspended: `_banking`, unlike
`_exiting`, leaves the position live so its stop keeps being watched. If the stop
fires during a bank, the exit is submitted for `remaining - committed` only —
a bank order is a sell on the same side, and counting those shares twice would
fill past flat into a position the other way.

The bracket target still rests a limit, because it only has to agree about a
*close*, and `_arm_target_confirm` already waits on it.

### Fill quantity is read, never assumed

`orderStatus.filled` is now read everywhere an order reaches a terminal state.
A cancelled order is the broker's final word on that order — it is **not** a
statement that nothing filled:

* a partially filled entry becomes a real position sized off the broker's fill,
  with its resting target sized to match (`entry_partial_fill`);
* a partially filled exit *reduces* the position and releases it, so the
  remainder can be exited again rather than stranding in `_exiting` forever;
* the 09:35 flatten cancels working entries but no longer discards them — the
  drain keeps pumping until the broker says what actually happened, then adopts
  and exits anything that filled late.

## Observer mode vs trading mode

The master switch is `trading.enabled` in `bot_config.yaml`:

- **`false` (default) — observer.** Connects **read-only**, records decisions,
  places nothing.
- **`true` — places orders.** Connects writable, but **refuses to arm unless the
  account is paper** (DU/DF prefix — `require_paper_account: true`). Sizes with
  the backtester's real config sizing against the broker's live NetLiquidation
  (clamped to BuyingPower). Concurrent positions are funded per
  `risk.max_leverage`.

**Safety floor (always on):**
- **Paper-account guard** — the code will not place an order on a live (`U`)
  account while `require_paper_account: true`. This is the one thing between the
  bot and real money.
- **Client-side synthetic stop** — IBKR has no premarket stop orders, so the bot
  watches each position and submits a marketable exit when the stop is breached.
  **⇒ while a position is open, the bot being up IS the stop.** Never `kill -9`
  it with a position open unless you then flatten by hand.
- **`flatten` / kill-file** — `python -m live_bot flatten` drops a `STOP` file the
  running bot polls each loop; it flattens everything and stops entering. Delete
  the file to resume on a later run.
- **Crash adoption** — on restart (or reconnect) the bot reconciles its durable
  `state/open_positions.json` against the broker's actual positions and adopts
  them; any mismatch is a loud alarm.

**Exit modes (live).** Each position runs the exit `strategy.exit_mode` selects:
`bracket` (fixed structural stop + resting target, via `exit_mode: "standard"`)
or `candle_trail` (the dip's configured `candle_trail_1min` runner, so the live
default): the synthetic stop ratchets to
each completed 1-minute low, the first fat-bodied close in the trade's favour
drops the target, and a target cross before that banks `trail_partial_at_target`
and sends the runner to breakeven. Direction-aware. Where live minute
boundaries/fills differ from the backtest's, that is Class-1 (measured), not
decision divergence.
