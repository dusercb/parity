# Parity

A live IBKR trading bot held to decision parity with its backtester: it imports the backtester's own decision code, records every session event by event, and replays each recording through the backtest to diff the decisions.

> **Public snapshot of a private repository.** The live bot's code and tests, without session recordings, position state, credentials or any account details. Sizing and scanner settings in `bot_config.yaml` are illustrative. It imports `signal_core` from [Hindsight](https://github.com/dusercb/hindsight), expected checked out next to this repo as `../hindsight`.

Live IBKR trading bot for the validated premarket dip strategy. Order placement
is LIVE (Stage 4) behind the paper-account guard; `trading.enabled: false` still
runs it as a pure read-only observer.

## Strategy scope — updated 2026-08-16

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
the cap (the live bot clamps to broker-reported `BuyingPower` — see DP-1).
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

## Running it

```bash
# THE ONE YOU WANT — daemon: stays up across days, sleeps between sessions
python -m live_bot serve

python -m live_bot run          # observe/trade TODAY's session only, then exit
python -m live_bot run --once   # single poll+evaluate pass (smoke test)
python -m live_bot status       # one-line-per-session table
python -m live_bot show [DATE]  # full detail of one session
python -m live_bot flatten      # EMERGENCY: flatten all + halt new entries
```

### Observer mode vs trading mode

The master switch is `trading.enabled` in `bot_config.yaml`:

- **`false` (default) — observer.** Connects **read-only**, records decisions,
  places nothing. This is what has run so far.
- **`true` — places orders.** Connects writable, but **refuses to arm unless the
  account is paper** (DU/DF prefix — `require_paper_account: true`). Sizes with
  the backtester's real config sizing against the broker's live NetLiquidation
  (clamped to BuyingPower). Concurrent positions are funded per
  `risk.max_leverage`.

**Safety floor (always on, regardless of the "no auto-halt" choice):**
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

### First live-paper run (do this watched, not on the VPS yet)

1. TWS/Gateway up and logged into the **paper** account, API enabled, port
   matches `bot_config.yaml`.
2. Set `trading.enabled: true` in `bot_config.yaml`.
3. **Before 04:00 ET**, `python -m live_bot run` (not `serve` yet) and **watch
   it**. It prints `[trading ARMED] account=DU... (paper)` on connect.
4. Keep a second terminal ready: `python -m live_bot flatten` is your kill.
5. After 09:35: `python -m live_bot show` for the session; compare fills to the
   backtest expectation.

**`serve` is the operational mode.** It idles outside the premarket window
(no connection, no IBKR requests), wakes at `session.start_et` (04:00 ET),
records the session, sleeps until the next one, and keeps doing that
indefinitely. Blackout days are skipped without connecting; a per-session crash
is logged and the loop continues to the next day rather than taking the process
down. Mid-session disconnects reconnect with backoff and re-subscribe streams.

Prerequisite: **TWS or IB Gateway must be running and logged in** (paper, API
enabled, port per `bot_config.yaml`). The observer connects `readonly: true` and
refuses to start otherwise — it cannot place an order.

On the dev machine you can run it inside the backtester's venv:
`..\hindsight\.venv\Scripts\python.exe -m live_bot serve`

### Keeping it up on the VPS (Stage 2)

`serve` survives sessions and errors, but not a reboot or a killed process. On
Windows Server, wrap it so the OS restarts it:

- **NSSM** (simplest): `nssm install live_bot "C:\path\to\python.exe" "-m live_bot serve"`,
  set the working directory to this folder, recovery = restart on failure.
- **Task Scheduler**: trigger "At startup", action = the same command, with
  "Restart if the task fails" and "Run whether user is logged on or not".

IB Gateway itself needs **IBC** for unattended login/daily restart — Gateway
logs out on its own schedule, and a logged-out Gateway means a blind observer.
Full ops detail in the master plan §10.

## Requirements

### Software / runtime
- **Python 3.13**, packages in `requirements.txt` (`ib_async`, `pandas`,
  `numpy`, `PyYAML`, `yfinance`). On the dev machine the bot can run inside the
  backtester's venv instead — `live_bot/ibcompat.py` falls back to `ib_insync`.
- **`signal_core`** from the **pinned** backtester repo (`bot_config.yaml`
  → `signal.backtester_path` + `signal.pin`). Not a PyPI dep — attached by path,
  SHA-verified at startup. This is the parity anchor; never vendored or copied.
- **IB Gateway** (headless; VPS) or **TWS** (dev), API enabled, bound to
  `127.0.0.1`. Paper port `7497` (TWS) / `4002` (Gateway). **IBC** for
  unattended Gateway login/restart from Stage 2.

### IBKR account
- **Paper account** for Stages 3–4 (the observer connects `readonly: true`;
  it *cannot* place an order).
- **API order permissions** are only needed from Stage 4 — not for the observer.

### Market-data subscriptions (staged — this is the paid part)

The strategies and the live marketable-limit pricing need **top-of-book L1
only (the consolidated NBBO)** — **no Level 2 / market depth** is ever required.
IBKR's US non-pro L1 is the NBBO for US equities.

| Stage | Real-time L1 (NBBO)? | Why / notes |
|---|---|---|
| **3 — observer** | *Recommended, not strictly required* | Parity vs replay holds even on delayed data (the replay uses the **recorded** feed). But the fast-mover **latency measurement (DP-2)** is only meaningful on real-time bars — run it subscribed. |
| **4 — paper orders** | **Required** | The live order is a marketable limit **pegged to the quote** (buy at ask + buffer / short at bid − buffer). Delayed data cannot price it. Enable **"share market data with paper account"** in Client Portal, or the paper session sees delayed data. |
| **5–6 — live** | **Required** | Same, on the live username. |

- **Cost**: IBKR non-pro US equity L1 is on the order of a few $/month (per
  exchange, or a bundle) — **confirm the exact product + price in IBKR's Market
  Data Subscription manager**; prices change and it's account-tier dependent.
  Historical bars (the 10s backfill) and the scanner use the account's standard
  entitlements — no extra subscription.
- **Data feeds we do NOT pay for**: the **scanner is IBKR's native
  `reqScannerData`** ($0, same connection) — no third-party screener unless
  Stage 3's recordings show it missing real movers (DP-2 → Polygon.io fallback,
  which would then also serve as the screener). No news feed. No depth.

### VPS (Stage 2+)
- **Windows Server 2022**, **3 vCPU ≤2.2 GHz, 4 GB RAM, 45 GB SSD** (the
  provisioned spec), **New York region** (~1–3 ms to IBKR vs ~70 ms Europe).
  Headroom analysis in the master plan §10; observer footprint is well within
  it (see the completion note in this repo's commit history / master plan).

## Master plan

The complete master plan (staged roadmap, architecture, parity framework, VPS
ops, decision register) is kept private.

The defining rule: **decision parity** — this bot must enter and exit at the
exact positions the backtester would, given the same inputs. No order code is
written until parity is proven by the observer stage.

## Stage status

| Stage | Name | Status |
|---|---|---|
| 0 | Repo bootstrap (two private repos, CI, GitHub) | **done 2026-07-22** (local: both repos committed, CI workflow in place; GitHub push pending `gh auth login` — commands in the backtester repo's PORTING_TO_LIVE.md Stage 0 note) |
| 1 | `signal_core/` extraction + parity gates (incl. `wedge.py`) | **done 2026-07-22** — curated-facade form: `signal_core/` exports the backtester's own decision objects (both engines, sizing, blackout, config); gates = subprocess import-purity (no ib_insync in the bot's chain) + object identity + 313-test suite green. Physical code move deferred as a behavior-free refactor; the interface is final. |
| 2 | VPS + Gateway ops shell (Win Server 2022, NY, IBC) | pending (needs the VPS provisioned) |
| 3 | Observer bot — parity proven, zero order code (both strategies) | **IN PROGRESS 2026-07-22** — skeleton shipped (`live_bot/` + `replay/parity_diff.py`); offline end-to-end green: cached AXIL 2025-12-11 → SignalRuntime → recording → replay diff = **0 Class-0 divergences**. **Tiered mover-coverage built** (`coverage.py`): the flat `max_tracked` confirm-and-freeze cap is replaced by a Tier-C monitor (every scanner mover, never forgotten) → snapshot dip/wedge early-warning → Tier-B stream promotion (positions/armed pinned, then early-warning, then %-gain rank), with both-way movement + re-backfill on re-promotion. Lossless in awareness; residual = scanner coverage + promotion latency (measured, not designed-in). Next: first live premarket session against paper TWS (`python -m live_bot run`, TWS/Gateway must be up), then ≥10 zero-divergence sessions = exit gate. |
| 4 | Paper trading — order plumbing + slippage calibration | **IN PROGRESS** — order layer built (order_manager, position_state, risk_guard, broker; DU-guard, synthetic stop, flatten/kill-file, crash-adoption). Phase A: tick-precise exits on open positions. **Phase B: minimal-latency entry** — dip setups pre-armed from bars (signal_core on_armed), promoted to tick slots, fired on the trigger cross via signal_core.evaluate_touch (same gate stack as the backtest). Live exits run each position's configured mode — bracket, or the dip's `candle_trail` runner (ratchet-to-prior-min-low + fat-green target drop + partial-then-breakeven), direction-aware. 55 bot tests. First live-paper run pending (watched). Still to build: slippage-calibration report. |
| 5 | Micro-live | pending |
| 6 | Full size + steady state | pending |
| 7 | Paper / live toggle | pending |

Update this table and the "CURRENT STAGE" marker in the master plan as stages
complete.


TODO: update email system - bugs with password.
      Gmail rejects account passwords (SMTP 530); needs a Google APP PASSWORD
      (16 chars, requires 2FA on the account) in email.local.yaml. Confirmed
      failing 2026-08-07 -- the session summary did not send.
TODO: ALERTING IS SAFETY-CRITICAL, not a nicety. On a VPS the console is not
      watched, so a broken notifier means a bot in trouble cannot say so --
      2026-08-07 ended with an unmanaged position and no alert went out. Wanted:
      a second channel independent of email (Telegram/webhook/SMS), a startup
      self-test that proves the channel works BEFORE the session, and treating a
      notifier failure as a loud condition in its own right rather than a
      swallowed exception.
TODO: check parity feature
TODO: parity_diff replays with tick_fetcher=None while live has real ticks, so
      any bar where BOTH pennant lines break resolves by the engine's tie-break
      heuristic in replay and causally in live -- a false Class-0 divergence
      (STAK 2026-07-30). Fix by recording TickStream._ticks for armed symbols so
      the replay can resolve breaks the same way live did.
TODO: broker-side stop backstop is NOT available -- all order types here are
      limit orders and IBKR does not honour native stops premarket. The stop is
      therefore client-side only, and the bot being UP is the only thing
      enforcing it. Any mitigation has to live in the bot (see D: feed watchdog).
TODO: D -- feed watchdog. An open position whose data feed has gone stale beyond
      N seconds should ALERT and/or flatten rather than sit silently, because
      "no data" currently means "hold". Any exit taken on that path MUST be
      recorded with its own explicit reason/flag (e.g. exit reason
      "stale_feed_watchdog") so it is never confused with a strategy stop in the
      P&L or the parity diff -- an unavoidable, clearly-labelled divergence.