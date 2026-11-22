"""live_bot -- premarket trading bot for ONE validated strategy: dip_red_break.

Discovers candidates, streams and folds bars, evaluates the strategy through
signal_core (the backtester's own engine -- parity by construction, never a
reimplementation), and records every input and intended trade for the nightly
parity diff.

Runs in one of two modes, derived from trading.enabled so they can never
contradict each other:
  * observer (Stage 3) -- connects READ-ONLY and can physically place nothing;
    records decisions only.
  * trading  (Stage 4) -- places real orders on the connected account, behind
    the paper-account guard.

The wedge strategy was removed from this package on 2026-08-16; the backtester
still ships it, and session_controller._require_dip_only refuses to start if the
shared config ever selects it again. See README.md "Strategy scope".
"""
