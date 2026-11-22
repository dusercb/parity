"""IBKR error-event triage for the observer.

IBKR sends a lot of "errors" that are actually INFORMATIONAL -- connection
status notices, and the acknowledgement TWS returns when the one-shot
reqScannerData cancels its own subscription (code 162, "API scanner
subscription cancelled"). ib_insync logs every one of these at ERROR level, so
without triage the console fills with red on every 30s poll and a REAL error
(a dropped connection, a bad contract) is invisible in the noise.

The rule here: an operator glancing at the console must be able to trust that a
red line means something. Benign codes are recorded (JSONL, for the audit) but
never printed; everything else is both recorded and surfaced.
"""
from __future__ import annotations

import logging

# Informational status codes -- connection/farm health and the scanner-cancel
# acknowledgement. Not failures; recorded quietly, never printed.
BENIGN_CODES = frozenset({
    162,   # "API scanner subscription cancelled" -- the normal reqScannerData teardown
    202,   # order cancelled (no orders exist at observer stage, but harmless if seen)
    300,   # can't find EId (cancel of an already-gone reqId)
    2100,  # API client unsubscribed from account data
    2103, 2104, 2105, 2106, 2107, 2108,  # market-data / HMDS farm connection status
    2119,  # market-data farm connecting
    2158,  # sec-def data farm connection OK
    2150,  # invalid position trade derived value (informational)
})


def install_error_triage(ib, recorder, console_print=print) -> None:
    """Attach an errorEvent handler that records everything and prints only
    non-benign codes, and stop ib_insync's own logger from mirroring the same
    events to the console (propagate=False + a WARNING file-less null so the
    recording is the single source of truth)."""

    def _on_error(reqId, code, msg, contract=None):
        sym = getattr(getattr(contract, "symbol", None), "upper", lambda: None)() \
            if contract is not None else None
        benign = code in BENIGN_CODES
        recorder.event("ib_message", code=code, reqId=reqId, msg=str(msg),
                       symbol=sym, benign=benign)
        if not benign:
            where = f" [{sym}]" if sym else ""
            console_print(f"IBKR error {code}{where}: {msg}", flush=True)

    ib.errorEvent += _on_error

    # ib_insync mirrors the same events through the standard logging module at
    # ERROR level; silence that path so triage above is the only voice.
    lg = logging.getLogger("ib_insync")
    lg.setLevel(logging.CRITICAL)
    lg.propagate = False
    # ib_async uses a different logger name; quiet it too if present.
    logging.getLogger("ib_async").setLevel(logging.CRITICAL)
