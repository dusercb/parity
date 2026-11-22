"""One import point for the IB API library. The VPS runs ib_async (the
maintained fork, per PORTING_TO_LIVE.md D-runtime); the dev machine may only
have ib_insync installed (the backtester's pin) -- same API surface for
everything the observer touches, so fall back with a warning rather than
forcing a second venv for local smoke runs. signal_core itself imports
NEITHER (enforced by tests/test_signal_core.py in the backtester repo)."""
try:
    from ib_async import IB, ScannerSubscription, Stock, TagValue  # noqa: F401
    IB_LIB = "ib_async"
except ImportError:                                      # dev fallback
    from ib_insync import IB, ScannerSubscription, Stock, TagValue  # noqa: F401
    IB_LIB = "ib_insync"
