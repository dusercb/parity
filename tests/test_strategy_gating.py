"""One strategy, enforced -- not assumed.

The wedge was removed from the live path on 2026-08-16: no LiveWedgeWatch, no
second engine pass, no wedge sizing caps, no coil early-warning. But the strategy
set still lives in the backtester's config.yaml, the shared parity anchor, which
the bot does not own and which anyone can flip back.

So "this bot is dip-only" cannot be left as an assumption sitting in deleted
code. If the anchor ever selects the wedge again, the backtest would trade two
engines while the live book traded one, and the parity numbers the whole project
rests on would be comparing different strategies. Observer.__init__ refuses to
start instead, exactly as the pin and paper-account guards do.

Historical recordings are a separate concern and deliberately still readable:
sessions before 2026-08-16 traded the wedge, and a replay must reproduce the
session it is replaying rather than today's config.
"""
import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

VPS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(VPS))
sys.path.insert(0, str(VPS.parent / "hindsight" / "ibkr_premarket_backtester"))

from live_bot.coverage import CoverageManager, MonitorEntry
from live_bot.recorder import session_strategies
from live_bot.session_controller import STRATEGIES, Observer, _require_dip_only


def _observer(tmp_path, trading: bool):
    cfg = copy.deepcopy(yaml.safe_load((VPS / "bot_config.yaml").read_text()))
    cfg["signal"]["backtester_path"] = str(
        (VPS.parent / "hindsight" / "ibkr_premarket_backtester").resolve())
    cfg["signal"]["pin"] = "UNPINNED-DEV"
    cfg["trading"]["enabled"] = trading
    cfg["recording"]["dir"] = str(tmp_path / "rec")
    cfg["state"]["dir"] = str(tmp_path / "state")
    p = tmp_path / "bot_config.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return Observer(p)


class _SC:
    """signal_core stand-in exposing only the truth table under test."""
    def __init__(self, active):
        self._active = active

    def active_strategies(self, _wedge_cfg):
        return self._active


def _cfg():
    return SimpleNamespace(wedge=SimpleNamespace(enabled=False, concurrent_with_dip=True))


# --- the anchor guard ---------------------------------------------------------

def test_a_dip_only_anchor_is_accepted():
    _require_dip_only(_SC(("dip",)), _cfg())          # must not raise


@pytest.mark.parametrize("active", [("dip", "wedge"), ("wedge",)])
def test_an_anchor_that_selects_the_wedge_refuses_to_run(active):
    """The bot has no wedge implementation left, so honouring this is impossible
    and ignoring it is a silent Class-0 divergence. Neither is acceptable."""
    with pytest.raises(SystemExit) as e:
        _require_dip_only(_SC(active), _cfg())
    msg = str(e.value)
    assert "STRATEGY MISMATCH" in msg
    assert "wedge.enabled: false" in msg, "the message must say how to fix it"


def test_the_real_anchor_currently_selects_dip_only(tmp_path):
    """End to end against the actual backtester config: the Observer builds."""
    obs = _observer(tmp_path, trading=True)
    assert obs.sc.active_strategies(obs.cfg.wedge) == ("dip",)


# --- nothing wedge-shaped survives on the trading path ------------------------

def test_the_controller_has_no_wedge_watch_at_all(tmp_path):
    obs = _observer(tmp_path, trading=True)
    assert not hasattr(obs, "wedge_watch")
    assert STRATEGIES == ("dip",)


def test_the_session_records_the_strategy_set_it_ran(tmp_path):
    obs = _observer(tmp_path, trading=True)
    meta = [__import__("json").loads(l) for l in obs.rec.path.read_text().splitlines()][0]
    assert meta["kind"] == "session_meta"
    assert meta["strategies"] == ["dip"]


def test_the_coverage_early_warning_is_dip_only():
    cm = CoverageManager(max_streamed=5)
    assert not hasattr(cm, "strategies")
    assert not hasattr(cm, "wedge_contraction")
    # a coil near the high -- the wedge's old precursor -- buys nothing now
    coil = MonitorEntry(symbol="T", first_poll=0)
    coil.samples = [1.00, 1.30, 1.02, 1.28, 1.24, 1.27, 1.25, 1.30]
    coil.high = 1.30
    assert cm._early_warning(coil) is None
    # the dip's pullback still fires
    dip = MonitorEntry(symbol="T", first_poll=0)
    dip.samples = [1.00, 1.10, 1.20, 1.30, 1.40, 1.50, 1.60, 1.45]
    dip.high = 1.60
    assert cm._early_warning(dip) == "dip"


# --- historical recordings stay readable --------------------------------------

def test_a_new_recording_reports_its_own_strategy_set():
    assert session_strategies({"strategies": ["dip"]}) == ("dip",)


def test_a_pre_removal_concurrent_session_still_reads_as_dip_plus_wedge():
    """The 08-14 session and its neighbours. A replay of these must run BOTH
    engines or it silently under-reports what the session actually decided."""
    assert session_strategies(
        {"wedge_enabled": True, "wedge_concurrent": True}) == ("dip", "wedge")


def test_a_pre_removal_wedge_only_session_still_reads_as_wedge():
    assert session_strategies(
        {"wedge_enabled": True, "wedge_concurrent": False}) == ("wedge",)


def test_a_session_older_than_the_wedge_reads_as_dip():
    assert session_strategies({}) == ("dip",)
