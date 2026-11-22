"""End-of-session email summary: builds a compact trade digest from the finished
recording (streaming, skipping the big frame dumps) and sends it over SMTP -- OFF
unless configured, and a send failure can never raise. No real SMTP, no network.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from live_bot import notifier


def _rec(tmp_path, rows):
    p = tmp_path / "session_2026-07-27.jsonl"
    p.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    return p


TRADING = [
    {"kind": "session_meta", "trading": True, "pin": "abc123def4567890"},
    {"kind": "trading_armed", "account": "DU1234567"},
    {"kind": "position_opened", "ticker": "VIVK", "engine": "dip_red_break", "direction": "long",
     "shares": 800, "entry": 3.00, "stop": 2.90, "target": 3.10, "leverage_at_entry": 1.5},
    {"kind": "position_opened", "ticker": "EXYN", "engine": "wedge_break", "direction": "long",
     "shares": 300, "entry": 3.22, "stop": 3.17, "target": 3.53},
    {"kind": "partial_banked", "ticker": "VIVK", "shares": 400, "price": 3.10, "pnl": 40.0},
    {"kind": "position_closed", "ticker": "VIVK", "engine": "dip_red_break", "reason": "trail_stop",
     "exit": 3.05, "shares": 400, "pnl_per_share": 0.05, "pnl": 20.0, "realized_session_pnl": 60.0},
    {"kind": "position_closed", "ticker": "EXYN", "engine": "wedge_break", "reason": "stop",
     "exit": 3.17, "shares": 300, "pnl_per_share": -0.05, "pnl": -15.0, "realized_session_pnl": 45.0},
    {"kind": "frame", "symbol": "VIVK", "bars": [[str(i)] * 6 for i in range(5000)]},  # must be skipped
    {"kind": "summary", "tracked": 12, "intended_trades": 2, "scanner_seen": 30, "trading": True,
     "realized_pnl": 45.0, "closed_trades": 2, "open_at_end": 0},
]


def test_build_summary_trading_day(tmp_path):
    subject, text, html = notifier.build_summary(_rec(tmp_path, TRADING))
    assert "2026-07-27" in subject
    assert "2 trade(s)" in subject and "+45.00" in subject and "(1W/1L)" in subject
    # rows present (2 closes + the partial); R computed for the dip close
    for tok in ("VIVK", "EXYN", "dip", "wedge", "trail_stop", "DU1234567"):
        assert tok in text
    assert "R" in text                              # R column header
    assert "+0.50" in text                          # 0.05 / (3.00-2.90) = +0.5R
    assert "<table" in html and "VIVK" in html


def test_frame_lines_are_skipped_not_parsed(tmp_path):
    # the 5000-bar frame row must not appear in the summary nor blow anything up
    _subject, text, _html = notifier.build_summary(_rec(tmp_path, TRADING))
    assert "bars" not in text and "4999" not in text


def test_observer_day_reports_signals_not_trades(tmp_path):
    rows = [
        {"kind": "session_meta", "trading": False, "pin": "x"},
        {"kind": "intended_trade", "ticker": "AXIL", "engine": "dip", "entry_price": 5.0},
        {"kind": "summary", "tracked": 8, "intended_trades": 1, "scanner_seen": 20, "trading": False},
    ]
    subject, text, _html = notifier.build_summary(_rec(tmp_path, rows))
    assert "observer" in subject and "1 signal(s)" in subject
    assert "No trades today." in text


def test_alarms_flag_the_subject(tmp_path):
    rows = TRADING + [{"kind": "reconcile_alarm", "detail": "broker holds 500 ZZZZ with no book position"}]
    subject, _text, html = notifier.build_summary(_rec(tmp_path, rows))
    assert subject.startswith("[ALARM]")
    assert "ZZZZ" in html


# -- config + send -------------------------------------------------------------

def test_email_off_when_disabled_or_misconfigured():
    assert notifier.load_email_config({}) is None
    assert notifier.load_email_config({"email": {"enabled": False}}) is None
    assert notifier.load_email_config({"email": {"enabled": True, "smtp_host": "h"}}) is None  # missing fields


def test_email_config_comes_from_gitignored_local_file_over_bot_config(tmp_path):
    # the gitignored email.local.yaml wins, and bot_config need carry no addresses
    (tmp_path / "email.local.yaml").write_text(
        "email:\n  enabled: true\n  smtp_host: smtp.local\n  username: s@x.com\n"
        "  from: s@x.com\n  to:\n    - a@x.com\n    - b@x.com\n")
    cfg = notifier.load_email_config({"email": {"enabled": False}}, root=tmp_path)
    assert cfg is not None and cfg.host == "smtp.local"
    assert cfg.to_addrs == ["a@x.com", "b@x.com"]      # multiple recipients


def test_no_local_file_and_no_block_means_off(tmp_path):
    assert notifier.load_email_config({}, root=tmp_path) is None  # nothing configured


def test_email_config_reads_password_from_env(monkeypatch):
    monkeypatch.setenv("LIVE_BOT_SMTP_PASSWORD", "s3cret")
    cfg = notifier.load_email_config({"email": {
        "enabled": True, "smtp_host": "smtp.example.com", "smtp_port": 587,
        "username": "u@example.com", "from": "u@example.com", "to": "d@example.com"}})
    assert cfg is not None and cfg.password == "s3cret"
    assert cfg.to_addrs == ["d@example.com"] and cfg.port == 587


class _FakeSMTP:
    sent = []

    def __init__(self, host, port, timeout=None):
        self.host = host

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def starttls(self, context=None):
        pass

    def login(self, u, p):
        pass

    def send_message(self, msg):
        _FakeSMTP.sent.append(msg)


def _cfg(**over):
    base = {"enabled": True, "smtp_host": "smtp.example.com", "smtp_port": 587,
            "username": "u@example.com", "from": "u@example.com", "to": ["d@example.com"]}
    base.update(over)
    return {"email": base}


def test_send_session_summary_sends_when_configured(tmp_path, monkeypatch):
    monkeypatch.setattr(notifier.smtplib, "SMTP", _FakeSMTP)
    monkeypatch.setenv("LIVE_BOT_SMTP_PASSWORD", "pw")
    _FakeSMTP.sent.clear()
    ok = notifier.send_session_summary(_cfg(), _rec(tmp_path, TRADING))
    assert ok is True and len(_FakeSMTP.sent) == 1
    msg = _FakeSMTP.sent[0]
    assert msg["To"] == "d@example.com" and "2 trade(s)" in msg["Subject"]


def test_send_is_a_noop_when_off(tmp_path):
    assert notifier.send_session_summary({}, _rec(tmp_path, TRADING)) is False


def test_send_never_raises_on_smtp_failure(tmp_path, monkeypatch):
    class _Boom(_FakeSMTP):
        def send_message(self, msg):
            raise OSError("smtp down")
    monkeypatch.setattr(notifier.smtplib, "SMTP", _Boom)
    logs = []
    ok = notifier.send_session_summary(_cfg(), _rec(tmp_path, TRADING),
                                       logger=lambda k, **p: logs.append((k, p)))
    assert ok is False                              # swallowed, not raised
    assert logs and logs[-1][0] == "email_error"
