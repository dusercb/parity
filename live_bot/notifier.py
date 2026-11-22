"""End-of-session email summary (PORTING_TO_LIVE.md §12 observability).

Sent ONCE at session end, so it costs nothing during the trading session: it
streams the day's finished recording file (skipping the big `frame` bar-dumps
without parsing them), formats a compact summary of the trades, and sends it over
SMTP. Fully optional and OFF unless bot_config's `email:` block is enabled; a send
failure is logged and swallowed so it can NEVER affect trading or the daemon loop.

Credentials are never stored in config or git: the SMTP password is read from the
environment variable named by `email.password_env` (default LIVE_BOT_SMTP_PASSWORD).
"""
from __future__ import annotations

import json
import os
import smtplib
import ssl
from dataclasses import dataclass, field
from email.message import EmailMessage
from pathlib import Path


@dataclass
class EmailConfig:
    host: str
    port: int
    username: str
    password: str
    from_addr: str
    to_addrs: list[str]
    use_starttls: bool = True


def _load_email_block(bcfg: dict, root) -> dict:
    """The email settings dict. ALL email addresses (sender AND recipients) and
    the SMTP host live in the GITIGNORED `email.local.yaml` beside bot_config, so
    they are never committed -- copy email.example.yaml and fill it in on the box.
    Falls back to a bot_config `email:` block only if no local file exists (kept
    for tests / a machine that puts non-secret settings inline)."""
    if root is not None:
        local = Path(root) / "email.local.yaml"
        if local.exists():
            try:
                import yaml
                return (yaml.safe_load(local.read_text()) or {}).get("email", {}) or {}
            except Exception:
                return {}
    return bcfg.get("email") or {}


def load_email_config(bcfg: dict, root=None) -> EmailConfig | None:
    """Build the SMTP config from the gitignored email.local.yaml (or a bot_config
    `email:` fallback), or None if off/misconfigured (treated as off -- never a
    crash). The password always comes from the environment, never a file."""
    e = _load_email_block(bcfg, root)
    if not e.get("enabled"):
        return None
    to = e.get("to") or []
    if isinstance(to, str):
        to = [to]
    host, username, from_addr = e.get("smtp_host"), e.get("username"), e.get("from")
    if not (host and username and from_addr and to):
        return None
    password = os.environ.get(e.get("password_env", "LIVE_BOT_SMTP_PASSWORD"), "")
    return EmailConfig(host=host, port=int(e.get("smtp_port", 587)), username=username,
                       password=password, from_addr=from_addr, to_addrs=list(to),
                       use_starttls=bool(e.get("use_starttls", True)))


# -- summary from the recording ------------------------------------------------

@dataclass
class _Session:
    date: str = ""
    trading: bool = False
    account: str = ""
    pin: str = ""
    opens: dict = field(default_factory=dict)     # (ticker, engine) -> opened payload
    closes: list = field(default_factory=list)    # position_closed payloads
    partials: list = field(default_factory=list)  # partial_banked payloads
    tracked: int = 0
    signals: int = 0
    scanner_seen: int = 0
    open_at_end: int | None = None
    realized: float | None = None
    broker_realized: float | None = None   # IBKR's own figure, from commission reports
    commission: float | None = None
    audit: dict | None = None              # end-of-session broker reconciliation
    alarms: list = field(default_factory=list)
    notices: list = field(default_factory=list)   # noteworthy, but not [ALARM]-worthy


def _line(e: dict) -> str:
    """One-line rendering of an alarm/notice. Free-text events carry `detail` or
    `error`; the structured operational events carry named fields instead, and
    dropping those would report the event with no content at all."""
    d = e.get("detail") or e.get("error")
    if d:
        return str(d)
    return " ".join(f"{k}={v}" for k, v in e.items() if k not in ("kind", "ts"))


def _collect(path: Path) -> _Session:
    s = _Session(date=Path(path).stem.replace("session_", ""))
    try:
        fh = open(path, encoding="utf-8")
    except FileNotFoundError:
        return s
    with fh:
        for line in fh:
            # Skip the big per-symbol bar dumps WITHOUT parsing them -- they are
            # the only large rows and the summary never needs them.
            if '"kind": "frame"' in line or '"kind":"frame"' in line:
                continue
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except Exception:
                continue
            k = e.get("kind")
            if k == "session_meta":
                s.trading = bool(e.get("trading", s.trading)); s.pin = e.get("pin", s.pin)
            elif k == "trading_armed":
                s.account = e.get("account", s.account)
            elif k == "position_opened":
                s.opens[(e.get("ticker"), e.get("engine"))] = e
            elif k == "position_closed":
                s.closes.append(e)
            elif k == "partial_banked":
                s.partials.append(e)
            elif k == "broker_audit":
                s.audit = e
            elif k in ("reconcile_alarm", "engine_error", "flatten_incomplete", "scanner_error",
                       # the bot's model of its own trading disagreeing with the
                       # account is the single most alarming thing in the file
                       "broker_audit_drift", "broker_audit_error", "commission_event_error"):
                s.alarms.append(e)
            elif k in ("entry_stale_cancel", "target_unconfirmed", "entry_partial_fill",
                       "exit_partial_fill", "exit_unfilled", "entry_cancelled_at_flatten"):
                # An abandoned entry or an escalated target is a normal, handled
                # outcome -- it must be VISIBLE (BOXL 2026-08-14 leaked for 3h34m
                # and left no trace in the recording at all) without crying ALARM
                # over the whole day and training the prefix out of meaning.
                s.notices.append(e)
            elif k == "summary":
                s.tracked = e.get("tracked", 0); s.signals = e.get("intended_trades", 0)
                s.scanner_seen = e.get("scanner_seen", 0); s.open_at_end = e.get("open_at_end")
                if e.get("realized_pnl") is not None:
                    s.realized = e.get("realized_pnl")
                if e.get("broker_realized_pnl") is not None:
                    s.broker_realized = e.get("broker_realized_pnl")
                if e.get("commission") is not None:
                    s.commission = e.get("commission")
                s.trading = bool(e.get("trading", s.trading))
    return s


def _rows(s: _Session) -> list[dict]:
    """One row per realized leg (a full close, or a banked partial), joined to its
    open for entry/direction/R where available."""
    rows = []
    for c in s.closes:
        o = s.opens.get((c.get("ticker"), c.get("engine")), {})
        entry, stop = o.get("entry"), o.get("stop")
        pps = c.get("pnl_per_share")
        r = None
        if pps is not None and entry is not None and stop is not None and abs(entry - stop) > 0:
            r = pps / abs(entry - stop)
        rows.append(dict(ticker=c.get("ticker"), engine=c.get("engine"),
                         direction=o.get("direction", ""), entry=entry, exit=c.get("exit"),
                         shares=c.get("shares"), pnl=c.get("pnl"), reason=c.get("reason"), r=r))
    for p in s.partials:
        o = s.opens.get((p.get("ticker"),), {})
        rows.append(dict(ticker=p.get("ticker"), engine="partial", direction="",
                         entry=None, exit=p.get("price"), shares=p.get("shares"),
                         pnl=p.get("pnl"), reason="target (partial)", r=None))
    return rows


def build_summary(path) -> tuple[str, str, str]:
    """(subject, text_body, html_body) for a finished session recording."""
    s = _collect(Path(path))
    rows = _rows(s)
    pnl = s.realized if s.realized is not None else round(sum((r["pnl"] or 0.0) for r in rows), 2)
    # A "trade" is a closed POSITION; partials show as extra table rows but don't
    # inflate the trade/win/loss counts.
    cl = [(c.get("pnl") or 0.0) for c in s.closes]
    n = len(cl)
    wins = sum(1 for p in cl if p > 0)
    losses = n - wins

    if s.trading:
        subject = f"[live_bot] {s.date} - {n} trade(s), {pnl:+.2f} ({wins}W/{losses}L)"
    else:
        subject = f"[live_bot] {s.date} - observer: {s.signals} signal(s), {s.tracked} tracked"
    if s.alarms:
        subject = "[ALARM] " + subject

    # -- plain text --
    tl = [subject, "", f"mode: {'TRADING' if s.trading else 'observer'}"
          f"{'  account ' + s.account if s.account else ''}"
          f"{'  pin ' + str(s.pin)[:12] if s.pin else ''}", ""]
    if rows:
        tl.append(f"{'ticker':<7}{'strat':<8}{'dir':<6}{'entry':>8}{'exit':>8}{'shares':>8}{'pnl':>10}{'R':>6}  reason")
        for r in rows:
            tl.append(f"{(r['ticker'] or '?'):<7}{_short(r['engine']):<8}{(r['direction'] or ''):<6}"
                      f"{_num(r['entry']):>8}{_num(r['exit']):>8}{_int(r['shares']):>8}"
                      f"{_money(r['pnl']):>10}{(f'{r['r']:+.2f}' if r['r'] is not None else ''):>6}  {r['reason'] or ''}")
    else:
        tl.append("No trades today.")
    tl += ["", f"realized P&L: {pnl:+.2f}   wins {wins}  losses {losses}"
           f"   win rate {(wins / n * 100):.0f}%" if n else f"realized P&L: {pnl:+.2f}"]
    if s.broker_realized is not None:
        # The model's P&L is gross of fees and computed from observed fills; this
        # is what the account actually did. Showing both is the point.
        line = f"broker realized: {s.broker_realized:+.2f}"
        if s.commission is not None:
            line += f"   commission {s.commission:.2f}"
        if s.realized is not None:
            line += f"   delta vs model {s.broker_realized - s.realized:+.2f}"
        tl.append(line)
    tl.append(f"tracked {s.tracked}   scanner-seen {s.scanner_seen}   signals {s.signals}"
              f"   open at end {s.open_at_end if s.open_at_end is not None else 0}")
    if s.alarms:
        tl += ["", "ALARMS:"] + [f"  - {a.get('kind')}: {_line(a)}" for a in s.alarms]
    if s.notices:
        tl += ["", "NOTICES:"] + [f"  - {a.get('kind')}: {_line(a)}" for a in s.notices]
    text = "\n".join(tl)

    # -- html --
    html = _html(subject, s, rows, pnl, wins, losses, n)
    return subject, text, html


def _short(engine):
    return {"dip_red_break": "dip", "wedge_break": "wedge", "partial": "partial"}.get(engine, engine or "?")


def _num(x):
    return f"{x:.2f}" if isinstance(x, (int, float)) else "-"


def _int(x):
    return str(int(x)) if isinstance(x, (int, float)) else "-"


def _money(x):
    return f"{x:+.2f}" if isinstance(x, (int, float)) else "-"


def _html(subject, s, rows, pnl, wins, losses, n):
    pos = "#137333"; neg = "#c5221f"; muted = "#5f6368"
    head = (f"<h2 style='margin:0 0 4px;font:600 18px system-ui'>{subject}</h2>"
            f"<div style='color:{muted};font:13px system-ui'>mode {'TRADING' if s.trading else 'observer'}"
            f"{'&nbsp;&middot;&nbsp;account ' + s.account if s.account else ''}"
            f"{'&nbsp;&middot;&nbsp;pin ' + str(s.pin)[:12] if s.pin else ''}</div>")
    if rows:
        trs = ""
        for r in rows:
            c = pos if (r["pnl"] or 0) > 0 else neg
            trs += ("<tr>"
                    f"<td style='padding:4px 8px'>{r['ticker'] or '?'}</td>"
                    f"<td style='padding:4px 8px;color:{muted}'>{_short(r['engine'])}</td>"
                    f"<td style='padding:4px 8px'>{r['direction'] or ''}</td>"
                    f"<td style='padding:4px 8px;text-align:right'>{_num(r['entry'])}</td>"
                    f"<td style='padding:4px 8px;text-align:right'>{_num(r['exit'])}</td>"
                    f"<td style='padding:4px 8px;text-align:right'>{_int(r['shares'])}</td>"
                    f"<td style='padding:4px 8px;text-align:right;color:{c};font-weight:600'>{_money(r['pnl'])}</td>"
                    f"<td style='padding:4px 8px;text-align:right'>{f'{r['r']:+.2f}' if r['r'] is not None else ''}</td>"
                    f"<td style='padding:4px 8px;color:{muted}'>{r['reason'] or ''}</td></tr>")
        table = ("<table style='border-collapse:collapse;font:13px system-ui;margin:10px 0'>"
                 "<thead><tr style='text-align:left;border-bottom:1px solid #ddd'>"
                 "<th style='padding:4px 8px'>ticker</th><th style='padding:4px 8px'>strat</th>"
                 "<th style='padding:4px 8px'>dir</th><th style='padding:4px 8px;text-align:right'>entry</th>"
                 "<th style='padding:4px 8px;text-align:right'>exit</th><th style='padding:4px 8px;text-align:right'>shares</th>"
                 "<th style='padding:4px 8px;text-align:right'>P&amp;L</th><th style='padding:4px 8px;text-align:right'>R</th>"
                 f"<th style='padding:4px 8px'>reason</th></tr></thead><tbody>{trs}</tbody></table>")
    else:
        table = "<p style='font:13px system-ui'>No trades today.</p>"
    pc = pos if pnl > 0 else neg
    totals = (f"<div style='font:14px system-ui;margin-top:8px'>realized "
              f"<b style='color:{pc}'>{pnl:+.2f}</b>"
              f"{f' &middot; wins {wins} losses {losses} &middot; win rate {wins/n*100:.0f}%' if n else ''}</div>"
              f"<div style='color:{muted};font:12px system-ui;margin-top:4px'>tracked {s.tracked}"
              f" &middot; scanner-seen {s.scanner_seen} &middot; signals {s.signals}"
              f" &middot; open at end {s.open_at_end if s.open_at_end is not None else 0}</div>")
    alarms = ""
    if s.alarms:
        items = "".join(f"<li>{a.get('kind')}: {_line(a)}</li>" for a in s.alarms)
        alarms = f"<div style='margin-top:10px;color:{neg};font:13px system-ui'><b>ALARMS</b><ul>{items}</ul></div>"
    notices = ""
    if s.notices:
        items = "".join(f"<li>{a.get('kind')}: {_line(a)}</li>" for a in s.notices)
        notices = (f"<div style='margin-top:10px;color:{muted};font:13px system-ui'>"
                   f"<b>NOTICES</b><ul>{items}</ul></div>")
    return f"<div style='max-width:720px'>{head}{table}{totals}{alarms}{notices}</div>"


# -- send ----------------------------------------------------------------------

def _send(cfg: EmailConfig, subject: str, text: str, html: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.from_addr
    msg["To"] = ", ".join(cfg.to_addrs)
    msg.set_content(text)
    msg.add_alternative(html, subtype="html")
    with smtplib.SMTP(cfg.host, cfg.port, timeout=30) as srv:
        if cfg.use_starttls:
            srv.starttls(context=ssl.create_default_context())
        if cfg.password:
            srv.login(cfg.username, cfg.password)
        srv.send_message(msg)


def send_session_summary(bcfg: dict, recording_path, root=None, logger=None) -> bool:
    """Build + send the end-of-session summary if email is configured (in the
    gitignored email.local.yaml beside bot_config, found via `root`). Returns True
    if sent. NEVER raises -- any failure (no config, SMTP down, bad creds) is
    logged via `logger(kind, **fields)` and swallowed, so the trading session and
    daemon are never affected by a mail problem."""
    try:
        cfg = load_email_config(bcfg, root)
        if cfg is None:
            return False
        subject, text, html = build_summary(recording_path)
        _send(cfg, subject, text, html)
        if logger:
            logger("email_sent", to=",".join(cfg.to_addrs), subject=subject)
        return True
    except Exception as e:
        if logger:
            logger("email_error", error=repr(e))
        return False
