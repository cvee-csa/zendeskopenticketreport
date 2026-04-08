#!/usr/bin/env python3
"""
IT Ops SLA Breach Report — sla_breach_report.py

Shows all open IT-Operations tickets, highlights SLA breaches,
tracks when Ryan was last tagged, and recommends the next step
(Slack Ryan the ticket URL, or tag Ryan in the ticket).

Required environment variables:
    ZENDESK_EMAIL   your Zendesk login email
    ZENDESK_TOKEN   Zendesk API token
"""

import os, re, time, base64, json, html as _html
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    GDRIVE_AVAILABLE = True
except ImportError:
    GDRIVE_AVAILABLE = False

# ── Credentials ──────────────────────────────────────────────────────────────
ZENDESK_EMAIL = os.environ.get("ZENDESK_EMAIL", "")
ZENDESK_TOKEN = os.environ.get("ZENDESK_TOKEN", "")

GDRIVE_SA_JSON   = os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON")
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")

# ── Constants ────────────────────────────────────────────────────────────────
ZENDESK_DOMAIN = "cloudsecurityalliance.zendesk.com"
BASE_ZD        = f"https://{ZENDESK_DOMAIN}/api/v2"
TICKET_URL     = f"https://{ZENDESK_DOMAIN}/agent/tickets/"

_PACIFIC = ZoneInfo("America/Los_Angeles")
_now     = datetime.now(_PACIFIC)
TODAY    = _now.strftime("%Y-%m-%d")
NOW      = _now.strftime("%Y-%m-%d_%I%M") + ("am" if _now.hour < 12 else "pm")
REPORT_PATH = os.environ.get("OUTPUT_FILE", f"/tmp/IT_Ops_SLA_Report_{NOW}.xlsx")

IT_OPS_GROUPS = {
    7783360594455:  "IT-Operations",
    37981538647191: "IT-Operations-Projects",
    38675924427287: "IT-Operations-Tasks",
}

IT_OPS_AGENT_IDS = {19148954105367, 5720866160535, 38942574549655}  # Neeks, Jacob, Catherine
RYAN_ID = 396710941733

# ── SLA thresholds (business hours) ─────────────────────────────────────────
SLA_INITIAL_RESPONSE_HRS = 2   # first IT Ops comment within 2 biz hrs
SLA_REQUESTER_WAIT_HRS   = 4   # max biz hours requester waits unanswered
SLA_NO_UPDATE_HRS        = 8   # 1 biz day = 8 hrs max since any update
SLA_RESOLUTION_DAYS      = 2   # open > 2 biz days flagged


# ── Zendesk API helpers ─────────────────────────────────────────────────────
def _zd_headers():
    token = base64.b64encode(
        f"{ZENDESK_EMAIL}/token:{ZENDESK_TOKEN}".encode()
    ).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def fetch_tickets():
    """Fetch IT Ops open/pending/hold tickets via incremental cursor export."""
    LOOKBACK_DAYS = 60
    since = int((datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).timestamp())
    print(f"  Fetching tickets (last {LOOKBACK_DAYS} days)...")

    url = f"{BASE_ZD}/incremental/tickets/cursor.json?start_time={since}"
    all_tickets = []
    batch = 1

    while url:
        r = requests.get(url, headers=_zd_headers(), timeout=60)
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", 60)))
            continue
        r.raise_for_status()
        data = r.json()
        all_tickets.extend(data.get("tickets", []))
        print(f"  Batch {batch}: {len(data.get('tickets', []))} tickets")
        if data.get("end_of_stream", False):
            break
        after_url = data.get("after_url")
        if not after_url or after_url == url:
            break
        url = after_url
        batch += 1
        time.sleep(0.5)

    it_ops_ids = set(IT_OPS_GROUPS.keys())
    tickets = [
        t for t in all_tickets
        if t.get("group_id") in it_ops_ids
        and t.get("status") in {"open", "pending", "hold"}
    ]
    print(f"  {len(tickets)} IT Ops open/pending/hold tickets (of {len(all_tickets)} total)")
    return tickets


def fetch_comments(ticket_id):
    """Return comment list for a ticket with retry logic."""
    url = f"{BASE_ZD}/tickets/{ticket_id}/comments.json"
    for attempt in range(3):
        r = requests.get(url, headers=_zd_headers(), timeout=30)
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", 10)))
            continue
        if r.status_code in (500, 502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json().get("comments", [])
    return []


# ── SLA calculation ─────────────────────────────────────────────────────────
def _biz_hours_between(start_utc: datetime, end_utc: datetime) -> float:
    """Count business hours (Mon-Fri, 09:00-17:00 Pacific) between two UTC datetimes."""
    if end_utc <= start_utc:
        return 0.0
    PST_OFFSET = timedelta(hours=-8)
    s = (start_utc + PST_OFFSET).replace(tzinfo=None)
    e = (end_utc + PST_OFFSET).replace(tzinfo=None)
    total = 0.0
    day = s.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = e.replace(hour=0, minute=0, second=0, microsecond=0)
    while day <= end_day:
        if day.weekday() < 5:
            seg_start = max(s, day.replace(hour=9))
            seg_end = min(e, day.replace(hour=17))
            if seg_end > seg_start:
                total += (seg_end - seg_start).total_seconds() / 3600
        day += timedelta(days=1)
    return total


def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def check_sla(ticket: dict, comments: list) -> dict:
    """
    Evaluate SLA compliance. Returns:
        breached  — bool, True if any SLA threshold is exceeded
        flags     — list of human-readable flag strings
        severity  — "alert", "warn", or "ok"
        display   — formatted flag string
    """
    now = datetime.now(timezone.utc)
    created_at = _parse_dt(ticket.get("created_at", ""))
    updated_at = _parse_dt(ticket.get("updated_at", ""))
    req_id = ticket.get("requester_id")
    it_ops_cmts = [c for c in comments if c.get("author_id") in IT_OPS_AGENT_IDS]

    flags = []
    severity = "ok"

    # 1. Initial response: first IT Ops comment within 2 biz hrs
    if created_at:
        if it_ops_cmts:
            first_resp_dt = _parse_dt(it_ops_cmts[0].get("created_at", ""))
            if first_resp_dt:
                hrs = _biz_hours_between(created_at, first_resp_dt)
                if hrs > SLA_INITIAL_RESPONSE_HRS:
                    flags.append(f"1st response: {hrs:.0f}h (>{SLA_INITIAL_RESPONSE_HRS}h)")
                    severity = "warn"
        else:
            hrs = _biz_hours_between(created_at, now)
            if hrs > SLA_INITIAL_RESPONSE_HRS:
                flags.append(f"No response: {hrs:.0f}h")
                severity = "alert"

    # 2. Requester wait: unanswered for >4 biz hrs
    if req_id and comments:
        last_req = next((c for c in reversed(comments) if c.get("author_id") == req_id), None)
        if last_req:
            req_dt = _parse_dt(last_req.get("created_at", ""))
            req_idx = next((i for i, c in enumerate(comments) if c["id"] == last_req["id"]), -1)
            answered = any(c.get("author_id") in IT_OPS_AGENT_IDS for c in comments[req_idx + 1:])
            if not answered and req_dt:
                hrs = _biz_hours_between(req_dt, now)
                if hrs > SLA_REQUESTER_WAIT_HRS:
                    flags.append(f"Unanswered: {hrs:.0f}h (>{SLA_REQUESTER_WAIT_HRS}h)")
                    severity = "alert"

    # 3. No update: stale for >8 biz hrs
    if updated_at:
        hrs = _biz_hours_between(updated_at, now)
        if hrs > SLA_NO_UPDATE_HRS:
            flags.append(f"Stale: {hrs:.0f}h (>{SLA_NO_UPDATE_HRS}h)")
            if severity == "ok":
                severity = "warn"

    # 4. Resolution: open > 2 biz days (informational)
    if created_at:
        open_hrs = _biz_hours_between(created_at, now)
        open_days = open_hrs / 8
        if open_days > SLA_RESOLUTION_DAYS:
            flags.append(f"Age: {open_days:.0f}d")

    display = " | ".join(flags) if flags else "OK"
    breached = severity in ("warn", "alert")
    return {"breached": breached, "flags": flags, "display": display, "severity": severity}


# ── Ryan tracking ───────────────────────────────────────────────────────────
def last_ryan_mention(comments) -> dict:
    """
    Find the most recent comment mentioning Ryan.
    Returns: {"date": "MM/DD/YYYY" or "", "days_ago": int or None, "found": bool}
    """
    ryan_cmts = [
        c for c in comments
        if re.search(r"ryan", c.get("plain_body") or c.get("body") or "", re.IGNORECASE)
    ]
    if not ryan_cmts:
        return {"date": "", "days_ago": None, "found": False}

    last_dt = max(
        datetime.fromisoformat(c["created_at"].replace("Z", "+00:00"))
        for c in ryan_cmts
    )
    days_ago = (datetime.now(timezone.utc) - last_dt).days
    return {"date": last_dt.strftime("%m/%d/%Y"), "days_ago": days_ago, "found": True}


def recommend_next_step(ryan_info: dict, sla: dict) -> str:
    """
    Determine the recommended next step based on Ryan involvement and SLA status.
    Returns one of:
        - "Slack Ryan ticket URL"  (Ryan tagged >3 days ago with no response)
        - "Tag Ryan in ticket"     (Ryan not yet mentioned in the ticket)
        - "Allow time to respond"  (Ryan tagged recently, ≤3 days)
        - ""                       (within SLA, no action needed)
    """
    if not sla["breached"]:
        if ryan_info["found"] and ryan_info["days_ago"] is not None and ryan_info["days_ago"] > 7:
            return "Slack Ryan ticket URL"
        return ""

    # SLA is breached — decide based on Ryan involvement
    if not ryan_info["found"]:
        return "Tag Ryan in ticket"

    days = ryan_info["days_ago"]
    if days is None:
        return "Tag Ryan in ticket"
    if days <= 3:
        return "Allow time to respond"
    return "Slack Ryan ticket URL"


# ── ESC / RARC classification ───────────────────────────────────────────────
ESC_PATTERNS = [
    re.compile(r"blocked\s+on", re.IGNORECASE),
    re.compile(r"waiting\s+on\s+(ryan|kurt|dev|r&d|leadership)", re.IGNORECASE),
    re.compile(r"need[s]?\s+(ryan|kurt|dev|leadership|r&d|approval)", re.IGNORECASE),
    re.compile(r"(ryan|kurt)\s+(need[s]?|has\s+to|must|should|is\s+required)", re.IGNORECASE),
    re.compile(r"pending\s+(ryan|kurt|dev|leadership|r&d|approval)", re.IGNORECASE),
    re.compile(r"requires?\s+(ryan|kurt|dev|leadership|approval)", re.IGNORECASE),
    re.compile(r"escalat", re.IGNORECASE),
    re.compile(r"business\s+decision", re.IGNORECASE),
    re.compile(r"leadership\s+decision", re.IGNORECASE),
    re.compile(r"waiting\s+for\s+(a\s+)?(response|decision|approval|review)", re.IGNORECASE),
    re.compile(r"no\s+response\s+(from|since)", re.IGNORECASE),
    re.compile(r"ryan\s+bergsma", re.IGNORECASE),
    re.compile(r"kurt\s+seigfried", re.IGNORECASE),
]

RARC_PATTERNS = [
    re.compile(r"can\s+you\s+confirm", re.IGNORECASE),
    re.compile(r"please\s+confirm", re.IGNORECASE),
    re.compile(r"let\s+me\s+know\s+if", re.IGNORECASE),
    re.compile(r"does\s+this\s+(work|look\s+right|meet)", re.IGNORECASE),
    re.compile(r"is\s+this\s+satisfactory", re.IGNORECASE),
    re.compile(r"can\s+we\s+(go\s+ahead\s+and\s+)?close", re.IGNORECASE),
    re.compile(r"please\s+verify", re.IGNORECASE),
    re.compile(r"good\s+to\s+(go|close)", re.IGNORECASE),
    re.compile(r"everything\s+(look|work|seem)\s+(good|ok|right)", re.IGNORECASE),
    re.compile(r"(ticket|this)\s+can\s+be\s+closed", re.IGNORECASE),
    re.compile(r"let\s+us\s+know\s+when", re.IGNORECASE),
    re.compile(r"confirm.*and\s+(we|i)\s+(will|can|shall)\s+close", re.IGNORECASE),
]


def classify_esc_rarc(ticket: dict, comments: list) -> str:
    """Classify ticket as 'esc', 'rarc', or '' (neither)."""
    # RARC: check last IT Ops comment for close-ready language
    it_ops_cmts = [c for c in comments if c.get("author_id") in IT_OPS_AGENT_IDS]
    if it_ops_cmts:
        last_itops = it_ops_cmts[-1]
        last_idx = next((i for i, c in enumerate(comments) if c["id"] == last_itops["id"]), -1)
        subsequent = comments[last_idx + 1:]
        req_id = ticket.get("requester_id")
        requester_replied = any(c.get("author_id") == req_id for c in subsequent)
        body = last_itops.get("body") or ""
        if not requester_replied and any(p.search(body) for p in RARC_PATTERNS):
            return "rarc"

    # ESC: check all text for escalation signals
    all_text = " ".join(
        [ticket.get("subject") or "", ticket.get("description") or ""]
        + [c.get("body", "") for c in comments]
    )
    if any(p.search(all_text) for p in ESC_PATTERNS):
        return "esc"

    # On-hold tickets default to ESC
    if ticket.get("status") == "hold":
        return "esc"

    return ""


def build_claude_prompt(ticket_id: int, subject: str, tag: str,
                        next_step: str, sla_display: str) -> str:
    """Build a ticket-specific Claude prompt for automation."""
    parts = []

    # 1. Apply ESC or RARC tag
    if tag == "esc":
        parts.append(
            f'Apply the "esc" tag to Zendesk ticket #{ticket_id} '
            f'using the Zendesk API (PUT /api/v2/tickets/{ticket_id}) '
            f'— add "esc" to the ticket\'s tags array. '
            f'This ticket needs escalation.'
        )
    elif tag == "rarc":
        parts.append(
            f'Apply the "rarc" tag to Zendesk ticket #{ticket_id} '
            f'using the Zendesk API (PUT /api/v2/tickets/{ticket_id}) '
            f'— add "rarc" to the ticket\'s tags array. '
            f'This ticket is ready to close pending requester confirmation.'
        )

    # 2. Next step action
    if next_step == "Tag Ryan in ticket":
        parts.append(
            f'Add an internal note to Zendesk ticket #{ticket_id} tagging '
            f'Ryan Bergsma for review. The ticket "{subject}" needs his attention. '
            f'SLA status: {sla_display}.'
        )
    elif next_step == "Allow time to respond":
        parts.append(
            f'No action needed yet for ticket #{ticket_id} — Ryan was recently tagged. '
            f'Check back if no response within 3 business days.'
        )

    if not parts:
        return ""

    return " ".join(parts)


# ── Spreadsheet builder — CSA Brand Colors ─────────────────────────────────
CSA_NAVY      = "003366"   # primary — deep navy (logo "CSA" text)
CSA_ORANGE    = "E87722"   # secondary — orange (logo "cloud security alliance")
CSA_BLUE      = "0085CA"   # accent — bright blue (logo bar, links)
CSA_LIGHT     = "E8F1F8"   # light blue tint (backgrounds)
CSA_WHITE     = "FFFFFF"
CSA_DARK_TEXT = "2D3436"   # body text


DARK_HEADER   = CSA_NAVY
BREACH_BG     = "FDE8E8"   # light red for breached rows
BREACH_ALT_BG = "FBDCDC"   # alt row for breached
OK_BG         = CSA_LIGHT  # CSA light blue for OK rows
OK_ALT_BG     = "D6E8F4"   # slightly deeper blue alt
LINK_COLOR    = CSA_BLUE
SUMMARY_BG    = CSA_LIGHT

HEADERS = ["Ticket #", "Subject", "SLA", "Ryan", "Next Step", "Status", "Claude Prompt"]
WIDTHS = [10, 40, 34, 18, 24, 10, 50]

SEVERITY_STYLE = {
    "alert": {"bg": "FBDCDC", "fc": "B71C1C", "label": "BREACH"},
    "warn":  {"bg": "FFF3CD", "fc": "856404", "label": "WARNING"},
    "ok":    {"bg": CSA_LIGHT, "fc": CSA_NAVY, "label": "OK"},
}

STEP_STYLE = {
    "Slack Ryan ticket URL": {"bg": "FBDCDC", "fc": "B71C1C"},
    "Tag Ryan in ticket":    {"bg": "FFF3CD", "fc": "856404"},
    "Allow time to respond": {"bg": CSA_LIGHT, "fc": CSA_NAVY},
    "Follow up in ticket":   {"bg": "FFF3CD", "fc": "856404"},
}


def _border():
    s = Side(style="thin", color="CCCCCC")
    return Border(left=s, right=s, top=s, bottom=s)


def _cell(ws, row, col, value, bold=False, fc=None,
          bg=None, wrap=False, align="left", size=11):
    if fc is None:
        fc = CSA_DARK_TEXT
    c = ws.cell(row=row, column=col, value=value)
    c.font = Font(name="Arial", bold=bold, color=fc, size=size)
    c.alignment = Alignment(horizontal=align, vertical="top", wrap_text=wrap)
    if bg:
        c.fill = PatternFill("solid", start_color=bg)
    c.border = _border()
    return c


def write_report(rows: list[dict], output_path: str):
    """Build the styled SLA breach report spreadsheet."""
    wb = Workbook()

    # ── Main sheet: All Tickets ──────────────────────────────────────────
    ws = wb.active
    ws.title = "All Open Tickets"

    # Summary stats at top
    total = len(rows)
    breached = sum(1 for r in rows if r["sla_breached"])
    alerts = sum(1 for r in rows if r["sla_severity"] == "alert")
    warnings = sum(1 for r in rows if r["sla_severity"] == "warn")
    ryan_involved = sum(1 for r in rows if r["ryan_found"])
    action_needed = sum(1 for r in rows if r["next_step"])

    summary_items = [
        ("Open Tickets:",     str(total),          CSA_NAVY),
        ("SLA Breaches:",     str(breached),        "B71C1C"),
        ("  Alerts:",         str(alerts),          "B71C1C"),
        ("  Warnings:",       str(warnings),        "856404"),
        ("Ryan Involved:",    str(ryan_involved),   CSA_BLUE),
        ("Action Needed:",    str(action_needed),   "B71C1C"),
    ]
    for sr, (label, val, color) in enumerate(summary_items, 1):
        c = ws.cell(row=sr, column=1, value=label)
        c.font = Font(name="Arial", bold=True, size=12, color=color)
        c.fill = PatternFill("solid", start_color=SUMMARY_BG)
        c.alignment = Alignment(horizontal="right", vertical="center")
        c2 = ws.cell(row=sr, column=2, value=int(val))
        c2.font = Font(name="Arial", bold=True, size=14, color=color)
        c2.fill = PatternFill("solid", start_color=SUMMARY_BG)
        c2.alignment = Alignment(horizontal="left", vertical="center")
        for col in range(3, len(HEADERS) + 1):
            sc = ws.cell(row=sr, column=col)
            sc.fill = PatternFill("solid", start_color=SUMMARY_BG)

    spacer_row = len(summary_items) + 1
    ws.row_dimensions[spacer_row].height = 6

    # Header row
    HEADER_ROW = spacer_row + 1
    for ci, (h, w) in enumerate(zip(HEADERS, WIDTHS), 1):
        c = ws.cell(row=HEADER_ROW, column=ci, value=h)
        c.font = Font(name="Arial", bold=True, color="FFFFFF", size=11)
        c.fill = PatternFill("solid", start_color=DARK_HEADER)
        c.alignment = Alignment(horizontal="center", vertical="center")
        c.border = _border()
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[HEADER_ROW].height = 24

    # Data rows — breached tickets first, then OK tickets
    sorted_rows = sorted(rows, key=lambda r: (
        0 if r["sla_severity"] == "alert" else 1 if r["sla_severity"] == "warn" else 2,
        -r["days_open"],
    ))

    cur_row = HEADER_ROW + 1
    for r in sorted_rows:
        breached = r["sla_breached"]
        even = cur_row % 2 == 0
        if breached:
            bg = BREACH_BG if not even else BREACH_ALT_BG
        else:
            bg = OK_BG if not even else OK_ALT_BG

        sev = SEVERITY_STYLE[r["sla_severity"]]

        # Col 1: Ticket #
        tid_cell = _cell(ws, cur_row, 1, r["ticket_id"], bold=True, fc=LINK_COLOR, bg=bg, align="center")
        tid_cell.font = Font(name="Arial", bold=True, color=LINK_COLOR, underline="single", size=11)
        tid_cell.hyperlink = r["ticket_url"]

        # Col 2: Subject
        _cell(ws, cur_row, 2, r["subject"], bg=bg, wrap=True)

        # Col 3: SLA — badge + details merged into one cell
        sla_text = f"{sev['label']} — {r['sla_display']}" if r["sla_display"] != "OK" else "OK"
        _cell(ws, cur_row, 3, sla_text, bold=breached, fc=sev["fc"], bg=sev["bg"], wrap=True, size=10)

        # Col 4: Ryan — "5 days ago (04/02)" or "Never"
        ryan_days = r["ryan_days_ago"]
        if ryan_days is not None:
            ryan_fc = CSA_NAVY if ryan_days <= 3 else "F57F17" if ryan_days <= 7 else "B71C1C"
            ryan_text = f"{ryan_days}d ago ({r['ryan_date']})" if ryan_days > 0 else "Today"
            _cell(ws, cur_row, 4, ryan_text, bold=True, fc=ryan_fc, bg=bg, align="center")
        else:
            _cell(ws, cur_row, 4, "Never", fc="999999", bg=bg, align="center")

        # Col 5: Next Step
        step = r["next_step"]
        if step:
            ss = STEP_STYLE.get(step, {"bg": bg, "fc": CSA_DARK_TEXT})
            _cell(ws, cur_row, 5, step, bold=True, fc=ss["fc"], bg=ss["bg"], wrap=True)
        else:
            _cell(ws, cur_row, 5, "—", fc=CSA_NAVY, bg=bg, align="center")

        # Col 6: Status (open/hold/pending)
        _cell(ws, cur_row, 6, r["ticket_status"].capitalize(), bg=bg, align="center")

        # Col 7: Claude Prompt
        prompt = r.get("claude_prompt", "")
        if prompt:
            _cell(ws, cur_row, 7, prompt, bg=bg, wrap=True, size=9)
        else:
            _cell(ws, cur_row, 7, "", bg=bg)

        ws.row_dimensions[cur_row].height = 48
        cur_row += 1

    ws.freeze_panes = f"A{HEADER_ROW + 1}"

    # ── Summary sheet ────────────────────────────────────────────────────
    es = wb.create_sheet("Summary", 0)

    es.row_dimensions[1].height = 16

    es.merge_cells("A3:E3")
    title_cell = es.cell(row=3, column=1, value=f"IT Ops SLA Breach Report — {TODAY}")
    title_cell.font = Font(name="Arial", bold=True, size=16, color=DARK_HEADER)
    title_cell.alignment = Alignment(horizontal="left", vertical="center")
    es.row_dimensions[3].height = 32

    stats = [
        ("Run Date",           _now.strftime("%Y-%m-%d %H:%M:%S"), CSA_NAVY),
        ("Open Tickets",       total,                               CSA_NAVY),
        ("SLA Breaches",       breached,                            "B71C1C"),
        ("  — Alerts",         alerts,                              "B71C1C"),
        ("  — Warnings",       warnings,                            "856404"),
        ("Within SLA",         total - breached,                    CSA_NAVY),
        ("Ryan Involved",      ryan_involved,                       CSA_BLUE),
        ("Action Required",    action_needed,                       "B71C1C"),
    ]
    rn = 5
    for label, val, color in stats:
        es.cell(row=rn, column=1, value=label).font = Font(name="Arial", bold=True, size=11, color=CSA_DARK_TEXT)
        v = es.cell(row=rn, column=2, value=val)
        v.font = Font(name="Arial", bold=True, size=13, color=color)
        v.alignment = Alignment(horizontal="left")
        rn += 1

    # SLA thresholds reference
    rn += 1
    es.merge_cells(start_row=rn, start_column=1, end_row=rn, end_column=3)
    sec = es.cell(row=rn, column=1, value="SLA Thresholds")
    sec.font = Font(name="Arial", bold=True, size=12, color=DARK_HEADER)
    rn += 1

    thresholds = [
        ("First Response",   f"{SLA_INITIAL_RESPONSE_HRS} business hours"),
        ("Requester Wait",   f"{SLA_REQUESTER_WAIT_HRS} business hours"),
        ("Stale Ticket",     f"{SLA_NO_UPDATE_HRS} business hours (1 day)"),
        ("Resolution Flag",  f"{SLA_RESOLUTION_DAYS} business days"),
    ]
    for label, val in thresholds:
        es.cell(row=rn, column=1, value=label).font = Font(name="Arial", size=10, color="666666")
        es.cell(row=rn, column=2, value=val).font = Font(name="Arial", size=10, color=CSA_DARK_TEXT)
        rn += 1

    # Action needed table
    action_rows = [r for r in sorted_rows if r["next_step"]]
    if action_rows:
        rn += 1
        es.merge_cells(start_row=rn, start_column=1, end_row=rn, end_column=4)
        sec = es.cell(row=rn, column=1, value=f"Action Required ({len(action_rows)} tickets)")
        sec.font = Font(name="Arial", bold=True, size=13, color="FFFFFF")
        sec.fill = PatternFill("solid", start_color=CSA_NAVY)
        sec.alignment = Alignment(horizontal="left", vertical="center")
        es.row_dimensions[rn].height = 24
        rn += 1

        act_headers = ["#", "Subject", "Next Step", "Ryan"]
        act_widths = [10, 44, 24, 18]
        for ci, (h, w) in enumerate(zip(act_headers, act_widths), 1):
            c = es.cell(row=rn, column=ci, value=h)
            c.font = Font(name="Arial", bold=True, size=10, color="666666")
            c.border = _border()
            es.column_dimensions[get_column_letter(ci)].width = w
        rn += 1

        for r in action_rows:
            tid_cell = es.cell(row=rn, column=1, value=r["ticket_id"])
            tid_cell.font = Font(name="Arial", color=LINK_COLOR, underline="single", size=11)
            tid_cell.hyperlink = r["ticket_url"]
            tid_cell.border = _border()

            es.cell(row=rn, column=2, value=r["subject"]).border = _border()

            step_cell = es.cell(row=rn, column=3, value=r["next_step"])
            ss = STEP_STYLE.get(r["next_step"], {"fc": CSA_DARK_TEXT})
            step_cell.font = Font(name="Arial", bold=True, color=ss["fc"], size=11)
            step_cell.border = _border()

            ryan_days = r["ryan_days_ago"]
            ryan_txt = f"{ryan_days}d ago" if ryan_days is not None and ryan_days > 0 else "Today" if ryan_days == 0 else "Never"
            es.cell(row=rn, column=4, value=ryan_txt).border = _border()

            es.row_dimensions[rn].height = 28
            rn += 1

    wb.save(output_path)
    print(f"  Report saved: {output_path} ({len(rows)} tickets)")


# ── Google Drive upload ─────────────────────────────────────────────────────
def upload_to_gdrive(file_path):
    if not GDRIVE_AVAILABLE:
        print("  [Drive] google-auth libraries not installed — skipping.")
        return
    if not GDRIVE_SA_JSON or not GDRIVE_FOLDER_ID:
        print("  [Drive] GDRIVE_SERVICE_ACCOUNT_JSON or GDRIVE_FOLDER_ID not set — skipping.")
        return
    try:
        creds_info = json.loads(GDRIVE_SA_JSON.strip())
        if creds_info.get("type") == "service_account":
            creds = service_account.Credentials.from_service_account_info(
                creds_info, scopes=["https://www.googleapis.com/auth/drive"])
        else:
            from google.oauth2.credentials import Credentials
            creds = Credentials(
                token=creds_info.get("token"),
                refresh_token=creds_info["refresh_token"],
                token_uri=creds_info.get("token_uri", "https://oauth2.googleapis.com/token"),
                client_id=creds_info["client_id"],
                client_secret=creds_info["client_secret"],
            )
        service = build("drive", "v3", credentials=creds)
        name = os.path.basename(file_path)
        metadata = {"name": name, "parents": [GDRIVE_FOLDER_ID]}
        media = MediaFileUpload(file_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        f = service.files().create(body=metadata, media_body=media, fields="id,webViewLink", supportsAllDrives=True).execute()
        print(f"  [Drive] Uploaded: {f.get('webViewLink', f.get('id'))}")
    except Exception as e:
        print(f"  [Drive] Upload failed: {e}")


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"  IT OPS SLA BREACH REPORT — {TODAY}")
    print(f"{'='*60}\n")

    # 1. Fetch tickets
    print("[1/3] Fetching IT Ops tickets...")
    tickets = fetch_tickets()
    if not tickets:
        print("  No tickets found. Exiting.")
        return

    # 2. Analyze each ticket
    print(f"\n[2/3] Analyzing {len(tickets)} tickets...")
    rows = []
    for i, ticket in enumerate(tickets, 1):
        tid = ticket["id"]
        print(f"  [{i}/{len(tickets)}] #{tid} — {ticket.get('subject', '')[:50]}")

        comments = fetch_comments(tid)
        time.sleep(0.3)  # rate limit courtesy

        # SLA check
        sla = check_sla(ticket, comments)

        # Ryan tracking
        ryan = last_ryan_mention(comments)

        # Next step recommendation
        next_step = recommend_next_step(ryan, sla)

        # ESC/RARC classification
        tag = classify_esc_rarc(ticket, comments)

        # Claude automation prompt
        subject = ticket.get("subject", "")
        claude_prompt = build_claude_prompt(tid, subject, tag, next_step, sla["display"])

        # Days open (business hours / 8)
        created_at = _parse_dt(ticket.get("created_at", ""))
        days_open = 0
        if created_at:
            days_open = _biz_hours_between(created_at, datetime.now(timezone.utc)) / 8

        rows.append({
            "ticket_id":     tid,
            "ticket_url":    f"{TICKET_URL}{tid}",
            "subject":       subject,
            "ticket_status": ticket.get("status", ""),
            "days_open":     days_open,
            "sla_breached":  sla["breached"],
            "sla_severity":  sla["severity"],
            "sla_display":   sla["display"],
            "ryan_found":    ryan["found"],
            "ryan_date":     ryan["date"],
            "ryan_days_ago": ryan["days_ago"],
            "next_step":     next_step,
            "tag":           tag,
            "claude_prompt": claude_prompt,
        })

    # 3. Build report
    print(f"\n[3/3] Building report...")
    write_report(rows, REPORT_PATH)

    # Upload to Google Drive
    upload_to_gdrive(REPORT_PATH)

    # Print summary
    breached = sum(1 for r in rows if r["sla_breached"])
    action = sum(1 for r in rows if r["next_step"])
    print(f"\n{'='*60}")
    print(f"  DONE — {len(rows)} tickets | {breached} SLA breaches | {action} need action")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
