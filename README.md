# Zendesk Open Ticket Reports

Automated reporting for Cloud Security Alliance IT-Operations Zendesk tickets. Generates styled Excel spreadsheets that track SLA compliance, escalation status, and recommended actions — then uploads them to Google Drive.

## Scripts

### `sla_breach_report.py` — SLA Breach Report

Shows all open IT-Ops tickets with SLA compliance status, Ryan Bergsma tracking, and automation-ready Claude prompts.

**Output columns:** Ticket #, Subject, SLA (breach/warning/ok + details), Ryan (last tagged date + days ago), Next Step, Status, Claude Prompt.

**What it does:**

- Fetches all open/pending/hold tickets from the three IT-Ops Zendesk groups via the incremental cursor export API
- Evaluates each ticket against four SLA thresholds (first response, requester wait, stale ticket, resolution age) using business-hours-only calculation (Mon–Fri 9–5 Pacific)
- Tracks when Ryan Bergsma was last @mentioned in ticket comments and how many days ago
- Recommends a next step: "Slack Ryan ticket URL", "Tag Ryan in ticket", or "Allow time to respond"
- Classifies tickets as ESC (escalation) or RARC (ready to close) using regex pattern matching against comments
- Generates per-ticket Claude prompts that instruct Claude to apply ESC/RARC tags and add internal notes via the Zendesk API
- Produces a CSA-branded Excel report with color-coded severity rows and a Summary sheet with an action-required table

### `zendeskreport_opentickets_esc-rarc_bidaily.py` — ESC/RARC Tag Report (legacy)

The original report script. Classifies tickets as ESC or RARC, assesses urgency using Claude (Anthropic API), generates draft replies, and outputs a detailed operational report. This script requires an Anthropic API key for full functionality.

## SLA Thresholds

All thresholds use business hours only (Mon–Fri, 09:00–17:00 US/Pacific):

| Metric | Threshold | Severity |
|---|---|---|
| First response | 2 business hours | Alert (no response) or Warning (late response) |
| Requester unanswered | 4 business hours | Alert |
| Stale ticket | 8 business hours (1 day) | Warning |
| Resolution age | 2 business days | Informational flag |

## Setup

### Prerequisites

Python 3.10+ and pip.

### Install

```bash
git clone https://github.com/cvee-csa/zendeskopenticketreport.git
cd zendeskopenticketreport
pip install -r requirements.txt
```

### Required Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `ZENDESK_EMAIL` | Yes | Zendesk login email |
| `ZENDESK_TOKEN` | Yes | Zendesk API token (Admin > Apps & Integrations > API) |
| `GDRIVE_SERVICE_ACCOUNT_JSON` | No | Google service account JSON for Drive upload |
| `GDRIVE_FOLDER_ID` | No | Target Google Drive folder ID |
| `ANTHROPIC_API_KEY` | No | Enables Claude draft replies in the legacy ESC/RARC script |
| `DRY_RUN` | No | Set to `"true"` to prevent posting replies to Zendesk (default: true) |

### Run

```bash
# SLA Breach Report
export ZENDESK_EMAIL="you@cloudsecurityalliance.org"
export ZENDESK_TOKEN="your-token"
python sla_breach_report.py

# Legacy ESC/RARC Report
python zendeskreport_opentickets_esc-rarc_bidaily.py
```

Reports are written to `/tmp/` and optionally uploaded to Google Drive.

## GitHub Actions

The workflow in `.github/workflows/zendeskreport_esc-rarc.yml` runs the legacy ESC/RARC report. It is triggered via `workflow_dispatch` — either manually from the Actions tab or externally via cron-job.org hitting the GitHub API at 8:00 AM and 4:00 PM Pacific, Mon–Fri.

All credentials are stored as repository secrets under Settings > Secrets and variables > Actions.

## ESC/RARC Classification

**ESC (Escalation)** — ticket is blocked on an external actor. Matched when comments mention: blocked on, waiting on Ryan/Kurt/Dev/Leadership, needs approval, escalation, business/leadership decision, or Ryan/Kurt by name. On-hold tickets default to ESC.

**RARC (Ready to Close)** — IT Ops has resolved the issue and is waiting for requester confirmation. Matched when the last IT Ops comment contains: please confirm, let me know if, can we close, please verify, good to go/close, and no requester reply has been received since.

## Contact

Catherine Vee — cvee@cloudsecurityalliance.org
