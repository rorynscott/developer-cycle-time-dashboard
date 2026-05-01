# Developer Effectiveness Dashboard

A self-hosted dashboard that combines signals from GitHub, Jira, Webex, VictorOps (Splunk On-Call), and FireHydrant to give engineering teams a full-picture view of developer effectiveness: PR lifecycle, review burden, AI co-authoring, end-to-end task cycle time, support load, on-call burden, and incident response performance.

**Stack**: Python 3.11+ (ETLs are pure stdlib), Streamlit + Plotly (dashboard), SQLite (storage).

## Quick Start

```bash
git clone https://github.com/rorynscott/developer-cycle-time-dashboard.git
cd developer-cycle-time-dashboard

# 1. Install dashboard dependencies
pip install -r requirements.txt

# 2. Configure
cp config.example.toml config.toml
# Edit config.toml with your teams, repos, Jira details, etc.

# 3. Set up auth tokens (see Authentication section)

# 4. Run the ETLs (GitHub + Jira are the minimum; the rest are optional)
python3 github_etl.py                          # PR data
python3 jira_etl.py                            # Jira issue data
python3 jira_etl.py --backfill                 # Link Jira issues to PRs
python3 webex_etl.py --since 2026-01-01        # Webex room messages (optional)
python3 victorops_etl.py --since 2026-01-01    # VictorOps incidents (optional)
python3 firehydrant_etl.py --since 2026-01-01  # FireHydrant incidents (optional)

# 5. Start the dashboard
streamlit run dashboard.py --server.headless true
```

## Authentication

The dashboard supports five data sources. Only GitHub and Jira are required — the rest are optional. Tokens can be provided via environment variables (recommended) or file paths in `config.toml`.

### GitHub Token

1. Go to [Personal Access Tokens (Fine-grained)](https://github.com/settings/personal-access-tokens)
2. Click **Generate new token**, select the repos you need, and grant **Pull requests** → Read-only (plus **Contents** → Read-only if your repos are private)
3. Copy the token

**Option A — Environment variable** (recommended):
```bash
export GITHUB_TOKEN="ghp_your_token_here"
```

**Option B — File**:
```bash
echo "ghp_your_token_here" > ~/.github_pat
chmod 600 ~/.github_pat
```
Then set in `config.toml`:
```toml
[github]
token_path = "~/.github_pat"
```

### Jira API Token

1. Go to [id.atlassian.com/manage-profile/security/api-tokens](https://id.atlassian.com/manage-profile/security/api-tokens)
2. Click **Create API token**, give it a label
3. Copy the token

**Option A — Environment variable**:
```bash
export JIRA_TOKEN="your_jira_api_token"
export JIRA_EMAIL="you@example.com"
```

**Option B — File**:
```bash
echo "your_jira_api_token" > ~/.atlassian_token
chmod 600 ~/.atlassian_token
```
Then set in `config.toml`:
```toml
[jira]
email = "you@example.com"
token_path = "~/.atlassian_token"
```

### Webex Integration (optional)

Webex is ingested via an **OAuth Integration** acting as you, not a bot — bots can't read group-room history. Setup is a one-time browser flow.

1. Go to [developer.webex.com/my-apps](https://developer.webex.com/my-apps), click **Create a New App** → **Create an Integration**
2. Fill in name/description, set **Redirect URI** to `http://localhost:8080/callback`, and grant the `spark:all` scope
3. Save the **Client ID**, **Client Secret**, and **Redirect URI**
4. Write them to `~/.webex_integration`:
   ```
   CLIENT_ID=your_client_id
   CLIENT_SECRET=your_client_secret
   REDIRECT_URL=http://localhost:8080/callback
   ```
   ```bash
   chmod 600 ~/.webex_integration
   ```
5. Find the room ID you want to ingest. You can get it from the Webex API, or from the URL of the room in the Webex web app (it's base64-encoded).
6. Put the room ID in `config.toml` under `[webex]` and list each team's email addresses under `[[webex.team_emails]]`
7. Run the auth helper once to get a refresh token:
   ```bash
   python3 scripts/webex_auth.py
   ```
   This opens your browser for consent, captures the redirect, and writes the refresh token to `~/.webex_refresh_token` (mode 600). Refresh tokens are valid ~90 days and rotate automatically on each use.

### VictorOps / Splunk On-Call (optional)

1. Go to **Integrations** → **API** in the VictorOps/Splunk On-Call web app
2. Generate an **API ID** and **API Key**
3. Find your team slug — it's in the URL when you view the team (e.g., `team-jAyTi...`)
4. Write credentials to `~/.victorops` in `KEY:VALUE` format:
   ```
   API_ID:your_api_id
   API_KEY:your_api_key
   ```
   ```bash
   chmod 600 ~/.victorops
   ```
5. Put the team slug in `config.toml`:
   ```toml
   [victorops]
   credentials_path = "~/.victorops"
   team_slug = "team-xxxxxxxxxxxx"
   exclude_service_patterns = ["weekly page test", "test incident"]
   ```

### FireHydrant (optional)

1. In FireHydrant, go to **Settings** → **Bots** (or **Integrations**) and create a bot with read access to incidents
2. Copy the token and save to `~/.firehydrant_token`:
   ```bash
   echo "fhb_your_token_here" > ~/.firehydrant_token
   chmod 600 ~/.firehydrant_token
   ```
3. Find your team UUID — browse to `https://app.firehydrant.io/teams`, click your team, and copy the UUID from the URL
4. Configure:
   ```toml
   [firehydrant]
   token_path = "~/.firehydrant_token"
   team_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
   ```

## Configuration

All configuration lives in `config.toml`. See `config.example.toml` for the full reference with inline comments.

### Teams

```toml
[[teams]]
name = "Backend"
short_name = "BE"          # Optional: used in dashboard labels (defaults to first word)
color = "#636EFA"          # Optional: hex color for charts (auto-assigned if omitted)
members = ["alice", "bob", "carol"]

[[teams]]
name = "Frontend"
members = ["dave", "eve"]
```

### GitHub Repos

```toml
[github]
repos = ["my-org/api-server", "my-org/web-app"]
```

### Jira Projects & Custom Fields

```toml
[jira]
base_url = "https://your-org.atlassian.net"
email = "you@example.com"
projects = ["PROJ", "BACKEND", "FRONTEND"]

[jira.custom_fields]
story_points = "customfield_10028"
sprint = "customfield_10020"
# Add any extra fields — they'll be stored in a JSON column:
# aha_url = "customfield_11729"

[jira.status_categories]
in_progress = ["In Progress", "In Development", "In Review"]
done = ["Done", "Closed", "Resolved"]
```

To find your custom field IDs:
```bash
curl -u you@example.com:YOUR_TOKEN \
  "https://your-org.atlassian.net/rest/api/3/field" | python3 -m json.tool | grep -A2 "Story Points"
```

### Support Tickets (optional)

If you have a dedicated Jira project for support escalations, configure it under `[jira.support]`. The ETL will scope tickets to your teams (by a team custom field) and capture priority/severity/impact.

```toml
[jira.support]
project = "ZTCE"
team_field = "customfield_10001"
priority_field = "customfield_11649"
severity_field = "customfield_11619"
impact_field = "customfield_11651"
teams = ["ZT - Duo Directory", "ZT - Duo Directory Augmentation"]
```

### Categories

The dashboard applies regex-based categorization to ticket summaries and Webex messages at query time (updating patterns reclassifies everything immediately — no re-ETL needed). Define them once at the top of `config.toml`:

```toml
[[categories]]
name = "SCIM"
patterns = ["\\bscim\\b", "provisioning"]

[[categories]]
name = "Auth Proxy"
patterns = ["\\bauth[ _-]?proxy\\b"]
```

First match wins, so put specific patterns before generic ones.

## ETL Usage

All ETLs support incremental loading via watermarks — after the first run, they only fetch data updated since the last run.

```bash
# GitHub ETL
python3 github_etl.py                          # Incremental (since last run)
python3 github_etl.py --since 2026-01-01       # From a specific date
python3 github_etl.py --dry-run --verbose      # Preview without writing

# Jira ETL
python3 jira_etl.py                            # Incremental
python3 jira_etl.py --backfill                 # Fetch Jira issues linked to PRs
python3 jira_etl.py --backfill-changelog       # Fetch status change history
python3 jira_etl.py --dry-run --verbose        # Preview without writing

# Webex ETL
python3 webex_etl.py                           # Incremental
python3 webex_etl.py --since 2026-01-01        # Backfill from date
python3 webex_etl.py --dry-run --verbose       # Preview

# VictorOps ETL
python3 victorops_etl.py                       # Incremental
python3 victorops_etl.py --since 2026-01-01    # Backfill
python3 victorops_etl.py --dry-run --verbose   # Preview

# FireHydrant ETL
python3 firehydrant_etl.py                     # Incremental
python3 firehydrant_etl.py --since 2026-01-01  # Backfill
python3 firehydrant_etl.py --dry-run --verbose # Preview
```

### Recommended first-run order

```bash
python3 github_etl.py --since 2026-01-01       # 1. Load PR history
python3 jira_etl.py --since 2026-01-01         # 2. Load Jira issue history
python3 jira_etl.py --backfill                 # 3. Fill in Jira issues linked from PRs
python3 jira_etl.py --backfill-changelog       # 4. Get status transitions for cycle time
python3 webex_etl.py --since 2026-01-01        # 5. Optional: Webex messages
python3 victorops_etl.py --since 2026-01-01    # 6. Optional: VictorOps incidents
python3 firehydrant_etl.py --since 2026-01-01  # 7. Optional: FireHydrant incidents
```

## Scheduling with Cron

Cron wrappers are included in `scripts/` for all five ETLs. They handle logging and log rotation.

```bash
crontab -e
```

Example schedule (adjust paths as needed):

```cron
0  8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_github_etl.sh
5  8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_jira_etl.sh
10 8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_webex_etl.sh
15 8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_victorops_etl.sh
20 8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_firehydrant_etl.sh
```

All ETLs can run concurrently — they use SQLite WAL mode with a 60-second busy timeout and exponential backoff retry on lock contention. Webex, VictorOps, and FireHydrant write to tables that don't overlap with GitHub or Jira, so the 5-minute stagger above is just belt-and-suspenders.

Logs are written to `data/logs/` and can be viewed in the dashboard's **ETL Logs** page.

## Dashboard Features

- **PR Lifecycle** — PRs created, AI co-authoring breakdown (Claude, Copilot, ChatGPT, Gemini, Cursor via Co-authored-by trailers), PR size (files + lines, avg + P90)
- **Reviews** — reviews submitted, review burden by origin (internal vs external PRs), top external authors your teams review
- **Task Cycle Time** — three-phase breakdown: In Progress → first PR → merged → Done (requires Jira data)
- **Support Load** — support tickets (if `[jira.support]` configured), Webex asks in your configured room, FireHydrant S1–S4 incident volume
- **On-Call** — VictorOps incident counts, MTTA/MTTR, services most frequently paging, per-team breakdown
- **Incident Response** — FireHydrant incidents with time-to-detect / ack / mitigate / resolve, incident commander assignments
- **Individual Breakdown** — per-person stats (toggle in sidebar)
- **ETL Logs** — view run history and errors for all five pipelines

## Architecture

```
GitHub REST API   ──→ github_etl.py     ──→ ┐
Jira REST API     ──→ jira_etl.py       ──→ │
Webex REST API    ──→ webex_etl.py      ──→ ├──→ SQLite DB ──→ dashboard.py (Streamlit)
VictorOps REST    ──→ victorops_etl.py  ──→ │
FireHydrant REST  ──→ firehydrant_etl.py ──→ ┘
```

The ETLs are pure Python stdlib (no pip dependencies) so they run anywhere Python 3.11+ is installed. The dashboard adds Streamlit, Pandas, Plotly, and NumPy.

Data flows through a shared SQLite database:
- `fact_pr`, `dim_review`, `dim_author`, `dim_repo`, `bridge_pr_jira` — GitHub
- `dim_jira_issue`, `dim_jira_status_change` — Jira
- `fact_webex_message` — Webex
- `fact_vo_incident` — VictorOps
- `fact_fh_incident`, `dim_fh_role_assignment` — FireHydrant
- `etl_watermark*` — per-source incremental-load bookmarks

## License

MIT
