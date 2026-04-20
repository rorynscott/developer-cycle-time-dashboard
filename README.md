# Developer Cycle Time Dashboard

A self-hosted dashboard that tracks PR lifecycle metrics, review burden, AI co-authoring trends, and end-to-end task cycle time by joining GitHub PR data with Jira issue data.

**Stack**: Python 3.11+ (ETLs are pure stdlib), Streamlit + Plotly (dashboard), SQLite (storage).

## Quick Start

```bash
git clone https://github.com/rorynscott/developer-cycle-time-dashboard.git
cd developer-cycle-time-dashboard

# 1. Install dashboard dependencies
pip install -r requirements.txt

# 2. Configure
cp config.example.toml config.toml
# Edit config.toml with your teams, repos, and Jira details (see below)

# 3. Set up auth tokens (see Authentication section)

# 4. Run the ETLs
python3 github_etl.py              # Fetch GitHub PR data
python3 jira_etl.py                # Fetch Jira issue data
python3 jira_etl.py --backfill     # Link Jira issues to PRs

# 5. Start the dashboard
streamlit run dashboard.py --server.headless true
```

## Authentication

Both ETLs need API tokens. You can provide them via **environment variables** (recommended) or **file paths** in `config.toml`.

### GitHub Token

1. Go to [Personal Access Tokens (Fine-grained)](https://github.com/settings/personal-access-tokens)
2. Click **Generate new token**, select the repos you need, and grant **Pull requests** → Read-only permission (plus **Contents** → Read-only if your repos are private)
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
2. Click **Create API token**, give it a label (e.g., "cycle-time-dashboard")
3. Copy the token

**Option A — Environment variable** (recommended):
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

## Configuration

All configuration lives in `config.toml`. See `config.example.toml` for the full reference with inline comments.

### Teams

Define your teams with GitHub usernames. The ETLs use these to filter PRs and attribute reviews.

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
story_points = "customfield_10028"    # Your instance's story points field ID
sprint = "customfield_10020"          # Your instance's sprint field ID
# Add any extra fields — they'll be stored in a JSON column:
# aha_url = "customfield_11729"
```

To find your custom field IDs, use the Jira REST API:
```bash
curl -u you@example.com:YOUR_TOKEN \
  "https://your-org.atlassian.net/rest/api/3/field" | python3 -m json.tool | grep -A2 "Story Points"
```

### Jira Status Categories

The cycle time calculation needs to know which Jira statuses mean "in progress" and "done":

```toml
[jira.status_categories]
in_progress = ["In Progress", "In Development", "In Review"]
done = ["Done", "Closed", "Resolved"]
```

## ETL Usage

Both ETLs support incremental loading via watermarks — after the first run, they only fetch data updated since the last run.

```bash
# GitHub ETL
python3 github_etl.py                          # Incremental (since last run)
python3 github_etl.py --since 2025-01-01       # From a specific date
python3 github_etl.py --dry-run --verbose      # Preview without writing

# Jira ETL
python3 jira_etl.py                            # Incremental (since last run)
python3 jira_etl.py --backfill                 # Fetch Jira issues linked to PRs
python3 jira_etl.py --backfill-changelog       # Fetch status change history
python3 jira_etl.py --dry-run --verbose        # Preview without writing
```

### Recommended first-run order

```bash
python3 github_etl.py --since 2025-01-01       # 1. Load PR history
python3 jira_etl.py --since 2025-01-01         # 2. Load Jira issue history
python3 jira_etl.py --backfill                 # 3. Fill in Jira issues linked from PRs
python3 jira_etl.py --backfill-changelog       # 4. Get status transitions for cycle time
```

## Scheduling with Cron

Cron wrappers are included in `scripts/`. They handle logging and log rotation.

```bash
# Edit your crontab
crontab -e

# Add these lines (adjust paths and times):
0 8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_github_etl.sh
5 8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_jira_etl.sh
```

Both ETLs can run concurrently — they use SQLite WAL mode with a 60-second busy timeout and exponential backoff retry on lock contention.

Logs are written to `data/logs/` and can be viewed in the dashboard's **ETL Logs** page.

## Dashboard Features

- **PRs Created** — daily count by team, with trend line
- **AI Co-authoring** — tracks Claude, Copilot, ChatGPT, Gemini, Cursor via Co-authored-by trailers
- **Reviews Submitted** — daily review count by team
- **Review Burden by Origin** — shows what proportion of your reviews are for external (non-team) PRs
- **Who Are We Reviewing?** — top external PR authors your teams review
- **PR Size** — files changed and lines changed (avg + P90) for authored and reviewed PRs
- **Task Cycle Time** — three-phase breakdown: In Progress → first PR → merged → Done (requires Jira data)
- **Individual Breakdown** — per-person stats (toggle in sidebar)
- **ETL Logs** — view run history and errors

## Architecture

```
GitHub REST API ──→ github_etl.py ──→ ┐
                                       ├──→ SQLite DB ──→ dashboard.py (Streamlit)
Jira REST API  ──→ jira_etl.py   ──→ ┘
```

The ETLs are pure Python stdlib (no pip dependencies) so they run anywhere Python 3.11+ is installed. The dashboard adds Streamlit, Pandas, Plotly, and NumPy.

Data flows through a shared SQLite database with a star schema:
- `fact_pr` — one row per PR
- `dim_review` — one row per review action
- `dim_jira_issue` — one row per Jira issue
- `bridge_pr_jira` — links PRs to Jira issues via ticket keys found in PR titles/branches
- `dim_author` — team membership
- `dim_jira_status_change` — Jira status transition audit trail

## License

MIT
