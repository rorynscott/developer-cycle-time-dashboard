# Developer Cycle Time Dashboard

A self-hosted dashboard built from GitHub data for one engineering team: PR output and AI co-authoring, review volume and burden, time through approval gates, who approves the team's PRs, PR size, and task delivery tracked as GitHub Issues rolled up to their epics.

**Stack**: Python 3.11+ (ETLs are pure stdlib), Streamlit + Plotly (dashboard), SQLite (storage). The UI uses the [Magnetic](https://github.com/CiscoDesignSystems/magnetic-common-design-system) light-theme design tokens (colors, type, radius); see `.streamlit/config.toml` and `lib/charts.py`.

## Quick Start

```bash
git clone https://github.com/rorynscott/developer-cycle-time-dashboard.git
cd developer-cycle-time-dashboard

# 1. Install dashboard dependencies
pip install -r requirements.txt

# 2. Configure
cp config.example.toml config.toml
# Edit config.toml: org, team roster, optional approval gates

# 3. Set up the GitHub token (see below)

# 4. Run the ETLs
python3 github_etl.py           # PRs + reviews (new repos backfill from history_start)
python3 github_issues_etl.py    # GitHub Issues tasks + their epics

# 5. Start the dashboard
streamlit run dashboard.py --server.headless true
```

## GitHub Token

1. Go to [Personal Access Tokens (Fine-grained)](https://github.com/settings/personal-access-tokens)
2. Click **Generate new token**, pick the resource owner (your org), and **select the repos to track** — the ETLs track every non-archived repo in `[github] org` that the token can read
3. Repository permissions: **Pull requests** and **Issues** → Read-only (plus **Contents** → Read-only if your repos are private)
4. Organization permissions: **Members** → Read-only (only needed for `[[github.approval_gates]]`)

Provide it via `GITHUB_TOKEN`, or a file:

```bash
echo "github_pat_..." > ~/.github_pat
chmod 600 ~/.github_pat
```

```toml
[github]
token_path = "~/.github_pat"
```

Fine-grained tokens expire. When one does, every ETL run fails until it's replaced.

The token can't read org Projects v2, and doesn't need to: in-progress times come from the status-change events on each issue's timeline.

## Configuration

All configuration lives in `config.toml`; `config.example.toml` is the commented reference.

### Repos

```toml
[github]
org = "my-org"                      # track every repo the token can read...
exclude_repos = ["my-org/docs"]     # ...except these
# repos = ["my-org/a", "my-org/b"]  # or list them explicitly
history_start = 2025-01-01          # backfill start for a newly seen repo
```

`python3 github_etl.py --list-repos` prints the resolved list.

### Team

```toml
[[teams]]
name = "Backend"
members = ["alice", "bob", "carol"]
former_members = [{ login = "dave", left_on = "2026-10-01" }]
```

Former members' work stays on the team if it finished before `left_on`. The ETLs re-attribute stored history on every run, so roster edits apply retroactively.

### Approval gates (optional)

A gate is an org team whose approval a repo requires — typically the CODEOWNERS `*` owner with "Require review from Code Owners" turned on:

```toml
[[github.approval_gates]]
repo = "my-org/api-server"
team = "api-server-approvers"
effective = 2026-10-22     # PRs created from this date are measured against it
```

Each PR ETL run snapshots the team's members into `dim_gate_member`. GitHub keeps no membership history, so who counts as a gate approver is based on the snapshots taken since the gate was configured.

### GitHub Issues as task tracker (optional)

Issues are searched in the tracked repos and credited to the first assignee who's on the team. The in-progress timestamp is the earliest Projects v2 status change matching one of these names, since each board has its own vocabulary:

```toml
[github.issues]
in_progress_statuses = ["In Progress", "In Development", "In Review"]
exclude_types = ["Epic"]     # container types; issues with sub-issues are containers too
```

## ETL Usage

```bash
# PRs: each repo syncs from its own watermark
python3 github_etl.py                            # incremental
python3 github_etl.py --since 2026-01-01         # re-sync every repo from a date
python3 github_etl.py --repos my-org/a --since 2026-01-01  # one repo
python3 github_etl.py --list-repos               # show tracked repos, then exit
python3 github_etl.py --dry-run --since 2026-10-01 --verbose

# Issues: full re-sync of team-assigned issues and their parents each run
python3 github_issues_etl.py
python3 github_issues_etl.py --dry-run
```

A repo that fails mid-run keeps its old watermark and is retried next run; the ETL exits non-zero so the cron wrapper logs the failure.

## Scheduling with Cron

```cron
0  8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_github_etl.sh
10 8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_github_issues_etl.sh
```

Set `PYTHON=/path/to/python` in the cron line to pin an interpreter. The ETLs share the SQLite DB (WAL mode, 60 s busy timeout, retry on lock). Logs go to `data/logs/` and show on the dashboard's **ETL Logs** page.

## Dashboard

The sidebar sets the date range, the repos, and **Break out by** (None / Engineer / Repo), which applies to every tab. A repo breakout stacks by repo with fixed colors. An engineer breakout adds ranked bars per engineer instead: the roster is bigger than the 7-color chart palette.

- **Output**: PRs created and merged; AI co-authoring (Claude, Copilot, ChatGPT, Gemini, Cursor via `Co-authored-by` trailers)
- **Reviews**: reviews submitted per week; review burden by origin (our PRs vs other teams'); which repos we review in
- **PR Flow**: median/P90 hours for open → first approval → gate approval → merge; gated PRs merged without a gate approval; who approves our PRs (teammate / gate / other) and who clears each gate
- **PR Size**: weekly lines and files changed, authored and reviewed
- **Delivery**: tasks completed per week, created → closed and in progress → closed, linked-PR coverage, and the epics team tasks roll up to (progress counts every sub-issue)
- **ETL Logs**: run history and errors for both pipelines

## Architecture

```
GitHub REST API  ──→ github_etl.py         ──→ ┐
GitHub GraphQL   ──→ github_issues_etl.py  ──→ ┴──→ SQLite DB ──→ dashboard.py (Streamlit)
```

- `lib/github.py`: API helpers and repo discovery; `lib/config.py`: config accessors
- `lib/data.py`: cached dashboard queries; `lib/charts.py`: Magnetic chart styling and builders
- `views/`: one module per tab

Tables:
- `fact_pr`, `dim_review`, `dim_author`, `dim_repo` (per-repo watermark), `dim_gate_member`: PRs
- `dim_gh_issue`, `dim_gh_issue_status_change`, `bridge_pr_gh_issue`, `dim_gh_epic`: Issues
- `etl_watermark*`: last-run bookmarks

## Deployment

### Production (Cisco internal)

A copy of this dashboard runs at **https://things.cisco.com/app/duo-directory-cycle-time/**. It's hosted on a Foundry VM and proxied through the Things platform (`things.cisco.com`), which handles SSO and tile gating.

The systemd unit driving it lives in `deploy/dashboard.service`. Two flags in the `ExecStart` are mandatory when running behind the Things proxy:

```
--server.enableCORS false --server.enableXsrfProtection false
```

Without them Streamlit rejects the websocket upgrade because the browser's `Origin` header (`things.cisco.com`) doesn't match the host (Foundry). The dashboard appears to load but never connects — you'll see a permanent loading spinner. These flags do **not** weaken security in this deployment because Cisco SSO + Things tile membership already gate access; do not copy them into a public-internet deployment.

`deploy/ship.sh` copies modified and new files to the VM; it does not delete files removed locally.

### Local development

For local runs use the simpler command in **Quick Start** above — those flags would weaken security with no benefit when there's no proxy in front of you.

## License

MIT
