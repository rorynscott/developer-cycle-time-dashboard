"""
Configuration loader for Developer Cycle Time Dashboard.

Reads config.toml (TOML format) and provides typed accessors for all
configurable values. Secrets can be overridden via environment variables.
"""

import os
import sys

try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # pip install tomli (Python <3.11)
    except ModuleNotFoundError:
        print(
            "ERROR: Python 3.11+ required (for tomllib), "
            "or install 'tomli': pip install tomli",
            file=sys.stderr,
        )
        sys.exit(1)

# Resolved project root (directory containing this lib/ package)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_config = None


def load_config(path=None):
    """Load and cache the TOML config file.

    Resolution order for config path:
    1. Explicit ``path`` argument
    2. ``CONFIG_PATH`` environment variable
    3. ``config.toml`` in the project root
    """
    global _config
    if _config is not None and path is None:
        return _config

    if path is None:
        path = os.environ.get("CONFIG_PATH")
    if path is None:
        path = os.path.join(PROJECT_ROOT, "config.toml")

    if not os.path.exists(path):
        print(
            f"ERROR: Config file not found at {path}\n"
            "  Copy the example and edit it:\n"
            "    cp config.example.toml config.toml",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(path, "rb") as f:
        _config = tomllib.load(f)

    _validate(_config)
    return _config


def _validate(cfg):
    """Check that required sections and keys exist."""
    errors = []

    gh = cfg.get("github", {})
    if not gh.get("org") and not gh.get("repos"):
        errors.append("[github] needs org (repos discovered from the token) "
                      "or an explicit repos list")
    for i, gate in enumerate(gh.get("approval_gates", [])):
        for key in ("repo", "team", "effective"):
            if not gate.get(key):
                errors.append(f"[[github.approval_gates]] entry {i}: "
                              f"{key} is required")

    if "teams" not in cfg or not cfg["teams"]:
        errors.append("At least one [[teams]] block is required")
    else:
        for i, team in enumerate(cfg["teams"]):
            if "name" not in team:
                errors.append(f"[[teams]] entry {i}: name is required")
            if "members" not in team or not team["members"]:
                errors.append(f"[[teams]] entry {i} ({team.get('name', '?')}): members is required")
            for fm in team.get("former_members", []):
                if not fm.get("login") or not fm.get("left_on"):
                    errors.append(
                        f"[[teams]] entry {i} ({team.get('name', '?')}): "
                        "former_members entries need login and left_on"
                    )

    if errors:
        print("Config validation errors:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)


# ── Teams ──────────────────────────────────────────────────────────────────


def get_teams():
    """Return list of team dicts with defaults filled in.

    Each dict has: name, short_name, members, former_members.
    former_members is a list of {login, left_on} where left_on is an ISO
    date (YYYY-MM-DD); work finished before that date stays on the team.
    """
    cfg = load_config()
    return [
        {
            "name": t["name"],
            "short_name": t.get("short_name") or t["name"].split()[0],
            "members": list(t["members"]),
            "former_members": [
                {"login": fm["login"], "left_on": str(fm["left_on"])}
                for fm in t.get("former_members", [])
            ],
        }
        for t in cfg["teams"]
    ]


def get_team_members():
    """Return flat set of all GitHub usernames across all teams.

    Includes former members so ETLs keep their pre-departure history
    current; use get_team_resolver() to decide attribution.
    """
    return set(get_team_lookup())


def get_current_members():
    """Return current (not former) members, in config order."""
    return [m for t in get_teams() for m in t["members"]]


def get_team_lookup():
    """Return {github_username: team_name}, including former members."""
    lookup = {}
    for t in get_teams():
        for m in t["members"]:
            lookup[m] = t["name"]
        for fm in t["former_members"]:
            lookup[fm["login"]] = t["name"]
    return lookup


def get_former_members():
    """Return {github_username: left_on 'YYYY-MM-DD'} for members who left."""
    return {
        fm["login"]: fm["left_on"]
        for t in get_teams() for fm in t["former_members"]
    }


def get_team_resolver():
    """Return team_at(login, when) -> team name or None.

    ``when`` is when the work finished (ISO string or datetime; None means
    still open, i.e. now). Former members only count before their left_on.
    """
    lookup = get_team_lookup()
    left_on = get_former_members()

    def team_at(login, when=None):
        team = lookup.get(login)
        cutoff = left_on.get(login)
        if team and cutoff and (when is None or str(when)[:10] >= cutoff):
            return None
        return team

    return team_at


# ── GitHub ─────────────────────────────────────────────────────────────────


def _read_token_file(path):
    """Read a token from a file path, expanding ~ and stripping whitespace."""
    expanded = os.path.expanduser(path)
    try:
        with open(expanded) as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"ERROR: Token file not found: {expanded}", file=sys.stderr)
        sys.exit(1)


def get_github_token():
    """Return GitHub token. Checks GITHUB_TOKEN env var, then config token_path."""
    env = os.environ.get("GITHUB_TOKEN")
    if env:
        return env.strip()
    cfg = load_config()
    path = cfg.get("github", {}).get("token_path")
    if path:
        return _read_token_file(path)
    print(
        "ERROR: No GitHub token. Set GITHUB_TOKEN env var or "
        "github.token_path in config.toml",
        file=sys.stderr,
    )
    sys.exit(1)


def get_github_repo_config():
    """Return how to resolve the tracked repo list.

    Shape: {org, repos, exclude_repos}. When ``repos`` is set it is used
    as-is; otherwise every non-archived repo in ``org`` that the token can
    read is tracked, minus ``exclude_repos``.
    """
    gh = load_config()["github"]
    return {
        "org": gh.get("org"),
        "repos": list(gh.get("repos", [])),
        "exclude_repos": set(gh.get("exclude_repos", [])),
    }


def get_history_start():
    """Return the YYYY-MM-DD date a newly tracked repo is backfilled from."""
    return str(load_config()["github"].get("history_start", "2025-01-01"))


def get_approval_gates():
    """Return required-approval gates: [{repo, team, effective}].

    ``team`` is an org team slug whose approval a PR in ``repo`` needs
    (e.g. the CODEOWNERS catch-all owner); ``effective`` is the YYYY-MM-DD
    the requirement started. PRs created before then aren't measured
    against the gate.
    """
    return [
        {"repo": g["repo"], "team": g["team"], "effective": str(g["effective"])}
        for g in load_config()["github"].get("approval_gates", [])
    ]


def get_github_issues_config():
    """Return GitHub Issues task-tracking config, or None if disabled.

    Shape: {in_progress_statuses: [...], exclude_types: [...]}. Issues are
    searched in the same repos as PRs.
    """
    issues = load_config().get("github", {}).get("issues")
    if issues is None or issues.get("enabled") is False:
        return None
    return {
        "in_progress_statuses": list(issues.get(
            "in_progress_statuses", ["In Progress", "In Development"],
        )),
        "exclude_types": list(issues.get("exclude_types", [])),
    }


# ── Database ───────────────────────────────────────────────────────────────


def get_db_path():
    """Return resolved database path."""
    cfg = load_config()
    db_path = cfg.get("database", {}).get("path", "data/cycle_time.db")
    if not os.path.isabs(db_path):
        db_path = os.path.join(PROJECT_ROOT, db_path)
    return db_path


def get_log_dir():
    """Return log directory path (sibling of database)."""
    db_dir = os.path.dirname(get_db_path())
    return os.path.join(db_dir, "logs")


# ── Dashboard ──────────────────────────────────────────────────────────────


def get_dashboard_title():
    """Return dashboard title."""
    return load_config().get("dashboard", {}).get(
        "title", "Developer Cycle Time Dashboard"
    )


def get_dashboard_default_days():
    """Return default date range in days."""
    return load_config().get("dashboard", {}).get("default_days", 30)
