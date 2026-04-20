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

# Auto-assigned chart colors when teams don't specify one
_DEFAULT_PALETTE = [
    "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
    "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52",
]

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

    if "github" not in cfg or "repos" not in cfg.get("github", {}):
        errors.append("[github] repos is required")

    if "jira" not in cfg:
        errors.append("[jira] section is required")
    else:
        for key in ("base_url", "projects"):
            if key not in cfg["jira"]:
                errors.append(f"[jira] {key} is required")
        # email can come from env var
        if "email" not in cfg["jira"] and not os.environ.get("JIRA_EMAIL"):
            errors.append("[jira] email is required (or set JIRA_EMAIL env var)")

    if "teams" not in cfg or not cfg["teams"]:
        errors.append("At least one [[teams]] block is required")
    else:
        for i, team in enumerate(cfg["teams"]):
            if "name" not in team:
                errors.append(f"[[teams]] entry {i}: name is required")
            if "members" not in team or not team["members"]:
                errors.append(f"[[teams]] entry {i} ({team.get('name', '?')}): members is required")

    if errors:
        print("Config validation errors:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        sys.exit(1)


# ── Accessors ──────────────────────────────────────────────────────────────


def get_teams():
    """Return list of team dicts with defaults filled in.

    Each dict has: name, short_name, color, members.
    """
    cfg = load_config()
    teams = []
    for i, t in enumerate(cfg["teams"]):
        teams.append({
            "name": t["name"],
            "short_name": t.get("short_name") or t["name"].split()[0],
            "color": t.get("color") or _DEFAULT_PALETTE[i % len(_DEFAULT_PALETTE)],
            "members": list(t["members"]),
        })
    return teams


def get_team_members():
    """Return flat set of all GitHub usernames across all teams."""
    members = set()
    for t in get_teams():
        members.update(t["members"])
    return members


def get_team_lookup():
    """Return {github_username: team_name} mapping."""
    lookup = {}
    for t in get_teams():
        for m in t["members"]:
            lookup[m] = t["name"]
    return lookup


def get_team_colors():
    """Return {team_name: hex_color} mapping."""
    return {t["name"]: t["color"] for t in get_teams()}


def get_team_names():
    """Return list of team names in config order."""
    return [t["name"] for t in get_teams()]


# ── Secrets ────────────────────────────────────────────────────────────────


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


def get_jira_token():
    """Return Jira API token. Checks JIRA_TOKEN env var, then config token_path."""
    env = os.environ.get("JIRA_TOKEN")
    if env:
        return env.strip()
    cfg = load_config()
    path = cfg.get("jira", {}).get("token_path")
    if path:
        return _read_token_file(path)
    print(
        "ERROR: No Jira token. Set JIRA_TOKEN env var or "
        "jira.token_path in config.toml",
        file=sys.stderr,
    )
    sys.exit(1)


def get_jira_email():
    """Return Jira email. Checks JIRA_EMAIL env var, then config."""
    env = os.environ.get("JIRA_EMAIL")
    if env:
        return env.strip()
    cfg = load_config()
    email = cfg.get("jira", {}).get("email")
    if email:
        return email
    print(
        "ERROR: No Jira email. Set JIRA_EMAIL env var or "
        "jira.email in config.toml",
        file=sys.stderr,
    )
    sys.exit(1)


def get_jira_base_url():
    """Return Jira base URL from config."""
    return load_config()["jira"]["base_url"].rstrip("/")


def get_jira_projects():
    """Return list of Jira project keys."""
    return list(load_config()["jira"]["projects"])


def get_jira_custom_fields():
    """Return custom fields dict from config. Keys are logical names, values are field IDs."""
    return dict(load_config().get("jira", {}).get("custom_fields", {}))


def get_jira_status_categories():
    """Return status category mapping: {in_progress: [...], done: [...]}."""
    defaults = {
        "in_progress": ["In Progress", "In Development"],
        "done": ["Done", "Closed", "Resolved"],
    }
    cats = load_config().get("jira", {}).get("status_categories", {})
    return {
        "in_progress": list(cats.get("in_progress", defaults["in_progress"])),
        "done": list(cats.get("done", defaults["done"])),
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
