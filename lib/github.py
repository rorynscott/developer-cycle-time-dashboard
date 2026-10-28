"""
Shared GitHub API helpers for the ETL pipelines: REST and GraphQL calls,
repo discovery, and team membership.
"""

import json
import time
import urllib.request
import urllib.error

from lib.db import log, is_error

API = "https://api.github.com"


def github_api(token, url, retries=3):
    """GET a REST URL. Returns parsed JSON, or {"error", "status"} on failure."""
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req) as resp:
                remaining = resp.headers.get("X-RateLimit-Remaining")
                if remaining and int(remaining) < 50:
                    reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))
                    wait = max(reset_ts - int(time.time()), 1)
                    log(f"  Rate limit low ({remaining}), sleeping {wait}s...")
                    time.sleep(wait)
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            # A 403 is only a rate limit when the quota is exhausted or it's
            # a secondary limit (Retry-After); a permission 403 won't get
            # better by waiting.
            retry_after = e.headers.get("Retry-After")
            limited = e.code == 429 or retry_after or (
                e.code == 403 and e.headers.get("X-RateLimit-Remaining") == "0"
            )
            if limited:
                reset_ts = int(e.headers.get("X-RateLimit-Reset", 0))
                wait = (int(retry_after) if retry_after
                        else max(reset_ts - int(time.time()), 5))
                log(f"  Rate limited ({e.code}), sleeping {wait}s... "
                    f"(attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue
            if attempt == retries - 1 or e.code in (401, 403, 404):
                return {"error": str(e), "status": e.code}
            time.sleep(2)
        except urllib.error.URLError as e:
            if attempt == retries - 1:
                return {"error": str(e), "status": None}
            time.sleep(5)
    return {"error": "Max retries exceeded", "status": None}


def paginate(token, url, max_pages=None):
    """GET every page of a list endpoint (per_page=100).

    Raises RuntimeError on an API error so callers don't mistake a failed
    fetch for an empty result.
    """
    sep = "&" if "?" in url else "?"
    items, page = [], 1
    while True:
        result = github_api(token, f"{url}{sep}per_page=100&page={page}")
        if is_error(result):
            raise RuntimeError(f"GET {url} page {page}: {result}")
        items.extend(result)
        if len(result) < 100 or (max_pages and page >= max_pages):
            return items
        page += 1


def graphql(token, query, variables, retries=3):
    body = json.dumps({"query": query, "variables": variables}).encode()
    for attempt in range(retries):
        req = urllib.request.Request(f"{API}/graphql", data=body, method="POST")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code in (403, 429, 502):
                wait = 30 * (attempt + 1)
                log(f"  GraphQL HTTP {e.code}, sleeping {wait}s... "
                    f"(attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue
            raise
        if result.get("errors"):
            raise RuntimeError(f"GraphQL errors: {result['errors']}")
        return result["data"]
    raise RuntimeError("GraphQL: max retries exceeded")


def resolve_repos(token, repo_cfg):
    """Return the sorted list of org/repo names to track.

    An explicit ``repos`` list wins. Otherwise: every non-archived repo
    owned by ``org`` that the token can read (for a fine-grained PAT, the
    repos it was granted), minus ``exclude_repos``.
    """
    if repo_cfg["repos"]:
        return sorted(repo_cfg["repos"])
    org = repo_cfg["org"].lower()
    repos = paginate(token, f"{API}/user/repos?affiliation=owner,collaborator,"
                            "organization_member")
    return sorted(
        r["full_name"] for r in repos
        if r["owner"]["login"].lower() == org
        and not r["archived"]
        and r["full_name"] not in repo_cfg["exclude_repos"]
    )


def fetch_team_members(token, org, team_slug):
    """Return the logins in an org team (child-team members included)."""
    members = paginate(token, f"{API}/orgs/{org}/teams/{team_slug}/members")
    return sorted(m["login"] for m in members)
