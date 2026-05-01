#!/usr/bin/env python3
"""
Webex OAuth authorization helper — run once.

Opens the Webex consent page in your browser, listens on the redirect URI,
captures the authorization code, exchanges it for an access + refresh token,
and writes the refresh token to the configured path (mode 600).

Usage:
    python3 scripts/webex_auth.py

After this completes, webex_etl.py will use the refresh token on every run
with no further browser interaction needed. Refresh tokens are valid ~90 days
and rotate automatically on each exchange.
"""

import json
import os
import sys
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.config import get_webex_integration, get_webex_config

WEBEX_AUTHORIZE_URL = "https://webexapis.com/v1/authorize"
WEBEX_TOKEN_URL = "https://webexapis.com/v1/access_token"
DEFAULT_SCOPES = "spark:all"


class _CallbackHandler(BaseHTTPRequestHandler):
    captured = {}

    def do_GET(self):  # noqa: N802 — required by BaseHTTPRequestHandler
        qs = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(qs)
        self._respond(
            "Webex auth complete. You can close this tab and return to the terminal."
            if "code" in params
            else f"No code received. Error: {params.get('error', ['unknown'])[0]}"
        )
        _CallbackHandler.captured = {k: v[0] for k, v in params.items()}

    def _respond(self, msg):
        body = f"<html><body><h3>{msg}</h3></body></html>".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args, **kwargs):  # silence noisy default logging
        pass


def _post_form(url, data):
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main():
    integ = get_webex_integration()
    cfg = get_webex_config() or {}

    redirect = integ["redirect_uri"]
    parsed = urllib.parse.urlparse(redirect)
    host, port = parsed.hostname or "localhost", parsed.port or 8080
    path = parsed.path or "/callback"

    scopes = os.environ.get("WEBEX_SCOPES", DEFAULT_SCOPES)
    auth_url = (
        f"{WEBEX_AUTHORIZE_URL}?"
        + urllib.parse.urlencode({
            "response_type": "code",
            "client_id": integ["client_id"],
            "redirect_uri": redirect,
            "scope": scopes,
            "state": "webex-etl-auth",
        })
    )

    print(f"\nStarting local callback server on {host}:{port}{path} ...")
    httpd = HTTPServer((host, port), _CallbackHandler)

    print("Opening browser to Webex consent screen...")
    print(f"If it doesn't open, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    # Serve one request (the redirect) then stop
    httpd.handle_request()
    httpd.server_close()

    if "code" not in _CallbackHandler.captured:
        print("ERROR: No authorization code captured.", file=sys.stderr)
        print(f"Response: {_CallbackHandler.captured}", file=sys.stderr)
        sys.exit(1)

    code = _CallbackHandler.captured["code"]
    print("Got authorization code. Exchanging for tokens...")

    try:
        tokens = _post_form(WEBEX_TOKEN_URL, {
            "grant_type": "authorization_code",
            "client_id": integ["client_id"],
            "client_secret": integ["client_secret"],
            "code": code,
            "redirect_uri": redirect,
        })
    except urllib.request.HTTPError as e:
        body = e.read().decode()
        print(f"ERROR: Token exchange failed ({e.code}): {body}", file=sys.stderr)
        sys.exit(1)

    if "refresh_token" not in tokens:
        print(f"ERROR: Response missing refresh_token: {tokens}", file=sys.stderr)
        sys.exit(1)

    out_path = os.path.expanduser(
        cfg.get("refresh_token_path") or "~/.webex_refresh_token"
    )
    with open(out_path, "w") as f:
        f.write(tokens["refresh_token"])
    os.chmod(out_path, 0o600)
    print(f"\nSUCCESS — refresh token written to {out_path}")
    print(f"Access token lifetime: {tokens.get('expires_in')}s")
    print(f"Refresh token lifetime: {tokens.get('refresh_token_expires_in')}s")
    print("\nRun the Webex ETL with:  python3 webex_etl.py --since 2026-04-01")


if __name__ == "__main__":
    main()
