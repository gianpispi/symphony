#!/usr/bin/env python3
"""Sign Symphony in to T3's MCP audience using OAuth authorization code + PKCE."""
import argparse
import base64
import hashlib
import http.server
import json
import os
from pathlib import Path
import secrets
import tempfile
import time
import urllib.parse
import urllib.request
import webbrowser


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def trusted_url(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("URL must not contain credentials or a fragment")
    if not parsed.hostname or not (parsed.scheme == "https" or (
        parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "localhost", "::1")
    )):
        raise ValueError("Use HTTPS or loopback HTTP")
    return parsed


def request_json(url, payload=None, form=False):
    trusted_url(url)
    data = None if payload is None else (
        urllib.parse.urlencode(payload).encode() if form else json.dumps(payload).encode()
    )
    headers = {"Content-Type": "application/x-www-form-urlencoded" if form else "application/json"}
    request = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.build_opener(NoRedirect).open(request, timeout=15) as response:
        return json.load(response)


def save_credentials(path, credentials):
    path = Path(path).expanduser().resolve()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(credentials, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def login(endpoint, output, no_browser=False):
    parsed = trusted_url(endpoint)
    origin = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    discovery = request_json(origin + "/.well-known/oauth-authorization-server")
    for name in ("registration_endpoint", "authorization_endpoint", "token_endpoint", "issuer"):
        url = trusted_url(discovery[name])
        if (url.scheme, url.netloc) != (parsed.scheme, parsed.netloc):
            raise ValueError("OAuth discovery points outside the selected T3 origin")

    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    state = secrets.token_urlsafe(32)
    result = {}

    class Callback(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            url = urllib.parse.urlsplit(self.path)
            values = urllib.parse.parse_qs(url.query)
            valid = (url.path == "/callback" and values.get("state") == [state]
                     and values.get("iss") == [discovery["issuer"]] and bool(values.get("code")))
            self.send_response(200 if valid else 400)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"You can close this window." if valid else b"Invalid OAuth callback.")
            if valid:
                result["code"] = values["code"][0]

        def log_message(self, *_args):
            pass  # The callback URL contains a one-use authorization code.

    with http.server.HTTPServer(("127.0.0.1", 0), Callback) as server:
        server.timeout = 1
        redirect = f"http://127.0.0.1:{server.server_port}/callback"
        registration = request_json(discovery["registration_endpoint"], {
            "client_name": "Symphony", "redirect_uris": [redirect],
            "grant_types": ["authorization_code"], "token_endpoint_auth_method": "none",
        })
        params = {
            "response_type": "code", "client_id": registration["client_id"],
            "redirect_uri": redirect, "code_challenge": challenge,
            "code_challenge_method": "S256", "state": state, "resource": endpoint,
        }
        url = discovery["authorization_endpoint"] + "?" + urllib.parse.urlencode(params)
        print("Approve Symphony in T3 and select Supervised access.\n" + url, flush=True)
        if not no_browser:
            webbrowser.open(url)
        deadline = time.monotonic() + 300
        while not result and time.monotonic() < deadline:
            server.handle_request()
        if not result:
            raise TimeoutError("OAuth approval timed out")
        token = request_json(discovery["token_endpoint"], {
            "grant_type": "authorization_code", "client_id": registration["client_id"],
            "redirect_uri": redirect, "code": result["code"], "code_verifier": verifier,
            "resource": endpoint,
        }, form=True)
    if not isinstance(token.get("access_token"), str):
        raise ValueError("T3 did not return an access token")
    save_credentials(output, {"endpoint": endpoint, "access_token": token["access_token"],
                             "expires_at": time.time() + token["expires_in"]})
    print(f"Saved OAuth credentials to {output}. Keep this file private.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:3773/mcp")
    parser.add_argument("--output", default=".symphony-t3/oauth.json")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    try:
        login(args.endpoint, args.output, args.no_browser)
    except Exception as error:
        # Do not print HTTP response bodies, callback URLs or tokens on failure.
        parser.exit(1, f"T3 sign-in failed: {type(error).__name__}. Check the endpoint and retry.\n")
