import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

spec = importlib.util.spec_from_file_location("t3_login", Path(__file__).with_name("t3-login.py"))
login = importlib.util.module_from_spec(spec)
spec.loader.exec_module(login)


class LoginTest(unittest.TestCase):
    def test_url_boundary(self):
        for url in ("http://example.com/mcp", "https://user:password@example.com/mcp", "file:///tmp/token"):
            with self.assertRaises(ValueError):
                login.trusted_url(url)
        login.trusted_url("http://127.0.0.1:3773/mcp")
        login.trusted_url("https://t3.example.com/mcp")

    def test_oauth_pkce_callback_and_private_credentials(self):
        observed = {}

        class Provider(BaseHTTPRequestHandler):
            def respond(self, value):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(value).encode())

            def do_GET(self):
                self.respond({"issuer": origin, "registration_endpoint": origin + "/register",
                              "authorization_endpoint": origin + "/authorize", "token_endpoint": origin + "/token"})

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                if self.path == "/register":
                    observed["registration"] = json.loads(body)
                    self.respond({"client_id": "client"})
                else:
                    observed["exchange"] = urllib.parse.parse_qs(body.decode())
                    self.respond({"access_token": "private-token", "expires_in": 3600})

            def log_message(self, *_args):
                pass

        with HTTPServer(("127.0.0.1", 0), Provider) as server, tempfile.TemporaryDirectory() as directory:
            origin = f"http://127.0.0.1:{server.server_port}"
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            callbacks = []

            def browser(url):
                params = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
                observed["authorize"] = params

                def callback():
                    redirect = params["redirect_uri"][0]
                    try:
                        with urllib.request.urlopen(redirect + "?state=wrong&iss=" + origin + "&code=code", timeout=2) as response:
                            observed["invalid_callback_status"] = response.status
                    except urllib.error.HTTPError as error:
                        observed["invalid_callback_status"] = error.code
                        error.close()
                    query = urllib.parse.urlencode({"state": params["state"][0], "iss": origin, "code": "code"})
                    with urllib.request.urlopen(redirect + "?" + query, timeout=2) as response:
                        observed["callback_status"] = response.status

                thread = threading.Thread(target=callback)
                callbacks.append(thread)
                thread.start()

            output = Path(directory) / "private" / "oauth.json"
            stdout = io.StringIO()
            try:
                with patch.object(login.webbrowser, "open", browser), contextlib.redirect_stdout(stdout):
                    login.login(origin + "/mcp", output)
                for callback in callbacks:
                    callback.join(timeout=3)
                self.assertEqual(observed["invalid_callback_status"], 400)
                self.assertEqual(observed["callback_status"], 200)
                self.assertTrue(all(not callback.is_alive() for callback in callbacks))
                token = json.loads(output.read_text())
                self.assertEqual(token["access_token"], "private-token")
                self.assertEqual(output.stat().st_mode & 0o777, 0o600)
                self.assertEqual(output.parent.stat().st_mode & 0o777, 0o700)
                self.assertNotIn("private-token", stdout.getvalue())
                verifier = observed["exchange"]["code_verifier"][0]
                challenge = login.base64.urlsafe_b64encode(login.hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
                self.assertEqual(observed["authorize"]["code_challenge"], [challenge])
                self.assertEqual(observed["exchange"]["resource"], [origin + "/mcp"])
            finally:
                server.shutdown()
                worker.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
