"""Local dev server for the frontend.

Serves frontend/ on http://localhost:3000 and rewrites index.html to inject
`window.API_URL` from frontend/.env.development. The browser then sends data
fetches to the local Docker API at http://localhost:8000 instead of the
production Fly.io URL. CORS in the FastAPI app already allows
http://localhost:3000, so direct cross-origin fetches work — no proxy.

Production (GitHub Pages) is untouched: there's no env injection, so
`window.API_URL` is undefined and `index.html`'s fallback resolves to the
public Fly URL.

Stdlib only — no third-party deps.

Usage:
    python frontend/serve.py
    python frontend/serve.py --port 3001
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
import socketserver
import sys
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / ".env.development"
DEFAULT_PORT = 3000


def parse_env(path: Path) -> dict[str, str]:
    """Tiny KEY=VALUE parser. Skips blanks and `# ...` comments."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        # Strip optional surrounding quotes; values are not interpolated.
        v = value.strip()
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        out[key.strip()] = v
    return out


def make_injection(env: dict[str, str]) -> bytes:
    """Build the <script> block that primes window.* before the page's inline
    script runs."""
    api_url = env.get("API_URL", "")
    payload = json.dumps({"API_URL": api_url})  # safe JSON-encoded values
    snippet = (
        "<script>"
        "(function () { var c = " + payload + "; "
        "for (var k in c) { if (Object.prototype.hasOwnProperty.call(c, k) && c[k]) "
        "window[k] = c[k]; } })();"
        "</script>"
    )
    return snippet.encode("utf-8")


class DevHandler(http.server.SimpleHTTPRequestHandler):
    """Serve frontend/ as the document root and rewrite index.html on the fly."""

    env_snippet: bytes = b""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def _is_index(self) -> bool:
        # Match `/` and `/index.html` (with or without trailing query).
        path = self.path.split("?", 1)[0]
        return path == "/" or path.endswith("/index.html")

    def do_GET(self) -> None:  # noqa: N802 — http.server convention
        if not self._is_index():
            return super().do_GET()

        index_path = ROOT / "index.html"
        if not index_path.exists():
            self.send_error(404, "index.html not found")
            return

        body = index_path.read_bytes()
        # Inject right before the first <script ...> tag — that puts our
        # window.API_URL assignment ahead of MapLibre and the inline app code.
        marker = b"<script"
        idx = body.find(marker)
        if idx == -1:
            # No script tag in the file; serve untouched rather than guess.
            patched = body
        else:
            patched = body[:idx] + self.env_snippet + body[idx:]

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(patched)))
        # Tell browsers (and intermediaries) not to cache the HTML in dev —
        # the injected window.API_URL needs to be re-read on every reload.
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(patched)

    def log_message(self, fmt: str, *args) -> None:  # noqa: N802
        # Quieter log line: timestamp + method + path + status.
        sys.stderr.write("[dev] " + (fmt % args) + "\n")


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def main() -> int:
    parser = argparse.ArgumentParser(description="Local frontend dev server.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Don't auto-open the browser on startup.",
    )
    args = parser.parse_args()

    env = parse_env(ENV_FILE)
    api_url = env.get("API_URL", "(unset — index.html will use prod fallback)")
    DevHandler.env_snippet = make_injection(env)

    addr = ("0.0.0.0", args.port)
    url = f"http://localhost:{args.port}"
    print(f"Serving frontend/ at {url}  ->  API_URL = {api_url}")
    if not env:
        print(f"  (no .env.development at {ENV_FILE} — nothing to inject)")

    if not args.no_open and not os.getenv("CI"):
        webbrowser.open(url)

    with ReusableTCPServer(addr, DevHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nshutting down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
