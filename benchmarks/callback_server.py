#!/usr/bin/env python3
"""Lightweight HTTP callback receiver for benchmark jobs.

Listens on port 9099 (or $CALLBACK_PORT).
- POST /progress, /complete, /error  — stores body in thread-safe state
- GET  /status                       — returns current state as JSON
- Auto-shuts down on /complete or /error
"""

import json
import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

_state_lock = threading.Lock()
_state: dict = {"status": "waiting"}
_server_ref: HTTPServer | None = None


class CallbackHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            payload = {"raw": body.decode(errors="replace")}

        path = self.path.rstrip("/")
        if path == "/progress":
            with _state_lock:
                _state["status"] = "running"
                _state["progress"] = payload
            self._respond(200, {"ok": True})
        elif path == "/complete":
            with _state_lock:
                _state["status"] = "complete"
                _state["result"] = payload
            self._respond(200, {"ok": True})
            threading.Thread(target=self._shutdown, daemon=True).start()
        elif path == "/error":
            with _state_lock:
                _state["status"] = "error"
                _state["error"] = payload
            self._respond(200, {"ok": True})
            threading.Thread(target=self._shutdown, daemon=True).start()
        else:
            self._respond(404, {"error": "not found"})

    def do_GET(self):
        if self.path.rstrip("/") == "/status":
            with _state_lock:
                snapshot = dict(_state)
            self._respond(200, snapshot)
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, code: int, body: dict):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    @staticmethod
    def _shutdown():
        global _server_ref
        if _server_ref:
            _server_ref.shutdown()

    def log_message(self, fmt, *args):
        # Silence default request logging
        pass


def main():
    global _server_ref
    port = int(os.environ.get("CALLBACK_PORT", "9099"))
    server = HTTPServer(("127.0.0.1", port), CallbackHandler)
    _server_ref = server
    print(f"Callback server listening on :{port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
