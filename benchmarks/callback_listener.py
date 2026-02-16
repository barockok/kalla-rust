#!/usr/bin/env python3
"""Lightweight HTTP callback listener for benchmarks.

Starts a simple HTTP server that accepts POST requests on:
  /progress  — logs progress (ignored for completion detection)
  /complete  — writes result JSON to output file and exits
  /error     — writes error JSON to output file and exits

Usage:
    python benchmarks/callback_listener.py --port 0 --output /tmp/result.json
    # Prints the actual bound port to stdout, e.g.: CALLBACK_PORT=9876

The benchmark orchestrator starts this in the background, reads the port,
and passes http://localhost:<port> as the callback_url in the job payload.
"""

import argparse
import json
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler


class CallbackHandler(BaseHTTPRequestHandler):
    """Handle worker callback POSTs."""

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"raw": body.decode("utf-8", errors="replace")}

        path = self.path.rstrip("/")

        if path == "/complete":
            payload["_callback_type"] = "complete"
            self._write_result(payload)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            # Schedule shutdown after response is sent
            threading.Thread(target=self.server.shutdown, daemon=True).start()

        elif path == "/error":
            payload["_callback_type"] = "error"
            self._write_result(payload)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            threading.Thread(target=self.server.shutdown, daemon=True).start()

        elif path == "/progress":
            # Log but don't exit
            run_id = payload.get("run_id", "?")
            stage = payload.get("stage", "?")
            progress = payload.get("progress", "?")
            print(f"  progress: run={run_id} stage={stage} progress={progress}",
                  file=sys.stderr, flush=True)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')

        else:
            self.send_response(404)
            self.end_headers()

    def _write_result(self, payload):
        output_file = self.server.output_file
        with open(output_file, "w") as f:
            json.dump(payload, f, indent=2)

    def log_message(self, format, *args):
        # Suppress default access logs
        pass


def main():
    parser = argparse.ArgumentParser(description="Callback listener for benchmarks")
    parser.add_argument("--port", type=int, default=0,
                        help="Port to listen on (0 = random available port)")
    parser.add_argument("--output", required=True,
                        help="Path to write the result JSON file")
    args = parser.parse_args()

    server = HTTPServer(("127.0.0.1", args.port), CallbackHandler)
    server.output_file = args.output

    actual_port = server.server_address[1]
    # Print port so parent process can read it
    print(f"CALLBACK_PORT={actual_port}", flush=True)

    server.serve_forever()


if __name__ == "__main__":
    main()
