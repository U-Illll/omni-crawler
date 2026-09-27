#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""mock429_server — 红队工具：前 3 个请求返回 429，之后返回 200"""
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class H(BaseHTTPRequestHandler):
    count = 0

    def do_GET(self):  # noqa: N802
        H.count += 1
        if H.count <= 3:
            self.send_response(429)
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(b"rate limited")
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK-FINAL")

    def log_message(self, *a):  # noqa: A003
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8799
    print(f"mock429 on 127.0.0.1:{port}", flush=True)
    HTTPServer(("127.0.0.1", port), H).serve_forever()
