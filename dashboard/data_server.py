"""
data_server.py - Serve live_data.json on port 8502 with CORS.
Started by run_all.py before the dashboard and simulation.
"""
import http.server
import socketserver
import os
import sys

PORT       = 8502
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')


class _CORSHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC_DIR, **kwargs)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store, no-cache')
        super().end_headers()

    def log_message(self, *args):
        pass  # suppress per-request logs


class _ReuseServer(socketserver.TCPServer):
    allow_reuse_address = True


if __name__ == '__main__':
    print(f"[DATA SERVER] port {PORT} -> {STATIC_DIR}", flush=True)
    with _ReuseServer(('', PORT), _CORSHandler) as httpd:
        httpd.serve_forever()
