"""
Phase 13 Power BI & Interactive Web Dashboard Server.

Launches a light local HTTP server hosting powerbi/dashboard.html
With automatic port fallback (8080 -> 8085 -> 8000 -> 8088).
"""

import os
import sys
import http.server
import socketserver
import webbrowser
from pathlib import Path

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.logger import get_logger

logger = get_logger("DashboardServer")
PORTS_TO_TRY = [8080, 8085, 8000, 8088]


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def start_dashboard_server():
    pbi_dir = (PROJECT_ROOT / "powerbi").resolve()
    os.chdir(pbi_dir)

    Handler = http.server.SimpleHTTPRequestHandler
    logger.info("=== Starting Real-Time Interactive Web Dashboard Server ===")

    for port in PORTS_TO_TRY:
        try:
            with ReusableTCPServer(("", port), Handler) as httpd:
                url = f"http://localhost:{port}/dashboard.html"
                logger.info(f"Dashboard Server successfully listening at port {port}.")
                logger.info(f"Dashboard URL: {url}")
                webbrowser.open(url)
                try:
                    httpd.serve_forever()
                except KeyboardInterrupt:
                    logger.info("Dashboard server stopped.")
                return
        except OSError as e:
            logger.warning(f"Port {port} is occupied or unavailable. Trying next port...")
            continue

    logger.error("All candidate ports (8080, 8085, 8000, 8088) are occupied. Could not start server.")


if __name__ == "__main__":
    start_dashboard_server()
