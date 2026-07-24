"""
Phase 13 Power BI & Interactive Web Dashboard Server.

Launches a light local HTTP server hosting powerbi/dashboard.html at http://localhost:8080
"""

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
PORT = 8080


def start_dashboard_server():
    pbi_dir = (PROJECT_ROOT / "powerbi").resolve()
    os.chdir(pbi_dir)

    Handler = http.server.SimpleHTTPRequestHandler
    logger.info(f"=== Starting Real-Time Interactive Web Dashboard Server ===")
    logger.info(f"Dashboard URL: http://localhost:{PORT}/dashboard.html")

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        logger.info(f"Server serving at port {PORT}. Opening browser...")
        webbrowser.open(f"http://localhost:{PORT}/dashboard.html")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Dashboard server stopped.")


if __name__ == "__main__":
    import os
    start_dashboard_server()
