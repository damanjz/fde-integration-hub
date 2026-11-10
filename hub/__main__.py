"""`python -m hub`: run the API (with embedded workers) on port 8100.

Async psycopg needs the selector event loop, which uvicorn does not pick on Windows, so the server is
started on an explicit loop factory instead of `uvicorn hub.main:app`.
"""

from __future__ import annotations

import argparse
import asyncio

import uvicorn

from hub.config import load_settings
from hub.logging_ import configure_logging
from hub.main import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Larkfield integration hub")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    args = parser.parse_args()
    configure_logging()
    settings = load_settings()
    config = uvicorn.Config(
        create_app(settings), host=args.host, port=args.port, log_config=None, access_log=False
    )
    server = uvicorn.Server(config)
    with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
        runner.run(server.serve())


if __name__ == "__main__":
    main()
