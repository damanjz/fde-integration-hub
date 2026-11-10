"""`python -m mocks`: run the mock providers on port 9000. Reads MOCK_* from .env (environment wins)."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import uvicorn

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def load_dotenv() -> None:
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            if key.startswith("MOCK_"):
                os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    parser = argparse.ArgumentParser(description="Larkfield mock providers (simulated)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    load_dotenv()  # before the app is imported: credentials and faults are read at import time
    from mocks.app import app

    logging.basicConfig(level=logging.WARNING)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
