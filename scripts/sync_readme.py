"""Copy the result line, the Numbers table and "Reading these numbers" from reports/chaos.md into README.md,
verbatim.

Run after every chaos run: python scripts/sync_readme.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
START, END = "<!-- CHAOS-START -->", "<!-- CHAOS-END -->"


def main() -> int:
    report = (ROOT / "reports" / "chaos.md").read_text(encoding="utf-8")
    result = next(line for line in report.splitlines() if line.startswith("Result:"))
    numbers = re.search(r"## Numbers\n\n(.*?)\n\n## ", report, re.S).group(1)
    reading = re.search(r"## Reading these numbers\n\n(.*?)\n\n## ", report, re.S).group(1)
    block = (
        f"{START}\n{result}\n\n{numbers}\n\n**Reading these numbers**\n\n{reading}\n\n"
        "Per-destination figures, the settings table, injected-fault counts and every check are in "
        "[reports/chaos.md](reports/chaos.md). Counted at the mock providers, not in the hub's tables.\n"
        f"{END}"
    )
    readme_path = ROOT / "README.md"
    readme = readme_path.read_text(encoding="utf-8")
    new = re.sub(re.escape(START) + r".*?" + re.escape(END), lambda _: block, readme, flags=re.S)
    readme_path.write_text(new, encoding="utf-8")
    print("README.md chaos section updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
