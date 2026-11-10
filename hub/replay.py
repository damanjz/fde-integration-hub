"""`python -m hub.replay --destination warehouse --since 2026-11-10T00:00:00Z`: bulk replay of dead rows.

Replayed rows are marked needs_reconcile, so the worker asks the destination whether it already has the
delivery before sending (warehouse lookup, accounting duplicate 409, helpdesk Idempotency-Key). The
helpdesk only remembers a key for 24 hours: dead rows first sent longer ago than that are refused unless
--force-past-idempotency-window is given, because a repeat could create a second record.

`python -m hub.replay --skip 42 --reason "order was a test"` abandons one dead row so the rows blocked
behind it go out. Both actions are audited with --actor.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

from hub import outbox
from hub.config import load_settings
from hub.db import open_pool
from hub.destinations import destination_settings


def parse_when(value: str) -> datetime:
    when = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return when if when.tzinfo else when.replace(tzinfo=UTC)


async def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    pool = await open_pool(settings.database_url, 4)
    ttls = {
        name: cfg.idempotency_ttl_s
        for name, cfg in destination_settings(settings).items()
        if cfg.idempotency_ttl_s is not None
    }
    try:
        if args.skip is not None:
            if not args.reason:
                print("--skip needs --reason")
                return 2
            ok = await outbox.skip(pool, args.skip, actor=args.actor, reason=args.reason)
            print(
                f"skipped delivery {args.skip}" if ok else f"delivery {args.skip} is not dead: nothing done"
            )
            return 0 if ok else 1
        if args.dry_run:
            rows = await outbox.list_deliveries(
                pool, status="dead", destination=args.destination, since=None, limit=100000, offset=0
            )
            rows = [
                r
                for r in rows
                if (not args.since or (r["dead_at"] and r["dead_at"] >= args.since))
                and (not args.until or (r["dead_at"] and r["dead_at"] < args.until))
            ]
            print(f"would replay {len(rows)} dead deliveries")
            for r in rows[:20]:
                print(f"  id={r['id']} {r['destination']} {r['event_type']} reason={r['dead_reason']}")
            return 0
        result = await outbox.replay(
            pool,
            destination=args.destination,
            since=args.since,
            until=args.until,
            limit=args.limit,
            actor=args.actor,
            reason=args.reason,
            force=args.force_past_idempotency_window,
            idempotency_ttl_s=ttls,
        )
        print(f"replayed {len(result.replayed)} dead deliveries")
        for delivery_id, why in result.refused:
            print(f"REFUSED id={delivery_id}: {why}")
        if result.refused:
            print("re-run with --force-past-idempotency-window to replay the refused rows anyway")
        return 1 if result.refused else 0
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay or skip dead-lettered deliveries.")
    parser.add_argument("--destination", help="warehouse | helpdesk | accounting (default: all)")
    parser.add_argument(
        "--since", type=parse_when, help="only deliveries that died at or after this ISO time"
    )
    parser.add_argument("--until", type=parse_when, help="only deliveries that died before this ISO time")
    parser.add_argument("--limit", type=int, help="replay at most this many")
    parser.add_argument("--dry-run", action="store_true", help="list what would be replayed")
    parser.add_argument("--actor", default="cli", help="who is doing this (written to the audit log)")
    parser.add_argument("--reason", help="why (written to the audit log; required for --skip)")
    parser.add_argument(
        "--skip", type=int, metavar="ID", help="skip one dead delivery and release its blocked rows"
    )
    parser.add_argument(
        "--force-past-idempotency-window",
        action="store_true",
        help="replay helpdesk rows first sent more than 24 h ago (may create a second record)",
    )
    args = parser.parse_args()
    with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
        raise SystemExit(runner.run(run(args)))


if __name__ == "__main__":
    main()
