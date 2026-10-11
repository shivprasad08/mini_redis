"""
M24 demo: writes keep succeeding while the primary fails over.

    docker compose up --build -d
    python scripts/smart_client_demo.py            # in one terminal
    docker compose stop node-a                     # in another, while it runs

Every second the demo writes demo:<n> through the smart client and prints
which node accepted it. After node-a stops, a few writes pause while the
cluster elects a new primary, then continue on node-c.
"""

from __future__ import annotations

import argparse
import asyncio
import time

from miniredis.smart_client import SmartClient, SmartClientError


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", default="127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382")
    parser.add_argument("--count", type=int, default=30)
    args = parser.parse_args()

    client = SmartClient(args.nodes.split(","), max_attempts=30, backoff=0.25)
    for n in range(args.count):
        started = time.monotonic()
        try:
            await client.set(f"demo:{n}", str(n))
            check = await client.get(f"demo:{n}")
            print(f"write demo:{n} -> {client.primary}  read back {check!r}  ({time.monotonic() - started:.2f}s)")
        except SmartClientError as exc:
            print(f"write demo:{n} FAILED: {exc}")
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
