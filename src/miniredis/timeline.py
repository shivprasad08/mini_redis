"""Parse and merge Lamport event logs from multiple mini-redis nodes."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from miniredis.logical_clock import LamportEvent

EVENT_RE = re.compile(
    r"lamport_event ts=(?P<ts>\d+) node=(?P<node>\S+) event=(?P<event>\S+) "
    r"message=(?P<message>\S+) description=(?P<description>\S+)"
)


def parse_lamport_events(text: str) -> list[LamportEvent]:
    events: list[LamportEvent] = []
    for line in text.splitlines():
        match = EVENT_RE.search(line)
        if match is None:
            continue
        events.append(
            LamportEvent(
                timestamp=int(match.group("ts")),
                node_id=match.group("node"),
                event_type=match.group("event"),
                message_id=match.group("message"),
                description=match.group("description").replace("_", " "),
            )
        )
    return events


def merge_events(events: list[LamportEvent]) -> list[LamportEvent]:
    return sorted(events, key=lambda event: (event.timestamp, event.node_id, event.event_type, event.message_id))


def render_timeline(events: list[LamportEvent]) -> str:
    lines = ["timestamp  node      event                 message              description"]
    for event in merge_events(events):
        lines.append(
            f"{event.timestamp:<10} {event.node_id:<9} {event.event_type:<21} "
            f"{event.message_id:<20} {event.description}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="merge mini-redis Lamport logs")
    parser.add_argument("logs", nargs="+", help="node log files")
    args = parser.parse_args()
    events: list[LamportEvent] = []
    for path in args.logs:
        events.extend(parse_lamport_events(Path(path).read_text(encoding="utf-8")))
    print(render_timeline(events))


if __name__ == "__main__":
    main()
