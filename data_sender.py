#!/usr/bin/env python3
"""data_sender_flexible.py

Publish each element of a JSON array to an MQTT topic.

This is a drop‑in replacement for the original *data_sender.py* that let you
specify **any** input file at runtime (plus a few other niceties):

```
python data_sender_flexible.py my/topic -f path/to/file.json \
    --broker mqtt.example.com --port 1883 --delay 0.1
```

Arguments
---------
* **topic** (positional) – MQTT topic to publish to.
* **-f / --file** – path to JSON file (default: demo_data/air-quality-hourly.json).
* **--broker** – MQTT broker hostname/IP (default: localhost).
* **--port** – broker port (default: 1883).
* **--delay** – seconds to wait between messages (default: 0.2).

The script exits with a non‑zero status code if it can’t read the file, if the
file doesn’t contain a top‑level list, or if it can’t connect to the broker.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_BROKER_ADDRESS = "localhost"
DEFAULT_BROKER_PORT = 1883
DEFAULT_DATA_PATH = "demo_data/air-quality-hourly.json"
DEFAULT_DELAY = 0.2  # seconds

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:  # noqa: D401 – *returns* Namespace
    """Return parsed command‑line arguments."""
    parser = argparse.ArgumentParser(
        description="Publish each element from a JSON array to an MQTT topic.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "topic",
        help="MQTT topic that each JSON object will be published to",
    )
    parser.add_argument(
        "-f",
        "--file",
        default=DEFAULT_DATA_PATH,
        metavar="PATH",
        help="Path to JSON file containing a *list* of objects",
    )
    parser.add_argument(
        "--broker",
        default=DEFAULT_BROKER_ADDRESS,
        metavar="HOST",
        help="MQTT broker hostname or IP address",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_BROKER_PORT,
        metavar="N",
        help="MQTT broker port",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        metavar="SECONDS",
        help="Delay between messages",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def main() -> None:  # noqa: D401 – imperative mood
    args = parse_args()

    # 1. Connect to broker ---------------------------------------------------
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    try:
        client.connect(args.broker, args.port, keepalive=60)
        print(f"✓ Connected to MQTT broker at {args.broker}:{args.port}")
    except Exception as exc:  # pragma: no cover – connection failure
        print(f"✗ Could not connect to broker: {exc}", file=sys.stderr)
        sys.exit(1)

    # 2. Load JSON data ------------------------------------------------------
    try:
        with Path(args.file).expanduser().open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:  # pragma: no cover – I/O or JSON error
        print(f"✗ Failed to read JSON file {args.file!r}: {exc}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(payload, list):
        print("✗ JSON root must be a *list* of objects", file=sys.stderr)
        sys.exit(2)

    # 3. Publish -------------------------------------------------------------
    for item in payload:
        client.publish(args.topic, json.dumps(item))
        print(f"→ Published to {args.topic}: {item}")
        time.sleep(args.delay)

    client.disconnect()
    print("✓ Finished – connection closed")


if __name__ == "__main__":
    main()
