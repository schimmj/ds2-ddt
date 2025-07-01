#!/usr/bin/env python3
"""data_sender.py

Publish each element of a JSON array to an MQTT topic.

Now supports reading the broker host and port from a `.env` file that
lives in the *same directory* as this script.  Expected variables::

    MQTT_BROKER=broker.example.com
    MQTT_PORT=1883

Command‑line options still take precedence; if neither the CLI nor the
.env file provides a value we fall back to the hard‑coded defaults
(localhost / 1883).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import paho.mqtt.client as mqtt

try:
    # Optional dependency; fall back gracefully if not installed
    from dotenv import load_dotenv  # type: ignore
except ImportError:  # pragma: no cover
    load_dotenv = None  # type: ignore

# ---------------------------------------------------------------------------
# Environment defaults
# ---------------------------------------------------------------------------

# Load .env from the script's directory (if python‑dotenv is available)
if load_dotenv is not None:
    env_file = Path(__file__).with_name(".env")
    if env_file.exists():
        load_dotenv(env_file, override=True)

DEFAULT_BROKER_ADDRESS = os.getenv("BROKER", "localhost")
try:
    DEFAULT_BROKER_PORT = int(os.getenv("PORT", 1883))
except ValueError:  # pragma: no cover – invalid int in env
    DEFAULT_BROKER_PORT = 1883

DEFAULT_DATA_PATH = "demo_data/air-quality-hourly.json"
DEFAULT_DELAY = 1  # seconds

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
    print(DEFAULT_BROKER_ADDRESS, DEFAULT_BROKER_PORT)
    args = parse_args()

    # 1. Connect to broker ---------------------------------------------------
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    try:
        client.connect(args.broker, args.port, keepalive=60)
        print(f"✓ Connected to MQTT broker at {args.broker}:{args.port}")
    except Exception as exc:  # pragma: no cover – connection failure
        print(f"✗ Could not connect to broker({args.broker}): {exc}", file=sys.stderr)
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
