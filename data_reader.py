#!/usr/bin/env python3
"""data_reader.py

Subscribe to an MQTT topic and print each incoming message.

Configuration order of precedence (highest → lowest):
1. Command‑line options `--broker` / `--port`
2. Variables `MQTT_BROKER` and `MQTT_PORT` in a `.env` file that lies in
   the *same directory* as this script (loaded via `python‑dotenv`).
3. Environment variables already set in the container/host shell.
4. Hard‑coded defaults: `localhost` / `1883`.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import paho.mqtt.client as mqtt

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:  # pragma: no cover – optional dep
    load_dotenv = None  # type: ignore

# ---------------------------------------------------------------------------
# Load .env (if present) -----------------------------------------------------
# ---------------------------------------------------------------------------
if load_dotenv is not None:
    env_file = Path(__file__).with_name(".env")
    if env_file.exists():
        load_dotenv(env_file)

# ---------------------------------------------------------------------------
# Argument parsing ----------------------------------------------------------
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:  # noqa: D401
    parser = argparse.ArgumentParser(
        description="Subscribe to an MQTT topic and print JSON payloads.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "topic",
        metavar="TOPIC",
        help="MQTT topic to subscribe to",
    )
    parser.add_argument(
        "--broker",
        default=os.getenv("MQTT_BROKER", "localhost"),
        help="MQTT broker host",
        metavar="HOST",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MQTT_PORT", 1883)),
        help="MQTT broker port",
        metavar="N",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# MQTT callbacks ------------------------------------------------------------
# ---------------------------------------------------------------------------

def on_message(_: mqtt.Client, __, msg: mqtt.MQTTMessage) -> None:  # noqa: D401
    """Print the topic and payload of each received message."""
    try:
        payload = msg.payload.decode()
    except UnicodeDecodeError:
        payload = str(msg.payload)
    print(f"\nMessage on '{msg.topic}':\n{payload}\n")


# ---------------------------------------------------------------------------
# Main ----------------------------------------------------------------------
# ---------------------------------------------------------------------------

def main() -> None:  # noqa: D401 – imperative
    args = parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message

    try:
        client.connect(args.broker, args.port, keepalive=60)
        print(f"✓ Connected to {args.broker}:{args.port}")
    except Exception as exc:
        print(f"✗ Could not connect: {exc}", file=sys.stderr)
        sys.exit(1)

    client.subscribe(args.topic)
    print(f"Listening on topic '{args.topic}'. Press Ctrl+C to quit.")

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nDisconnecting…")
        client.disconnect()
        print("Disconnected.")


if __name__ == "__main__":
    main()
