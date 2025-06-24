# SPDX-FileCopyrightText: 2025 - 2025 Software GmbH, Darmstadt, Germany and/or its subsidiaries and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

# tests/test_result_mqtt_pipeline.py
from numpy import NaN
import pandas as pd
import pytest
import json
from mqtt import MqttPublisher
from result_handler import ResultPublisher
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------
class DummyClient:
    """
    Stand-in for mqtt_client.MqttClient.
    It only records publish calls so we can assert against them.
    """
    def __init__(self):
        self.calls = []        # list of (topic, payload) tuples

    def publish(self, topic: str, obj):
        self.calls.append((topic, obj))


@pytest.fixture
def pipeline():
    """
    Wire together ResultPublisher → MqttPublisher → DummyClient.

    Returns (result_publisher, dummy_client) so tests can push data in
    and inspect what eventually landed on the client.
    """
    dummy_client = DummyClient()
    mqtt_pub = MqttPublisher(dummy_client)            # layer 2
    topic_cfg = {"publish": {"validated": "sensor/validated"}}

    result_pub = ResultPublisher(                     # layer 1
        topic_cfg,
        mqtt_pub.publish,      # inject MqttPublisher.publish
        delay=0
    )
    return result_pub, dummy_client


# ---------------------------------------------------------------------------
# 1)  Single-row path: one publish, flattened keys
# ---------------------------------------------------------------------------
def test_mixed_types_payload_serialisable(pipeline):
    result_pub, dummy_client = pipeline

    # --- build one-row dataframes with 5 different dtypes -----------------


    raw = pd.DataFrame({
        "temperature": [22.5],         # float
        "count":       [5],            # int
        "status":      ["OK"],         # str
        "flag":        [True],         # bool
        "time":        [NaN],       # datetime64[ns, UTC]
    })
    cleaned = pd.DataFrame({
        "temperature": [23.0],
        "count":       [6],
        "status":      ["OK"],         # unchanged
        "flag":        [False],
        "time":        [NaN],
    })

    result_pub.emit(cleaned, raw)

    assert len(dummy_client.calls) == 1
    topic, payload = dummy_client.calls[0]

    # still correct topic
    assert topic == "sensor/validated"

    # <-- 1) JSON-serialisation must succeed --------------------------------
    json.dumps(payload)      # will raise TypeError if *anything* is not JSONable

    # <-- 2) spot-check the values -----------------------------------------
    expected = {
        "temperature.raw": 22.5,             "temperature.cleaned": 23.0,
        "count.raw":       5,                "count.cleaned":       6,
        "status.raw":      "OK",             "status.cleaned":      "OK",
        "flag.raw":        True,             "flag.cleaned":        False,
        "time.raw":        None,
        "time.cleaned":    None,
    }
    assert payload == expected


# ---------------------------------------------------------------------------
# 2)  Multi-row frame: one publish per row, payloads match
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("rows", [3])
def test_multi_row_emits_individually(pipeline, rows):
    result_pub, dummy_client = pipeline

    raw = pd.DataFrame({"temperature": [17 + i for i in range(rows)]})
    cleaned = raw + 0.5

    result_pub.emit(cleaned, raw)

    # Expect exactly <rows> calls, in order, fully flattened
    assert len(dummy_client.calls) == rows
    for i, call in enumerate(dummy_client.calls):
        topic, payload = call
        assert topic == "sensor/validated"
        assert payload == {
            "temperature.raw": 17 + i,
            "temperature.cleaned": 17 + i + 0.5,
        }
