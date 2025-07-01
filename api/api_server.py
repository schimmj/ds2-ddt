# api/api_server.py
from fastapi import FastAPI, HTTPException, Query, Request, Path, Body
from pydantic import BaseModel, Extra

from batch.pipeline_manager import PipelineManager
from tests.test_result_mqtt_pipeline import pipeline
from typing import List
import pandas as pd
import json


app = FastAPI(title="Data Ingestion API")

class LoosePayload(BaseModel):
    """
    A catch-all payload model that allows any extra fields.
    """
    class Config:
        extra = Extra.allow

@app.post("/ingest/{topic}")
async def ingest_data_item(
    payload: LoosePayload,
    request: Request,
    topic: str = Path(..., description="MQTT topic, e.g. /air-quality")
):
    """
    Receives POSTs to /ingest/{topic}, where:
      - `topic` is taken from the URL path (supports slashes via `{topic:path}`).
      - `payload` is any JSON object, with fields dynamically allowed.
    Dispatches into the same BatchPipeline(s) as MQTT.
    """
    manager = request.app.state.manager
    ok = manager.dispatch(topic, payload.dict())
    if not ok:
        raise HTTPException(status_code=404, detail=f"No pipeline for topic '{topic}'")
    return {"status": "queued", "topic": topic}




@app.post("/ingest/{topic}/sync")
async def ingest_data_batch(
    request: Request,
    topic: str = Path(..., description="MQTT topic, e.g. /air-quality"),
    payloads: List[LoosePayload] = Body(
        ..., 
        description="Array of records to process"
    ),
):
    manager: PipelineManager = request.app.state.manager

    try:
        pipeline = manager.get_pipeline(topic)
    except KeyError:
        raise HTTPException(
            status_code=404, 
            detail=f"No pipeline for topic '{topic}'"
        )

    # Convert your Pydantic models into plain dicts
    raw_dicts = [p.dict() for p in payloads]
    df = pd.DataFrame(raw_dicts)

    # Run your synchronous processing
    cleaned_df = pipeline.process_sync(df)
    cleaned = json.loads(cleaned_df.to_json(orient="records"))

    return {
        "status": "processed",
        "topic": topic,
        "count": len(cleaned),
        "cleaned": cleaned,
    }

