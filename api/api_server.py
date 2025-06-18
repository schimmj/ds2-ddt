# api/api_server.py
from fastapi import FastAPI, HTTPException, Query, Request, Path
from pydantic import BaseModel, Extra

app = FastAPI(title="Data Ingestion API")

class LoosePayload(BaseModel):
    """
    A catch-all payload model that allows any extra fields.
    """
    class Config:
        extra = Extra.allow

@app.post("/ingest")
async def ingest_data(
    payload: LoosePayload,
    request: Request,
    topic: str = Query(..., description="MQTT topic, e.g. /air-quality")
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
