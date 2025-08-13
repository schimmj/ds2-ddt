# api/api_server.py
from fastapi import FastAPI, HTTPException, Query, Request, Path, Body
from pydantic import BaseModel, Extra

from batch.batch_pipeline import BatchPipeline
from batch.pipeline_manager import PipelineManager
from tests.test_result_mqtt_pipeline import pipeline
from typing import List, Literal, Optional, Dict, Any
import pandas as pd
import json
from config import ConfigProvider
from config import ConfigSaver


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
    config_id: str = Query(None, description="Optional config ID to use for processing"),
    payloads: List[LoosePayload] = Body(
        ..., 
        description="Array of records to process"
    ),
):
    # manager: PipelineManager = request.app.state.manager

    # try:
    #     pipeline = manager.get_pipeline(topic)
    # except KeyError:
    #     raise HTTPException(
    #         status_code=404, 
    #         detail=f"No pipeline for topic '{topic}'"
    #     )
    config_name = f"{config_id}_{topic}"
    pipeline = BatchPipeline(topic, config_name, batch_size=1) # batch size of 1 for synchronous processing irrelevant

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



@app.get("/configs", 
         summary="List configurations",
        description="Retrieve configurations filtered by kind. Use pagination via limit/offset.")
async def get_configs(
    cfg_type: Optional[Literal["mqtt", "validation"]] = Query(
        None, alias="type", description="Filter to a specific config type"
    )
):
    """
    Return the currently saved configurations from disk.

    - type=mqtt        -> all JSON files in 'config/' (mapping: filename -> content)
    - type=validation  -> all JSON files in 'config/validations/'
    - (no type)        -> both, as { "mqtt": {...}, "validation": {...} }
    """
    try:
        provider = ConfigProvider()

        if cfg_type == "mqtt":
            mqtt_cfg = provider.mqtt() 
            return {"mqtt": mqtt_cfg}

        if cfg_type == "validation":
            validation_cfg = provider.validation() 
            return {"validation": validation_cfg}

        # default: both
        mqtt_cfg = provider.mqtt()
        validation_cfg = provider.validation()
        return {"mqtt": mqtt_cfg, "validation": validation_cfg}

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail={str(e)})
    except json.JSONDecodeError as e:
        # surface which file was malformed if available
        raise HTTPException(status_code=422, detail=f"Invalid JSON in config files: {e}")
    

@app.post("/configs",
          summary="Save a new configuration")
async def ingest_config(
    request: Request,
    cfg_type: Literal["mqtt", "validation"] = Query(..., alias="type", description="Type of configuration to save"),
    cfg_id: Optional[str] = Query(None, alias="config_id", description="Optional ID for the configuration (required for validation)"),
    payload: Dict[str, Any] = Body(..., description="Configuration JSON object.")):


    config_saver = ConfigSaver(base_path="config")
    
    try:
        target = config_saver.write_atomic(cfg_type, cfg_id, payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write config: {e}")
    

    return {
        "status": "ok",
        "type": cfg_type,
        "name": cfg_id or ("generated_mqtt_config" if cfg_type == "mqtt" else None),
        "path": str(target)
    }