# config/config_saver.py
import json, os, tempfile
from pathlib import Path
from typing import Any, Dict, Optional

class ConfigSaver:
    def __init__(self, base_path: str | Path = "config"):
        self.base_path = Path(base_path)

    def _target_path(self, cfg_type: str, config_id: Optional[str]) -> Path:
        if cfg_type == "mqtt":
            stem = "generated_mqtt_config"
            return self.base_path / f"{stem}.json"
        elif cfg_type == "validation":
            if not config_id:
                raise ValueError("Validation config requires ?config_id=<file-stem>")
            return self.base_path / "validations" / f"{config_id}.json"
        else:
            raise ValueError("cfg_type must be 'mqtt' or 'validation'")


    def read_current(self, cfg_type: str, config_id: Optional[str]) -> Optional[Dict[str, Any]]:
        p = self._target_path(cfg_type, config_id)
        if not p.exists():
            return None
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)

    def write_atomic(self, cfg_type: str, config_id: Optional[str], content: Dict[str, Any]) -> Path:
        target = self._target_path(cfg_type, config_id)
        target.parent.mkdir(parents=True, exist_ok=True)

        # write to temp file then rename (atomic on POSIX)
        fd, tmp_path = tempfile.mkstemp(dir=str(target.parent), prefix=target.stem + ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                json.dump(content, tmp, ensure_ascii=False, indent=2)
                tmp.flush()
                os.fsync(tmp.fileno())
            os.replace(tmp_path, target)  # atomic move
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
        return target
