import config
from .config_loader import ConfigLoader


class ConfigProvider:
    def __init__(self):
        loader = ConfigLoader()
        self._cache = {
            "mqtt": loader.load_config(),
            "validation": loader.load_config("validations")
        }
    def mqtt(self):        return self._cache["mqtt"]["generated_mqtt_config"]
    def validation(self):  return self._cache["validation"]
