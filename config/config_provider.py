from .config_loader import ConfigLoader


class ConfigProvider:
    def __init__(self, loader=ConfigLoader()):
        self._cache = {
            "mqtt": loader.load_config("generated_mqtt_config.json"),
            "validation": loader.load_config("generated_validation_config.json")
        }
    def mqtt(self):        return self._cache["mqtt"]
    def validation(self):  return self._cache["validation"]
