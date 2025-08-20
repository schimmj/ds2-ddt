# config/config_provider.py
from .config_loader import ConfigLoader
from .config_manager import ConfigManager
from config import config_manager

class ConfigProvider:
    def __init__(self):
        self.reload()

    def reload(self):
        loader = ConfigLoader()
        config_manager = ConfigManager()
        
        # self._cache = {
        #     "mqtt": loader.load_config(),
        #     "validation": loader.load_config("validations"),
        # }

        self._cache = {
            "mqtt": config_manager.load("generated_mqtt_config.json"),
            "validation": config_manager.load("validations"),
        }

    def mqtt(self):
        # Keep your current access pattern
        return self._cache["mqtt"]

    def validation(self):
        return self._cache["validation"]
