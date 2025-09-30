# SPDX-FileCopyrightText: 2025 - 2025 Software GmbH, Darmstadt, Germany and/or its subsidiaries and/or its affiliates
# SPDX-License-Identifier: Apache-2.0

from .config_manager import ConfigManager


class ConfigProvider:
    def __init__(self):
        self.reload()

    def reload(self):
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
