import json
from pathlib import Path
from typing import Dict, Any, Union

class ConfigLoader:
    def __init__(self, base_path: Union[str, Path] = 'config'):
        """Initialize ConfigLoader with base path for config files."""
        self.base_path = Path(base_path)
        if not self.base_path.exists():
            raise FileNotFoundError(f"Config directory {base_path} does not exist")

    def load_config(self, filename: str) -> Dict[str, Any]:
        """
        Load configuration from a JSON file.
        
        Args:
            filename: Name of the config file (e.g., 'mqtt_config.json')
            
        Returns:
            Dict containing the configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
        """
        config_path = self.base_path / filename
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file {filename} not found in {self.base_path}") 
            
        if not filename.endswith('.json'):
            raise ValueError("Config file must be a JSON file")
            
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Error parsing {filename}: {str(e)}")