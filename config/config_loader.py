import yaml
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
        Load configuration from a YAML file.
        
        Args:
            filename: Name of the config file (e.g., 'mqtt_config.yaml')
            
        Returns:
            Dict containing the configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid YAML
        """
        config_path = self.base_path / filename
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file {filename} not found in {self.base_path}")
            
        if not filename.endswith('.yaml') and not filename.endswith('.yml'):
            raise ValueError("Config file must be a YAML file")
            
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing {filename}: {str(e)}")