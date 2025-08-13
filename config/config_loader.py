import json
from pathlib import Path
from typing import Dict, Any, Union, Optional


class ConfigLoader:
    def __init__(self, base_path: Union[str, Path] = 'config'):
        """
        Initialize ConfigLoader with a base directory for JSON config files.

        Args:
            base_path (str | Path): Path to the default directory containing JSON config files.

        Raises:
            FileNotFoundError: If the specified base_path does not exist.
        """
        self.base_path = Path(base_path)
        if not self.base_path.exists():
            raise FileNotFoundError(f"Config directory '{self.base_path}' does not exist")

    def load_config(self, path: Optional[Union[str, Path]] = None) -> Union[Dict[str, Any], Any]:
        """
        Load JSON configuration files.

        If `path` is:
        - None: loads all JSON files from the default base directory.
        - a directory: loads all JSON files from that directory.
        - a file: loads and returns only that JSON file's content.

        Args:
            path (str | Path | None): Optional path to a JSON file or directory of JSON files.
                If a relative path is provided, it is resolved against the base directory.

        Returns:
            Dict[str, Any]: When loading a directory (base or provided), returns a mapping
                from file stem to parsed JSON content.
            Any: When loading a single JSON file, returns its parsed content directly.

        Raises:
            FileNotFoundError: If the target directory or file does not exist,
                or if no JSON files are found in a directory.
            json.JSONDecodeError: If any JSON file is malformed.
        """
        # Determine the target path
        if path is None:
            target = self.base_path
        else:
            target = Path(path)
            # Resolve relative to base_path
            if not target.is_absolute():
                target = self.base_path / target

        if target.is_file():
            # Load and return single JSON file
            with target.open('r', encoding='utf-8') as f:
                return json.load(f)

        if target.is_dir():
            # Load all JSON files from the directory
            configs: Dict[str, Any] = {}
            for filepath in target.glob('*.json'):
                with filepath.open('r', encoding='utf-8') as f:
                    configs[filepath.stem] = json.load(f)

            if not configs:
                raise FileNotFoundError(f"No JSON configuration files found in '{target}'")

            return configs

        raise FileNotFoundError(f"Config path '{target}' does not exist")
