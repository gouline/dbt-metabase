import logging
import json
import uuid
import os
import re
from typing import Dict, List, Tuple, Optional

_logger = logging.getLogger(__name__)

class LockFile:

    def __init__(
        self,
        target_dir: str,
        models_subdir: Optional[str],
        lock_file: str,
    ) -> None:
        self.lock_file = os.path.join(target_dir, lock_file)
        self.models_subdir = models_subdir
        self.target_dir = target_dir

    def update_filter(self, col_name: str, model_name: str,
                      **kwargs: str) -> None:
        """Write or update a filter in the filters.lock file, preserving UUID for existing filters."""
        filters = self.read_filters() or {}
        key = f"{model_name}.{col_name}"
        if key not in filters:
            # If the filter is new, create it with a new UUID
            filters[key] = {
                "uuid": str(uuid.uuid4()),
                "col_name": col_name,
                "model_name": model_name
            }
        # Add additional keyword arguments
        filters[key].update(kwargs)

        # Write the updated filters dictionary back to the file
        with open(self.lock_file, 'w') as file:
            json.dump(filters, file, indent=4)

    def read_filters(self) -> dict:
        """Read data from the filters.lock file and parse it as JSON."""
        try:
            with open(self.lock_file, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return {}  # Return an empty dictionary if the file does not exist
        except json.JSONDecodeError:
            raise ValueError(f"File '{self.lock_file}' contains invalid JSON.")

    def get_model_dir(self, package_name: str) -> str:
        models_dir = os.path.join(self.target_dir, 'compiled', package_name,
                                  'models')
        if self.models_subdir:
            models_dir = os.path.join(models_dir, self.models_subdir)
        return models_dir

    def find_queries(self,
                     models_dir: str,
                     filter_keyword: str = '__filter__') -> Dict[str, str]:
        """
        Search for .sql files in the given directory that contain a specific keyword in their body.
    
        Args:
        directory (str): The path to the directory to search.
        filter_keyword (str): The keyword to search for in the file content.
    
        Returns:
        dict: A dictionary where keys are file paths and values are the content of the files that contain the keyword.
        """
        results = {}
        # Walk through all files and subdirectories in the given directory
        for root, dirs, files in os.walk(models_dir):
            for file in files:
                if file.endswith('.sql'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            # Check if the filter_keyword is in the file content
                            if filter_keyword in content:
                                file_name = file_name = os.path.splitext(
                                    os.path.basename(file_path))[0]
                                results[file_name] = content
                    except Exception as e:
                        raise ValueError(f"Failed to read {file_path}: {e}")

        return results

    def replace_filters(self, text: str) -> Tuple[str, List[str]]:
        """
    Replaces complex filter expressions in the form '__filter__.<table>.<column>' [any whitespace] = [any whitespace] '__filter__.<table>.<column>'
    with '{{<table>.<column>}}' and returns the modified text along with a list of all unique 'table.column' replacements found.

    Args:
    text (str): The input text containing complex filter expressions.

    Returns:
    tuple: A tuple containing:
           - The text with complex filter expressions replaced.
           - A list of unique 'table.column' used for the replacements.
    """
        pattern = r"'__filter__\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)'\s*=\s*'__filter__\.\1\.\2'"
        replacement = r"{{\1.\2}}"
        matches = set()  # To store unique table.column combinations
        def replacement_func(match: re.Match) -> str:
            table_column = f"{match.group(1)}.{match.group(2)}"
            matches.add(table_column)
            return replacement.replace(r"\1.\2", table_column)

        replaced_text = re.sub(pattern, replacement_func, text)
        return replaced_text, list(matches)
