# ==================================================================================
# UTILITY FUNCTIONS
# ==================================================================================
# Common utility functions for the streaming job.
# ==================================================================================

import importlib
from typing import Any


def load_transformer_class(module_path: str, class_name: str) -> Any:
    """Dynamically load a transformer class.
    
    Args:
        module_path: Python module path (e.g., 'transformations.bid_events_raw')
        class_name: Name of the class to load
        
    Returns:
        Transformer class
    """
    try:
        module = importlib.import_module(module_path)
        transformer_class = getattr(module, class_name)
        return transformer_class
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Failed to load transformer '{class_name}' from '{module_path}': {e}")


def sanitize_table_name(name: str) -> str:
    """Sanitize a string to be used as a SQL table name.
    
    Args:
        name: Original name
        
    Returns:
        Sanitized name safe for SQL
    """
    return name.replace('-', '_').replace('.', '_')


def format_dict_for_logging(data: dict, indent: int = 2) -> str:
    """Format a dictionary for pretty logging.
    
    Args:
        data: Dictionary to format
        indent: Indentation level
        
    Returns:
        Formatted string
    """
    import json
    return json.dumps(data, indent=indent)
