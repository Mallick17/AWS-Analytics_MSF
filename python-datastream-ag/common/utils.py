# ==================================================================================
# UTILITY FUNCTIONS
# ==================================================================================
# Common utility functions for the streaming job.
# ==================================================================================

import importlib
from typing import Any, Dict


def load_transformer_class(module_path: str, class_name: str) -> Any:
    """Dynamically load a transformer class.
    
    Args:
        module_path: Python module path (e.g., 'transformations.bid_events_raw')
        class_name: Name of the class to load
        
    Returns:
        Transformer class (not instance)
        
    Raises:
        ImportError: If module cannot be imported
        AttributeError: If class not found in module
        
    Example:
        >>> TransformerClass = load_transformer_class(
        ...     'transformations.bid_events_raw',
        ...     'BidEventsRawTransformer'
        ... )
        >>> transformer = TransformerClass(topic_config)
    """
    try:
        print(f"      Importing module: {module_path}")
        module = importlib.import_module(module_path)
        print(f"      ✓ Module imported successfully")
    except ImportError as e:
        raise ImportError(f"Failed to import module '{module_path}': {e}")
    
    try:
        print(f"      Loading class: {class_name}")
        transformer_class = getattr(module, class_name)
        print(f"      ✓ Class loaded successfully")
    except AttributeError as e:
        raise AttributeError(f"Class '{class_name}' not found in module '{module_path}': {e}")
    
    # Verify the class has required method
    if not hasattr(transformer_class, 'get_transformation_sql'):
        raise AttributeError(
            f"Transformer class '{class_name}' must have 'get_transformation_sql' method"
        )
    
    return transformer_class


def load_and_instantiate_transformer(
    transformation_name: str, 
    transformations_registry: Dict[str, Any],
    topic_config: Dict[str, Any]
) -> Any:
    """Load and instantiate a transformer from the registry.
    
    Args:
        transformation_name: Name of transformation from transformations.yaml
        transformations_registry: Dictionary of available transformations
        topic_config: Topic configuration dictionary
        
    Returns:
        Instantiated transformer object
        
    Raises:
        ValueError: If transformation not found in registry
        ImportError: If transformer cannot be loaded
        
    Example:
        >>> transformer = load_and_instantiate_transformer(
        ...     'bid_events_raw',
        ...     transformations_registry,
        ...     topic_config
        ... )
        >>> sql = transformer.get_transformation_sql(source_table)
    """
    # Validate transformation exists in registry
    if transformation_name not in transformations_registry:
        available = list(transformations_registry.keys())
        raise ValueError(
            f"Transformation '{transformation_name}' not found in registry. "
            f"Available transformations: {available}"
        )
    
    trans_config = transformations_registry[transformation_name]
    
    # Validate transformer configuration
    if 'module' not in trans_config or 'class' not in trans_config:
        raise ValueError(
            f"Transformer '{transformation_name}' configuration must have 'module' and 'class' fields"
        )
    
    module_path = trans_config['module']
    class_name = trans_config['class']
    
    print(f"    Loading transformer: {transformation_name}")
    print(f"      Module: {module_path}")
    print(f"      Class: {class_name}")
    
    # Load transformer class
    TransformerClass = load_transformer_class(module_path, class_name)
    
    # Instantiate transformer with topic config
    try:
        transformer = TransformerClass(topic_config)
        print(f"      ✓ Transformer instantiated: {class_name}")
    except Exception as e:
        raise RuntimeError(f"Failed to instantiate transformer '{class_name}': {e}")
    
    return transformer


def sanitize_table_name(name: str) -> str:
    """Sanitize a string to be used as a SQL table name.
    
    Args:
        name: Original name
        
    Returns:
        Sanitized name safe for SQL
        
    Example:
        >>> sanitize_table_name("my-topic.v1")
        'my_topic_v1'
    """
    return name.replace('-', '_').replace('.', '_')


def format_dict_for_logging(data: dict, indent: int = 2) -> str:
    """Format a dictionary for pretty logging.
    
    Args:
        data: Dictionary to format
        indent: Indentation level
        
    Returns:
        Formatted JSON string
        
    Example:
        >>> config = {'topic': 'test', 'enabled': True}
        >>> print(format_dict_for_logging(config))
        {
          "topic": "test",
          "enabled": true
        }
    """
    import json
    return json.dumps(data, indent=indent)


def validate_topic_config(topic_config: Dict[str, Any], topic_name: str) -> None:
    """Validate that a topic configuration has all required fields.
    
    Args:
        topic_config: Topic configuration dictionary
        topic_name: Name of the topic (for error messages)
        
    Raises:
        ValueError: If required fields are missing
    """
    required_fields = ['source_schema', 'sink', 'transformation']
    
    for field in required_fields:
        if field not in topic_config:
            raise ValueError(f"Topic '{topic_name}' missing required field: '{field}'")
    
    # Validate sink configuration
    sink_config = topic_config['sink']
    if 'table_name' not in sink_config:
        raise ValueError(f"Topic '{topic_name}' sink missing 'table_name'")
    
    if 'schema' not in sink_config:
        raise ValueError(f"Topic '{topic_name}' sink missing 'schema'")
    
    # Validate schemas are non-empty
    if not topic_config['source_schema']:
        raise ValueError(f"Topic '{topic_name}' has empty source_schema")
    
    if not sink_config['schema']:
        raise ValueError(f"Topic '{topic_name}' has empty sink schema")