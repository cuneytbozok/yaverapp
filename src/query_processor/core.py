"""
Core Query Processor for AI-powered data retrieval application.
This module contains the main components for processing natural language queries
and retrieving data from various sources.
"""

import logging
from typing import Dict, List, Any, Optional, Set
from enum import Enum
from dataclasses import dataclass
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class QueryIntent(Enum):
    """Enum representing different types of query intents."""
    RETRIEVAL = "retrieval"  # Simple data retrieval
    CALCULATION = "calculation"  # Requires computation on data
    COMPARISON = "comparison"  # Comparing multiple data points
    SUMMARY = "summary"  # Summarizing large datasets
    UNKNOWN = "unknown"  # Intent couldn't be determined

class DataSourceType(Enum):
    """Enum representing different types of data sources."""
    DATABASE = "database"
    API = "api"
    FILE = "file"
    VECTOR_STORE = "vector_store"
    CUSTOM = "custom"

@dataclass
class DataSource:
    """Represents a data source configuration."""
    id: str
    name: str
    type: DataSourceType
    connection_details: Dict[str, Any]
    requires_auth: bool = False
    
@dataclass
class ParsedQuery:
    """Represents a parsed natural language query."""
    original_query: str
    intent: QueryIntent
    entities: List[str]
    filters: Dict[str, Any]
    required_data_sources: List[str]
    
@dataclass
class SubQuery:
    """Represents a query for a specific data source."""
    data_source_id: str
    query_type: str
    parameters: Dict[str, Any]
    security_filters: Dict[str, Any]
    
@dataclass
class QueryResult:
    """Represents the result from a data source."""
    data_source_id: str
    status: str
    data: Any
    error: Optional[str] = None
    
@dataclass
class ProcessedResult:
    """Represents the final processed result to be returned to the user."""
    answer: str
    supporting_data: Dict[str, Any]
    confidence_score: float
    execution_time: float 