"""
Core module for data source connectors in the AI-powered data retrieval application.
This module defines the base interfaces and common functionality for all connectors.
"""

import logging
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConnectorType(Enum):
    """Enum representing different types of data source connectors."""
    SQL = "sql"
    DOCUMENT_REPOSITORY = "document_repository"
    REST_API = "rest_api"
    FILE_SYSTEM = "file_system"
    CUSTOM = "custom"

@dataclass
class ConnectorConfig:
    """Configuration for a data source connector."""
    connector_id: str
    connector_type: ConnectorType
    name: str
    description: Optional[str] = None
    connection_params: Dict[str, Any] = field(default_factory=dict)
    auth_params: Dict[str, Any] = field(default_factory=dict)
    additional_params: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SchemaElement:
    """Represents an element in a data source schema."""
    name: str
    data_type: str
    description: Optional[str] = None
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_reference: Optional[str] = None
    additional_metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Schema:
    """Represents the schema of a data source."""
    elements: Dict[str, SchemaElement] = field(default_factory=dict)
    relationships: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class QueryResult:
    """Represents the result of a query to a data source."""
    data: Any
    schema: Optional[Schema] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    execution_time: float = 0.0

class DataSourceConnector(ABC):
    """
    Abstract base class for data source connectors.
    """
    
    def __init__(self, config: ConnectorConfig):
        """
        Initialize the data source connector.
        
        Args:
            config: The connector configuration.
        """
        self.config = config
        self.connection = None
        self.schema = None
        self.is_connected = False
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Connect to the data source.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """
        Disconnect from the data source.
        
        Returns:
            True if the disconnection is successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test the connection to the data source.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def discover_schema(self) -> Schema:
        """
        Discover the schema of the data source.
        
        Returns:
            The schema of the data source.
        """
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """
        Execute a query against the data source.
        
        Args:
            query: The query to execute.
            params: Optional parameters for the query.
            
        Returns:
            The result of the query.
        """
        pass
    
    @abstractmethod
    def translate_query(self, natural_language_query: str) -> str:
        """
        Translate a natural language query to the native query format.
        
        Args:
            natural_language_query: The natural language query.
            
        Returns:
            The translated query in the native format.
        """
        pass
    
    def execute_natural_language_query(self, query: str) -> QueryResult:
        """
        Execute a natural language query against the data source.
        
        Args:
            query: The natural language query.
            
        Returns:
            The result of the query.
        """
        try:
            # Translate the query
            translated_query = self.translate_query(query)
            
            # Execute the translated query
            return self.execute_query(translated_query)
            
        except Exception as e:
            logger.error(f"Error executing natural language query: {e}")
            
            return QueryResult(
                data=None,
                error=str(e),
                execution_time=0.0
            )
    
    def get_schema(self) -> Schema:
        """
        Get the schema of the data source.
        
        Returns:
            The schema of the data source.
        """
        if self.schema is None:
            self.schema = self.discover_schema()
        
        return self.schema 