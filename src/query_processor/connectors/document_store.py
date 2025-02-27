"""
Document store connector for the query processor.
This module provides integration between the query processor and document repositories.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import json
import os

from ..core import QueryResult, QueryError, QueryStatus
from .base import BaseConnector
from ...data_connectors.document_repository_connector import DocumentRepositoryConnector
from ...data_connectors.core import ConnectorConfig, ConnectorType

logger = logging.getLogger(__name__)

class DocumentStoreConnector(BaseConnector):
    """
    Connector for document repositories.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the document store connector.
        
        Args:
            config: The connector configuration.
        """
        super().__init__(config)
        
        # Create the underlying document repository connector
        connector_config = ConnectorConfig(
            connector_id=self.connector_id,
            connector_type=ConnectorType.DOCUMENT_REPOSITORY,
            name=self.name,
            description=self.description,
            connection_params=self.config.get("connection_params", {}),
            auth_params=self.config.get("auth_params", {}),
            additional_params=self.config.get("additional_params", {})
        )
        
        self.doc_connector = DocumentRepositoryConnector(connector_config)
        self.is_connected = False
    
    def connect(self) -> bool:
        """
        Connect to the document store.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        success = self.doc_connector.connect()
        self.is_connected = success
        return success
    
    def disconnect(self) -> bool:
        """
        Disconnect from the document store.
        
        Returns:
            True if the disconnection is successful, False otherwise.
        """
        if self.is_connected:
            success = self.doc_connector.disconnect()
            self.is_connected = not success
            return success
        return True
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """
        Execute a query against the document store.
        
        Args:
            query: The query to execute.
            params: Optional parameters for the query.
            
        Returns:
            The result of the query.
        """
        if not self.is_connected:
            if not self.connect():
                return QueryResult(
                    status=QueryStatus.ERROR,
                    error=QueryError("Failed to connect to document store"),
                    data=None
                )
        
        try:
            # Execute the query using the document repository connector
            result = self.doc_connector.execute_query(query, params)
            
            # Convert to query processor's QueryResult format
            if result.error:
                return QueryResult(
                    status=QueryStatus.ERROR,
                    error=QueryError(result.error),
                    data=None
                )
            else:
                return QueryResult(
                    status=QueryStatus.SUCCESS,
                    data=result.data,
                    metadata=result.metadata
                )
                
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            
            return QueryResult(
                status=QueryStatus.ERROR,
                error=QueryError(str(e)),
                data=None
            )
    
    def execute_natural_language_query(self, query: str) -> QueryResult:
        """
        Execute a natural language query against the document store.
        
        Args:
            query: The natural language query.
            
        Returns:
            The result of the query.
        """
        if not self.is_connected:
            if not self.connect():
                return QueryResult(
                    status=QueryStatus.ERROR,
                    error=QueryError("Failed to connect to document store"),
                    data=None
                )
        
        try:
            # Translate and execute the query
            translated_query = self.doc_connector.translate_query(query)
            logger.info(f"Translated query: {translated_query}")
            
            return self.execute_query(translated_query)
            
        except Exception as e:
            logger.error(f"Error executing natural language query: {e}")
            
            return QueryResult(
                status=QueryStatus.ERROR,
                error=QueryError(str(e)),
                data=None
            )
    
    def get_schema(self) -> Dict[str, Any]:
        """
        Get the schema of the document store.
        
        Returns:
            The schema of the document store.
        """
        if not self.is_connected:
            if not self.connect():
                return {}
        
        try:
            schema = self.doc_connector.get_schema()
            
            # Convert to dictionary format
            schema_dict = {
                "elements": {name: element.__dict__ for name, element in schema.elements.items()},
                "relationships": schema.relationships,
                "metadata": schema.metadata
            }
            
            return schema_dict
            
        except Exception as e:
            logger.error(f"Error getting schema: {e}")
            return {}
