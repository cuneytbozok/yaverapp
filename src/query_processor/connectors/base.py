"""
Base connector interface for data sources.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any

class DataSourceConnector(ABC):
    """
    Abstract base class for data source connectors.
    """
    
    @abstractmethod
    def execute(self, query_type: str, parameters: Dict[str, Any]) -> Any:
        """
        Execute a query against the data source.
        
        Args:
            query_type: The type of query to execute.
            parameters: The parameters for the query.
            
        Returns:
            The query results.
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