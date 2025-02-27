"""
Query Executor module for executing sub-queries against data sources.
"""

import logging
import time
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from .core import SubQuery, QueryResult, DataSource

logger = logging.getLogger(__name__)

class QueryExecutor:
    """
    Executes sub-queries against data sources and collects results.
    """
    
    def __init__(self, data_source_connectors: Dict[str, Any]):
        """
        Initialize the query executor.
        
        Args:
            data_source_connectors: A dictionary mapping data source IDs to connector objects.
        """
        self.data_source_connectors = data_source_connectors
    
    def execute(self, sub_queries: List[SubQuery], timeout: int = 30) -> List[QueryResult]:
        """
        Execute a list of sub-queries.
        
        Args:
            sub_queries: The list of sub-queries to execute.
            timeout: The maximum time to wait for all queries to complete (in seconds).
            
        Returns:
            A list of QueryResult objects.
        """
        logger.info(f"Executing {len(sub_queries)} sub-queries")
        
        results = []
        
        # Execute sub-queries in parallel
        with ThreadPoolExecutor() as executor:
            # Submit all sub-queries for execution
            future_to_query = {
                executor.submit(self._execute_sub_query, sub_query): sub_query
                for sub_query in sub_queries
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_query, timeout=timeout):
                sub_query = future_to_query[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error executing sub-query for {sub_query.data_source_id}: {str(e)}")
                    # Create an error result
                    results.append(QueryResult(
                        data_source_id=sub_query.data_source_id,
                        status="error",
                        data=None,
                        error=str(e)
                    ))
        
        logger.debug(f"Executed {len(results)} sub-queries")
        return results
    
    def _execute_sub_query(self, sub_query: SubQuery) -> QueryResult:
        """
        Execute a single sub-query.
        
        Args:
            sub_query: The sub-query to execute.
            
        Returns:
            A QueryResult object.
        """
        logger.info(f"Executing sub-query for data source: {sub_query.data_source_id}")
        
        start_time = time.time()
        
        try:
            # Get the connector for this data source
            if sub_query.data_source_id not in self.data_source_connectors:
                raise ValueError(f"No connector found for data source: {sub_query.data_source_id}")
            
            connector = self.data_source_connectors[sub_query.data_source_id]
            
            # Apply security filters
            filtered_parameters = self._apply_security_filters(
                sub_query.parameters, 
                sub_query.security_filters
            )
            
            # Execute the query using the connector
            data = connector.execute(sub_query.query_type, filtered_parameters)
            
            execution_time = time.time() - start_time
            logger.info(f"Sub-query for {sub_query.data_source_id} completed in {execution_time:.2f}s")
            
            return QueryResult(
                data_source_id=sub_query.data_source_id,
                status="success",
                data=data
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Error executing sub-query for {sub_query.data_source_id}: {str(e)}")
            
            return QueryResult(
                data_source_id=sub_query.data_source_id,
                status="error",
                data=None,
                error=str(e)
            )
    
    def _apply_security_filters(self, parameters: Dict[str, Any], 
                              security_filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply security filters to query parameters.
        
        Args:
            parameters: The original query parameters.
            security_filters: The security filters to apply.
            
        Returns:
            The filtered parameters.
        """
        # Create a copy of the parameters to avoid modifying the original
        filtered_parameters = parameters.copy()
        
        # Apply column-level security
        if 'allowed_columns' in security_filters:
            allowed_columns = security_filters['allowed_columns']
            
            # If we have a 'select' parameter with columns, filter it
            if 'select' in filtered_parameters and isinstance(filtered_parameters['select'], list):
                filtered_parameters['select'] = [
                    col for col in filtered_parameters['select']
                    if col in allowed_columns or col == '*'
                ]
        
        # Apply row-level security by adding additional filters
        for key, value in security_filters.items():
            if key != 'allowed_columns':
                # Add the security filter to the parameters
                if 'filters' not in filtered_parameters:
                    filtered_parameters['filters'] = {}
                
                filtered_parameters['filters'][key] = value
        
        return filtered_parameters 