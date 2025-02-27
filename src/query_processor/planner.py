"""
Query Planner module for generating execution plans for queries.
"""

import logging
from typing import Dict, List, Any, Optional
from .core import ParsedQuery, SubQuery, DataSource

logger = logging.getLogger(__name__)

class QueryPlanner:
    """
    Plans the execution of a query by generating sub-queries for different data sources.
    """
    
    def __init__(self, data_sources: Dict[str, DataSource]):
        """
        Initialize the query planner.
        
        Args:
            data_sources: A dictionary mapping data source IDs to DataSource objects.
        """
        self.data_sources = data_sources
    
    def create_execution_plan(self, parsed_query: ParsedQuery, 
                             user_permissions: Dict[str, Any]) -> List[SubQuery]:
        """
        Create an execution plan for a parsed query.
        
        Args:
            parsed_query: The parsed query.
            user_permissions: The user's permissions.
            
        Returns:
            A list of SubQuery objects representing the execution plan.
        """
        logger.info(f"Creating execution plan for query with intent: {parsed_query.intent}")
        
        sub_queries = []
        
        for data_source_id in parsed_query.required_data_sources:
            if data_source_id not in self.data_sources:
                logger.warning(f"Required data source {data_source_id} not found")
                continue
            
            data_source = self.data_sources[data_source_id]
            
            # Check if user has permission to access this data source
            if not self._check_permission(user_permissions, data_source):
                logger.warning(f"User does not have permission to access {data_source_id}")
                continue
            
            # Generate sub-query based on data source type and query intent
            sub_query = self._generate_sub_query(parsed_query, data_source, user_permissions)
            if sub_query:
                sub_queries.append(sub_query)
        
        logger.debug(f"Created execution plan with {len(sub_queries)} sub-queries")
        return sub_queries
    
    def _check_permission(self, user_permissions: Dict[str, Any], 
                         data_source: DataSource) -> bool:
        """
        Check if the user has permission to access the data source.
        
        Args:
            user_permissions: The user's permissions.
            data_source: The data source to check.
            
        Returns:
            True if the user has permission, False otherwise.
        """
        # Check if user has access to this data source
        if 'data_sources' not in user_permissions:
            return False
        
        allowed_sources = user_permissions['data_sources']
        
        # Check if user has access to all data sources
        if '*' in allowed_sources:
            return True
        
        # Check if user has access to this specific data source
        return data_source.id in allowed_sources
    
    def _generate_sub_query(self, parsed_query: ParsedQuery, 
                           data_source: DataSource,
                           user_permissions: Dict[str, Any]) -> Optional[SubQuery]:
        """
        Generate a sub-query for a specific data source.
        
        Args:
            parsed_query: The parsed query.
            data_source: The data source to generate a sub-query for.
            user_permissions: The user's permissions.
            
        Returns:
            A SubQuery object, or None if a sub-query couldn't be generated.
        """
        # Generate query parameters based on the data source type and query intent
        parameters = self._generate_parameters(parsed_query, data_source)
        
        # Generate security filters based on user permissions
        security_filters = self._generate_security_filters(user_permissions, data_source)
        
        # Determine the query type based on the data source type and query intent
        query_type = self._determine_query_type(parsed_query.intent, data_source.type)
        
        return SubQuery(
            data_source_id=data_source.id,
            query_type=query_type,
            parameters=parameters,
            security_filters=security_filters
        )
    
    def _generate_parameters(self, parsed_query: ParsedQuery, 
                            data_source: DataSource) -> Dict[str, Any]:
        """
        Generate parameters for a sub-query.
        
        Args:
            parsed_query: The parsed query.
            data_source: The data source to generate parameters for.
            
        Returns:
            A dictionary of parameters.
        """
        # This is a simplified implementation
        # In a real system, you would have more sophisticated parameter generation
        
        parameters = {
            "intent": parsed_query.intent.value,
            "entities": parsed_query.entities,
            "filters": parsed_query.filters
        }
        
        return parameters
    
    def _generate_security_filters(self, user_permissions: Dict[str, Any], 
                                 data_source: DataSource) -> Dict[str, Any]:
        """
        Generate security filters based on user permissions.
        
        Args:
            user_permissions: The user's permissions.
            data_source: The data source to generate security filters for.
            
        Returns:
            A dictionary of security filters.
        """
        security_filters = {}
        
        # Check for row-level security
        if 'row_level_security' in user_permissions:
            rls = user_permissions['row_level_security']
            
            # Apply data source specific row-level security
            if data_source.id in rls:
                security_filters.update(rls[data_source.id])
            
            # Apply global row-level security
            if '*' in rls:
                security_filters.update(rls['*'])
        
        # Check for column-level security
        if 'column_level_security' in user_permissions:
            cls = user_permissions['column_level_security']
            
            # Apply data source specific column-level security
            if data_source.id in cls:
                security_filters['allowed_columns'] = cls[data_source.id]
            
            # Apply global column-level security
            if '*' in cls:
                security_filters['allowed_columns'] = cls['*']
        
        return security_filters
    
    def _determine_query_type(self, intent, data_source_type) -> str:
        """
        Determine the query type based on the intent and data source type.
        
        Args:
            intent: The query intent.
            data_source_type: The data source type.
            
        Returns:
            A string representing the query type.
        """
        # Map intent and data source type to a specific query type
        # This is a simplified implementation
        
        if data_source_type == "database":
            if intent == "retrieval":
                return "sql_select"
            elif intent == "calculation":
                return "sql_aggregate"
            elif intent == "comparison":
                return "sql_compare"
            elif intent == "summary":
                return "sql_summary"
        elif data_source_type == "api":
            return "api_request"
        elif data_source_type == "file":
            return "file_read"
        elif data_source_type == "vector_store":
            return "vector_search"
        
        # Default query type
        return "generic_query" 