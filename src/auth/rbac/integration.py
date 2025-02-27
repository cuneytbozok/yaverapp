"""
Integration of RBAC with the query processor.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Union
from functools import wraps

from .core import (
    PermissionEvaluator, ResourceType, PermissionLevel, PermissionDeniedError
)
from .query_modifier import QueryModifier
from .audit import AuditLogger

logger = logging.getLogger(__name__)

class RBACQueryProcessor:
    """
    Wrapper for the query processor that enforces RBAC security.
    """
    
    def __init__(self, query_processor, permission_evaluator: PermissionEvaluator, 
                audit_logger: Optional[AuditLogger] = None):
        """
        Initialize the RBAC query processor.
        
        Args:
            query_processor: The underlying query processor.
            permission_evaluator: The permission evaluator.
            audit_logger: The audit logger.
        """
        self.query_processor = query_processor
        self.permission_evaluator = permission_evaluator
        self.query_modifier = QueryModifier(permission_evaluator)
        self.audit_logger = audit_logger
    
    def execute_sql_query(self, user_id: str, query: str, data_source_id: str, 
                        client_ip: Optional[str] = None, request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a SQL query with RBAC security.
        
        Args:
            user_id: The ID of the user.
            query: The SQL query.
            data_source_id: The ID of the data source.
            client_ip: The client IP address.
            request_id: The request ID.
            
        Returns:
            The query results.
        """
        start_time = time.time()
        
        try:
            # Check data source permission
            self.permission_evaluator.check_permission(
                user_id, 
                ResourceType.DATA_SOURCE, 
                data_source_id, 
                PermissionLevel.READ
            )
            
            # Modify the query to enforce security
            modified_query = self.query_modifier.modify_sql_query(user_id, query, data_source_id)
            
            # Execute the modified query
            result = self.query_processor.execute_sql_query(modified_query, data_source_id)
            
            # Apply field-level redaction if needed
            result = self._apply_field_redaction(user_id, result, data_source_id)
            
            # Log the query execution
            execution_time_ms = (time.time() - start_time) * 1000
            if self.audit_logger:
                self.audit_logger.log_query_execution(
                    user_id=user_id,
                    query_type="SQL",
                    data_source_id=data_source_id,
                    original_query=query,
                    modified_query=modified_query,
                    execution_time_ms=execution_time_ms,
                    client_ip=client_ip,
                    request_id=request_id
                )
            
            return result
        
        except PermissionDeniedError as e:
            # Log the permission denial
            if self.audit_logger:
                self.audit_logger.log_permission_check(
                    user_id=user_id,
                    resource_type=e.resource_type,
                    resource_id=e.resource_id,
                    required_level=e.required_level,
                    granted=False,
                    client_ip=client_ip,
                    request_id=request_id
                )
            
            # Re-raise the exception
            raise
    
    def execute_nosql_query(self, user_id: str, query: Dict[str, Any], 
                          data_source_id: str, collection_id: str,
                          client_ip: Optional[str] = None, request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a NoSQL query with RBAC security.
        
        Args:
            user_id: The ID of the user.
            query: The NoSQL query.
            data_source_id: The ID of the data source.
            collection_id: The ID of the collection.
            client_ip: The client IP address.
            request_id: The request ID.
            
        Returns:
            The query results.
        """
        start_time = time.time()
        
        try:
            # Check data source permission
            self.permission_evaluator.check_permission(
                user_id, 
                ResourceType.DATA_SOURCE, 
                data_source_id, 
                PermissionLevel.READ
            )
            
            # Check collection permission
            collection_full_id = f"{data_source_id}.{collection_id}"
            self.permission_evaluator.check_permission(
                user_id, 
                ResourceType.COLLECTION, 
                collection_full_id, 
                PermissionLevel.READ
            )
            
            # Modify the query to enforce security
            modified_query = self.query_modifier.modify_nosql_query(
                user_id, query, data_source_id, collection_id
            )
            
            # Execute the modified query
            result = self.query_processor.execute_nosql_query(
                modified_query, data_source_id, collection_id
            )
            
            # Apply field-level redaction if needed
            result = self._apply_field_redaction(user_id, result, data_source_id, collection_id)
            
            # Log the query execution
            execution_time_ms = (time.time() - start_time) * 1000
            if self.audit_logger:
                self.audit_logger.log_query_execution(
                    user_id=user_id,
                    query_type="NoSQL",
                    data_source_id=data_source_id,
                    original_query=str(query),
                    modified_query=str(modified_query),
                    execution_time_ms=execution_time_ms,
                    client_ip=client_ip,
                    request_id=request_id
                )
            
            return result
        
        except PermissionDeniedError as e:
            # Log the permission denial
            if self.audit_logger:
                self.audit_logger.log_permission_check(
                    user_id=user_id,
                    resource_type=e.resource_type,
                    resource_id=e.resource_id,
                    required_level=e.required_level,
                    granted=False,
                    client_ip=client_ip,
                    request_id=request_id
                )
            
            # Re-raise the exception
            raise
    
    def _apply_field_redaction(self, user_id: str, result: Dict[str, Any], 
                             data_source_id: str, collection_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Apply field-level redaction to query results.
        
        Args:
            user_id: The ID of the user.
            result: The query results.
            data_source_id: The ID of the data source.
            collection_id: The ID of the collection (for NoSQL queries).
            
        Returns:
            The redacted query results.
        """
        # For SQL queries, the field permissions are already applied in the query
        # For NoSQL queries, we need to check if any fields need to be redacted
        if not collection_id or "data" not in result:
            return result
        
        collection_full_id = f"{data_source_id}.{collection_id}"
        field_permissions = self.permission_evaluator.get_field_permissions(user_id, collection_full_id)
        
        if not field_permissions:
            return result
        
        # Get fields that need to be redacted
        redacted_fields = []
        for field_id, level in field_permissions.items():
            if level.value < PermissionLevel.READ.value:
                redacted_fields.append(field_id)
        
        if not redacted_fields:
            return result
        
        # Apply redaction to each document
        redacted_data = []
        for document in result.get("data", []):
            redacted_doc = document.copy()
            for field in redacted_fields:
                if field in redacted_doc:
                    redacted_doc[field] = "[REDACTED]"
            redacted_data.append(redacted_doc)
        
        result["data"] = redacted_data
        return result 