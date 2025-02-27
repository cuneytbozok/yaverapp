"""
Query modifier for applying RBAC security to queries.
"""

import logging
from typing import Dict, List, Any, Optional, Union, Set, Tuple, Callable
import copy
import json
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Token

from .core import (
    PermissionEvaluator, ResourceType, PermissionLevel, PermissionDeniedError
)

logger = logging.getLogger(__name__)

class QueryModifier:
    """
    Modifies queries to enforce RBAC security.
    """
    
    def __init__(self, permission_evaluator: PermissionEvaluator):
        """
        Initialize the query modifier.
        
        Args:
            permission_evaluator: The permission evaluator.
        """
        self.permission_evaluator = permission_evaluator
    
    def modify_sql_query(self, user_id: str, query: str, data_source_id: str) -> str:
        """
        Modify a SQL query to enforce RBAC security.
        
        Args:
            user_id: The ID of the user.
            query: The SQL query.
            data_source_id: The ID of the data source.
            
        Returns:
            The modified SQL query.
        """
        # Parse the SQL query
        parsed = sqlparse.parse(query)
        if not parsed:
            logger.error(f"Failed to parse SQL query: {query}")
            return query
        
        stmt = parsed[0]
        
        # Get the tables referenced in the query
        tables = self._extract_tables(stmt)
        
        # Check data source permission
        self.permission_evaluator.check_permission(
            user_id, 
            ResourceType.DATA_SOURCE, 
            data_source_id, 
            PermissionLevel.READ
        )
        
        # Check table permissions and apply record filters
        modified_query = query
        for table_name in tables:
            table_id = f"{data_source_id}.{table_name}"
            
            # Check table permission
            self.permission_evaluator.check_permission(
                user_id, 
                ResourceType.TABLE, 
                table_id, 
                PermissionLevel.READ
            )
            
            # Get record filter for the table
            record_filter = self.permission_evaluator.get_record_filter(user_id, table_id)
            if record_filter:
                # Convert filter to SQL WHERE clause
                where_clause = self._filter_to_sql(record_filter, table_name)
                if where_clause:
                    # Add the WHERE clause to the query
                    modified_query = self._add_where_clause(modified_query, where_clause)
        
        # Apply field-level permissions
        modified_query = self._apply_field_permissions(user_id, modified_query, data_source_id, tables)
        
        return modified_query
    
    def modify_nosql_query(self, user_id: str, query: Dict[str, Any], 
                          data_source_id: str, collection_id: str) -> Dict[str, Any]:
        """
        Modify a NoSQL query to enforce RBAC security.
        
        Args:
            user_id: The ID of the user.
            query: The NoSQL query.
            data_source_id: The ID of the data source.
            collection_id: The ID of the collection.
            
        Returns:
            The modified NoSQL query.
        """
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
        
        # Get record filter for the collection
        record_filter = self.permission_evaluator.get_record_filter(user_id, collection_full_id)
        
        # Apply record filter to query
        modified_query = copy.deepcopy(query)
        if record_filter:
            if "$filter" in modified_query:
                # Combine with existing filter using $and
                existing_filter = modified_query["$filter"]
                modified_query["$filter"] = {"$and": [existing_filter, record_filter]}
            else:
                # Add filter
                modified_query["$filter"] = record_filter
        
        # Apply field-level permissions
        field_permissions = self.permission_evaluator.get_field_permissions(user_id, collection_full_id)
        if field_permissions:
            # Create projection to include only readable fields
            projection = {}
            for field_id, level in field_permissions.items():
                if level.value >= PermissionLevel.READ.value:
                    projection[field_id] = 1
            
            if projection:
                if "$projection" in modified_query:
                    # Combine with existing projection
                    existing_projection = modified_query["$projection"]
                    for field, value in existing_projection.items():
                        if value == 1 and field not in projection:
                            # Field is requested but not allowed
                            raise PermissionDeniedError(
                                user_id, 
                                ResourceType.FIELD, 
                                f"{collection_full_id}.{field}", 
                                PermissionLevel.READ
                            )
                    
                    # Keep only allowed fields
                    modified_query["$projection"] = {
                        field: 1 for field in existing_projection if field in projection
                    }
                else:
                    # Add projection
                    modified_query["$projection"] = projection
        
        return modified_query
    
    def redact_results(self, user_id: str, results: List[Dict[str, Any]], 
                      data_source_id: str, table_id: str) -> List[Dict[str, Any]]:
        """
        Redact fields in query results based on permissions.
        
        Args:
            user_id: The ID of the user.
            results: The query results.
            data_source_id: The ID of the data source.
            table_id: The ID of the table or collection.
            
        Returns:
            The redacted results.
        """
        # Get field permissions
        full_table_id = f"{data_source_id}.{table_id}"
        field_permissions = self.permission_evaluator.get_field_permissions(user_id, full_table_id)
        
        if not field_permissions:
            return results
        
        # Redact fields
        redacted_results = []
        for record in results:
            redacted_record = {}
            for field, value in record.items():
                # Check if field is readable
                if field in field_permissions and field_permissions[field].value >= PermissionLevel.READ.value:
                    redacted_record[field] = value
            
            redacted_results.append(redacted_record)
        
        return redacted_results
    
    def _extract_tables(self, stmt) -> List[str]:
        """
        Extract table names from a SQL statement.
        
        Args:
            stmt: The SQL statement.
            
        Returns:
            A list of table names.
        """
        tables = []
        
        # Find FROM and JOIN clauses
        from_seen = False
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'FROM':
                from_seen = True
                continue
            
            if from_seen and token.ttype is None:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        tables.append(str(identifier).strip('`"'))
                elif isinstance(token, Identifier):
                    tables.append(str(token).strip('`"'))
                from_seen = False
            
            if token.is_keyword and token.value.upper() == 'JOIN':
                for join_token in token.tokens:
                    if isinstance(join_token, Identifier):
                        tables.append(str(join_token).strip('`"'))
        
        return tables
    
    def _filter_to_sql(self, filter_dict: Dict[str, Any], table_name: str) -> str:
        """
        Convert a filter dictionary to a SQL WHERE clause.
        
        Args:
            filter_dict: The filter dictionary.
            table_name: The table name.
            
        Returns:
            The SQL WHERE clause.
        """
        # Simple implementation for common operators
        clauses = []
        
        for key, value in filter_dict.items():
            if key == "$or":
                or_clauses = []
                for sub_filter in value:
                    sub_clause = self._filter_to_sql(sub_filter, table_name)
                    if sub_clause:
                        or_clauses.append(f"({sub_clause})")
                
                if or_clauses:
                    clauses.append(f"({' OR '.join(or_clauses)})")
            
            elif key == "$and":
                and_clauses = []
                for sub_filter in value:
                    sub_clause = self._filter_to_sql(sub_filter, table_name)
                    if sub_clause:
                        and_clauses.append(f"({sub_clause})")
                
                if and_clauses:
                    clauses.append(f"({' AND '.join(and_clauses)})")
            
            elif key.startswith("$"):
                # Unsupported operator
                logger.warning(f"Unsupported filter operator: {key}")
            
            else:
                # Field comparison
                field = f"{table_name}.{key}"
                
                if isinstance(value, dict):
                    # Operator comparison
                    for op, op_value in value.items():
                        if op == "$eq":
                            clauses.append(f"{field} = {self._sql_value(op_value)}")
                        elif op == "$ne":
                            clauses.append(f"{field} != {self._sql_value(op_value)}")
                        elif op == "$gt":
                            clauses.append(f"{field} > {self._sql_value(op_value)}")
                        elif op == "$gte":
                            clauses.append(f"{field} >= {self._sql_value(op_value)}")
                        elif op == "$lt":
                            clauses.append(f"{field} < {self._sql_value(op_value)}")
                        elif op == "$lte":
                            clauses.append(f"{field} <= {self._sql_value(op_value)}")
                        elif op == "$in":
                            values = [self._sql_value(v) for v in op_value]
                            clauses.append(f"{field} IN ({', '.join(values)})")
                        elif op == "$nin":
                            values = [self._sql_value(v) for v in op_value]
                            clauses.append(f"{field} NOT IN ({', '.join(values)})")
                        else:
                            logger.warning(f"Unsupported comparison operator: {op}")
                else:
                    # Equality comparison
                    clauses.append(f"{field} = {self._sql_value(value)}")
        
        return " AND ".join(clauses)
    
    def _sql_value(self, value: Any) -> str:
        """
        Convert a value to a SQL literal.
        
        Args:
            value: The value to convert.
            
        Returns:
            The SQL literal.
        """
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # Escape single quotes
            return f"'{str(value).replace('\'', '\'\'')}'"
    
    def _add_where_clause(self, query: str, where_clause: str) -> str:
        """
        Add a WHERE clause to a SQL query.
        
        Args:
            query: The SQL query.
            where_clause: The WHERE clause to add.
            
        Returns:
            The modified SQL query.
        """
        # Parse the query
        parsed = sqlparse.parse(query)
        if not parsed:
            return query
        
        stmt = parsed[0]
        
        # Check if the query already has a WHERE clause
        where_found = False
        for token in stmt.tokens:
            if isinstance(token, Where):
                where_found = True
                # Add the new condition to the existing WHERE clause
                token_str = str(token)
                where_index = token_str.upper().find("WHERE")
                if where_index >= 0:
                    new_where = f"{token_str[:where_index + 5]} ({token_str[where_index + 5:].strip()}) AND ({where_clause})"
                    return query.replace(token_str, new_where)
        
        # If no WHERE clause found, add one
        if not where_found:
            # Find the position to insert the WHERE clause
            match = re.search(r'\bFROM\b.*?(?:\bWHERE\b|\bGROUP BY\b|\bHAVING\b|\bORDER BY\b|\bLIMIT\b|$)', query, re.IGNORECASE | re.DOTALL)
            if match:
                from_clause = match.group(0)
                if re.search(r'\bWHERE\b', from_clause, re.IGNORECASE):
                    # Should not happen, but just in case
                    return query.replace(from_clause, f"{from_clause} AND ({where_clause})")
                else:
                    # Insert WHERE clause after FROM clause
                    return query.replace(from_clause, f"{from_clause} WHERE {where_clause}")
        
        # Fallback: append WHERE clause to the query
        return f"{query} WHERE {where_clause}"
    
    def _apply_field_permissions(self, user_id: str, query: str, 
                               data_source_id: str, tables: List[str]) -> str:
        """
        Apply field-level permissions to a SQL query.
        
        Args:
            user_id: The ID of the user.
            query: The SQL query.
            data_source_id: The ID of the data source.
            tables: The tables referenced in the query.
            
        Returns:
            The modified SQL query.
        """
        # Parse the query
        parsed = sqlparse.parse(query)
        if not parsed:
            return query
        
        stmt = parsed[0]
        
        # Check if it's a SELECT query
        if stmt.get_type() != 'SELECT':
            return query
        
        # Get all field permissions for all tables
        table_field_permissions = {}
        for table_name in tables:
            table_id = f"{data_source_id}.{table_name}"
            field_permissions = self.permission_evaluator.get_field_permissions(user_id, table_id)
            table_field_permissions[table_name] = field_permissions
        
        # Extract the SELECT fields
        select_fields = []
        select_seen = False
        from_seen = False
        
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'SELECT':
                select_seen = True
                continue
            
            if select_seen and not from_seen:
                if token.is_keyword and token.value.upper() == 'FROM':
                    from_seen = True
                    continue
                
                if token.ttype is None:
                    if isinstance(token, IdentifierList):
                        for identifier in token.get_identifiers():
                            select_fields.append(str(identifier).strip())
                    elif isinstance(token, Identifier):
                        select_fields.append(str(token).strip())
        
        # Check if it's a SELECT * query
        if not select_fields or any(field == '*' for field in select_fields):
            # Replace with explicit field list based on permissions
            allowed_fields = []
            
            for table_name, field_permissions in table_field_permissions.items():
                for field_id, level in field_permissions.items():
                    if level.value >= PermissionLevel.READ.value:
                        allowed_fields.append(f"{table_name}.{field_id}")
            
            if allowed_fields:
                # Replace SELECT * with SELECT field1, field2, ...
                return re.sub(
                    r'SELECT\s+\*',
                    f"SELECT {', '.join(allowed_fields)}",
                    query,
                    flags=re.IGNORECASE
                )
        
        # Check permissions for explicitly selected fields
        for field in select_fields:
            if '.' in field:
                # Field with table qualifier
                table_name, field_name = field.split('.', 1)
                table_name = table_name.strip('`"')
                field_name = field_name.strip('`"')
                
                if table_name in table_field_permissions:
                    field_perms = table_field_permissions[table_name]
                    if field_name not in field_perms or field_perms[field_name].value < PermissionLevel.READ.value:
                        raise PermissionDeniedError(
                            user_id, 
                            ResourceType.FIELD, 
                            f"{data_source_id}.{table_name}.{field_name}", 
                            PermissionLevel.READ
                        )
        
        return query 