"""
Database connector for SQL databases.
"""

import logging
import sqlite3
from typing import Dict, Any, List, Optional
import json
from ..connectors.base import DataSourceConnector

logger = logging.getLogger(__name__)

class SQLDatabaseConnector(DataSourceConnector):
    """
    Connector for SQL databases.
    """
    
    def __init__(self, connection_details: Dict[str, Any]):
        """
        Initialize the SQL database connector.
        
        Args:
            connection_details: A dictionary containing connection details.
                Required keys depend on the database type:
                - 'type': The database type (sqlite, mysql, postgresql, etc.)
                - For SQLite: 'database': Path to the database file
                - For other databases: 'host', 'port', 'database', 'user', 'password'
        """
        self.connection_details = connection_details
        self.connection = None
        self.db_type = connection_details.get('type', 'sqlite').lower()
        
        # Connect to the database
        self._connect()
    
    def _connect(self):
        """
        Connect to the database.
        """
        try:
            if self.db_type == 'sqlite':
                self.connection = sqlite3.connect(self.connection_details['database'])
                # Enable dictionary cursor
                self.connection.row_factory = sqlite3.Row
            else:
                # For other database types, we would use the appropriate driver
                # This is a placeholder for MySQL, PostgreSQL, etc.
                logger.error(f"Database type {self.db_type} not implemented")
                raise NotImplementedError(f"Database type {self.db_type} not implemented")
                
            logger.info(f"Connected to {self.db_type} database")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def execute(self, query_type: str, parameters: Dict[str, Any]) -> Any:
        """
        Execute a query against the database.
        
        Args:
            query_type: The type of query to execute (sql_select, sql_aggregate, etc.)
            parameters: The parameters for the query.
            
        Returns:
            The query results.
        """
        logger.info(f"Executing {query_type} query")
        
        if not self.connection:
            self._connect()
        
        try:
            if query_type == 'sql_select':
                return self._execute_select(parameters)
            elif query_type == 'sql_aggregate':
                return self._execute_aggregate(parameters)
            elif query_type == 'sql_compare':
                return self._execute_compare(parameters)
            elif query_type == 'sql_summary':
                return self._execute_summary(parameters)
            else:
                logger.warning(f"Unknown query type: {query_type}")
                raise ValueError(f"Unknown query type: {query_type}")
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def _execute_select(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query.
        
        Args:
            parameters: The query parameters.
                Required keys:
                - 'table': The table to query
                - 'select': The columns to select (list or '*')
                - 'filters': Optional filters to apply
                - 'limit': Optional limit on the number of results
                
        Returns:
            A list of dictionaries representing the rows.
        """
        # Extract parameters
        table = parameters.get('table')
        if not table:
            raise ValueError("Table name is required for SELECT queries")
        
        select = parameters.get('select', '*')
        if isinstance(select, list):
            select = ', '.join(select)
        
        # Build the query
        query = f"SELECT {select} FROM {table}"
        
        # Add filters if provided
        filters = parameters.get('filters', {})
        if filters:
            where_clauses = []
            params = []
            
            for key, value in filters.items():
                if isinstance(value, dict):
                    # Handle range filters
                    if 'min' in value:
                        where_clauses.append(f"{key} >= ?")
                        params.append(value['min'])
                    if 'max' in value:
                        where_clauses.append(f"{key} <= ?")
                        params.append(value['max'])
                    if 'exact' in value:
                        where_clauses.append(f"{key} = ?")
                        params.append(value['exact'])
                else:
                    # Simple equality filter
                    where_clauses.append(f"{key} = ?")
                    params.append(value)
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
        else:
            params = []
        
        # Add limit if provided
        limit = parameters.get('limit')
        if limit:
            query += f" LIMIT {int(limit)}"
        
        # Execute the query
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        
        # Convert rows to dictionaries
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))
        
        return results
    
    def _execute_aggregate(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute an aggregate query (COUNT, SUM, AVG, etc.).
        
        Args:
            parameters: The query parameters.
                Required keys:
                - 'table': The table to query
                - 'aggregate_function': The aggregate function to use
                - 'column': The column to aggregate
                - 'filters': Optional filters to apply
                - 'group_by': Optional grouping
                
        Returns:
            A dictionary with the aggregate results.
        """
        # Extract parameters
        table = parameters.get('table')
        if not table:
            raise ValueError("Table name is required for aggregate queries")
        
        agg_function = parameters.get('aggregate_function')
        if not agg_function:
            raise ValueError("Aggregate function is required")
        
        column = parameters.get('column')
        if not column:
            raise ValueError("Column is required for aggregate queries")
        
        # Build the query
        query = f"SELECT {agg_function}({column}) as result FROM {table}"
        
        # Add filters if provided
        filters = parameters.get('filters', {})
        if filters:
            where_clauses = []
            params = []
            
            for key, value in filters.items():
                if isinstance(value, dict):
                    # Handle range filters
                    if 'min' in value:
                        where_clauses.append(f"{key} >= ?")
                        params.append(value['min'])
                    if 'max' in value:
                        where_clauses.append(f"{key} <= ?")
                        params.append(value['max'])
                    if 'exact' in value:
                        where_clauses.append(f"{key} = ?")
                        params.append(value['exact'])
                else:
                    # Simple equality filter
                    where_clauses.append(f"{key} = ?")
                    params.append(value)
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
        else:
            params = []
        
        # Add group by if provided
        group_by = parameters.get('group_by')
        if group_by:
            if isinstance(group_by, list):
                group_by = ', '.join(group_by)
            query += f" GROUP BY {group_by}"
            
            # If grouping, we need to select the group columns too
            query = query.replace(f"SELECT {agg_function}({column}) as result", 
                                 f"SELECT {group_by}, {agg_function}({column}) as result")
        
        # Execute the query
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        
        # Get the results
        if group_by:
            # If grouped, return a list of dictionaries
            results = []
            for row in cursor.fetchall():
                results.append(dict(row))
            return {"grouped_results": results}
        else:
            # If not grouped, return a single value
            row = cursor.fetchone()
            if row:
                return {"result": row['result']}
            else:
                return {"result": None}
    
    def _execute_compare(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a comparison query between two sets of data.
        
        Args:
            parameters: The query parameters.
                Required keys:
                - 'table': The table to query
                - 'compare_column': The column to compare
                - 'compare_groups': The groups to compare (list of filters)
                - 'aggregate_function': Optional aggregate function
                
        Returns:
            A dictionary with the comparison results.
        """
        # Extract parameters
        table = parameters.get('table')
        if not table:
            raise ValueError("Table name is required for comparison queries")
        
        compare_column = parameters.get('compare_column')
        if not compare_column:
            raise ValueError("Compare column is required")
        
        compare_groups = parameters.get('compare_groups')
        if not compare_groups or not isinstance(compare_groups, list):
            raise ValueError("Compare groups must be a list")
        
        agg_function = parameters.get('aggregate_function', 'AVG')
        
        # Execute a query for each group
        results = {}
        
        for i, group_filters in enumerate(compare_groups):
            # Build the query
            query = f"SELECT {agg_function}({compare_column}) as result FROM {table}"
            
            # Add filters
            if group_filters:
                where_clauses = []
                params = []
                
                for key, value in group_filters.items():
                    if isinstance(value, dict):
                        # Handle range filters
                        if 'min' in value:
                            where_clauses.append(f"{key} >= ?")
                            params.append(value['min'])
                        if 'max' in value:
                            where_clauses.append(f"{key} <= ?")
                            params.append(value['max'])
                        if 'exact' in value:
                            where_clauses.append(f"{key} = ?")
                            params.append(value['exact'])
                    else:
                        # Simple equality filter
                        where_clauses.append(f"{key} = ?")
                        params.append(value)
                
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
            else:
                params = []
            
            # Execute the query
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            # Get the result
            row = cursor.fetchone()
            if row:
                group_name = f"Group {i+1}"
                if 'name' in group_filters:
                    group_name = group_filters['name']
                results[group_name] = row['result']
            else:
                results[f"Group {i+1}"] = None
        
        return results
    
    def _execute_summary(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a summary query that provides an overview of the data.
        
        Args:
            parameters: The query parameters.
                Required keys:
                - 'table': The table to query
                - 'columns': The columns to summarize (list)
                - 'filters': Optional filters to apply
                
        Returns:
            A dictionary with summary statistics.
        """
        # Extract parameters
        table = parameters.get('table')
        if not table:
            raise ValueError("Table name is required for summary queries")
        
        columns = parameters.get('columns')
        if not columns or not isinstance(columns, list):
            raise ValueError("Columns must be a list")
        
        # Build a query to get basic statistics for each column
        summary = {}
        
        for column in columns:
            # Check if the column is numeric
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT typeof({column}) as type FROM {table} LIMIT 1")
            row = cursor.fetchone()
            
            if not row:
                continue
                
            column_type = row['type'].lower()
            
            if column_type in ('integer', 'real', 'float', 'double', 'numeric'):
                # For numeric columns, get min, max, avg, sum
                query = f"""
                SELECT 
                    MIN({column}) as min_value,
                    MAX({column}) as max_value,
                    AVG({column}) as avg_value,
                    SUM({column}) as sum_value,
                    COUNT({column}) as count_value
                FROM {table}
                """
                
                # Add filters if provided
                filters = parameters.get('filters', {})
                if filters:
                    where_clauses = []
                    params = []
                    
                    for key, value in filters.items():
                        if isinstance(value, dict):
                            # Handle range filters
                            if 'min' in value:
                                where_clauses.append(f"{key} >= ?")
                                params.append(value['min'])
                            if 'max' in value:
                                where_clauses.append(f"{key} <= ?")
                                params.append(value['max'])
                            if 'exact' in value:
                                where_clauses.append(f"{key} = ?")
                                params.append(value['exact'])
                        else:
                            # Simple equality filter
                            where_clauses.append(f"{key} = ?")
                            params.append(value)
                    
                    if where_clauses:
                        query += " WHERE " + " AND ".join(where_clauses)
                else:
                    params = []
                
                cursor.execute(query, params)
                row = cursor.fetchone()
                
                if row:
                    summary[column] = {
                        'type': 'numeric',
                        'min': row['min_value'],
                        'max': row['max_value'],
                        'avg': row['avg_value'],
                        'sum': row['sum_value'],
                        'count': row['count_value']
                    }
            else:
                # For non-numeric columns, get count and distinct count
                query = f"""
                SELECT 
                    COUNT({column}) as count_value,
                    COUNT(DISTINCT {column}) as distinct_count
                FROM {table}
                """
                
                # Add filters if provided
                filters = parameters.get('filters', {})
                if filters:
                    where_clauses = []
                    params = []
                    
                    for key, value in filters.items():
                        if isinstance(value, dict):
                            # Handle range filters
                            if 'min' in value:
                                where_clauses.append(f"{key} >= ?")
                                params.append(value['min'])
                            if 'max' in value:
                                where_clauses.append(f"{key} <= ?")
                                params.append(value['max'])
                            if 'exact' in value:
                                where_clauses.append(f"{key} = ?")
                                params.append(value['exact'])
                        else:
                            # Simple equality filter
                            where_clauses.append(f"{key} = ?")
                            params.append(value)
                    
                    if where_clauses:
                        query += " WHERE " + " AND ".join(where_clauses)
                else:
                    params = []
                
                cursor.execute(query, params)
                row = cursor.fetchone()
                
                if row:
                    summary[column] = {
                        'type': 'text',
                        'count': row['count_value'],
                        'distinct_count': row['distinct_count']
                    }
                    
                    # Get most common values (top 5)
                    query = f"""
                    SELECT {column}, COUNT(*) as count
                    FROM {table}
                    GROUP BY {column}
                    ORDER BY count DESC
                    LIMIT 5
                    """
                    
                    cursor.execute(query)
                    common_values = []
                    
                    for row in cursor.fetchall():
                        common_values.append({
                            'value': row[column],
                            'count': row['count']
                        })
                    
                    summary[column]['common_values'] = common_values
        
        return summary
    
    def test_connection(self) -> bool:
        """
        Test the connection to the database.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            if not self.connection:
                self._connect()
            
            # Try a simple query
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def __del__(self):
        """
        Close the connection when the object is destroyed.
        """
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}") 