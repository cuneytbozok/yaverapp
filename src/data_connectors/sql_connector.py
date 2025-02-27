"""
SQL database connector for the data source connector system.
"""

import logging
import time
import re
from typing import Dict, List, Any, Optional, Union, Tuple
import json

from .core import (
    DataSourceConnector, ConnectorConfig, Schema, SchemaElement, QueryResult, ConnectorType
)

logger = logging.getLogger(__name__)

class SQLConnector(DataSourceConnector):
    """
    Connector for SQL databases.
    """
    
    def __init__(self, config: ConnectorConfig):
        """
        Initialize the SQL connector.
        
        Args:
            config: The connector configuration.
        """
        super().__init__(config)
        
        # Extract database-specific configuration
        self.db_type = self.config.connection_params.get("db_type", "sqlite").lower()
        self.host = self.config.connection_params.get("host", "localhost")
        self.port = self.config.connection_params.get("port")
        self.database = self.config.connection_params.get("database")
        self.username = self.config.auth_params.get("username")
        self.password = self.config.auth_params.get("password")
        
        # Set default port based on database type if not specified
        if self.port is None:
            if self.db_type == "mysql":
                self.port = 3306
            elif self.db_type == "postgresql":
                self.port = 5432
            elif self.db_type == "mssql":
                self.port = 1433
            elif self.db_type == "oracle":
                self.port = 1521
        
        # Initialize connection
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Connect to the SQL database.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            if self.db_type == "sqlite":
                import sqlite3
                self.connection = sqlite3.connect(self.database)
                self.cursor = self.connection.cursor()
                
            elif self.db_type == "mysql":
                try:
                    import mysql.connector
                    self.connection = mysql.connector.connect(
                        host=self.host,
                        port=self.port,
                        database=self.database,
                        user=self.username,
                        password=self.password
                    )
                    self.cursor = self.connection.cursor(dictionary=True)
                except ImportError:
                    logger.error("mysql-connector-python not installed. Please install it with: pip install mysql-connector-python")
                    return False
                
            elif self.db_type == "postgresql":
                try:
                    import psycopg2
                    import psycopg2.extras
                    self.connection = psycopg2.connect(
                        host=self.host,
                        port=self.port,
                        dbname=self.database,
                        user=self.username,
                        password=self.password
                    )
                    self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                except ImportError:
                    logger.error("psycopg2 not installed. Please install it with: pip install psycopg2-binary")
                    return False
                
            elif self.db_type == "mssql":
                try:
                    import pyodbc
                    self.connection = pyodbc.connect(
                        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.host},{self.port};DATABASE={self.database};UID={self.username};PWD={self.password}"
                    )
                    self.cursor = self.connection.cursor()
                except ImportError:
                    logger.error("pyodbc not installed. Please install it with: pip install pyodbc")
                    return False
                
            else:
                logger.error(f"Unsupported database type: {self.db_type}")
                return False
            
            self.is_connected = True
            logger.info(f"Connected to {self.db_type} database: {self.database}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to {self.db_type} database: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from the SQL database.
        
        Returns:
            True if the disconnection is successful, False otherwise.
        """
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.cursor = None
                self.is_connected = False
                logger.info(f"Disconnected from {self.db_type} database: {self.database}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error disconnecting from {self.db_type} database: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test the connection to the SQL database.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            if not self.is_connected:
                success = self.connect()
                if not success:
                    return False
            
            # Try a simple query
            self.cursor.execute("SELECT 1")
            
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def discover_schema(self) -> Schema:
        """
        Discover the schema of the SQL database.
        
        Returns:
            The schema of the database.
        """
        try:
            if not self.is_connected:
                success = self.connect()
                if not success:
                    return Schema()
            
            schema = Schema()
            
            # Get tables
            tables = self._get_tables()
            
            # Get columns for each table
            for table in tables:
                columns = self._get_columns(table)
                
                for column in columns:
                    element = SchemaElement(
                        name=f"{table}.{column['name']}",
                        data_type=column["data_type"],
                        is_nullable=column["is_nullable"],
                        is_primary_key=column["is_primary_key"]
                    )
                    
                    schema.elements[element.name] = element
            
            # Get relationships
            schema.relationships = self._get_relationships()
            
            # Add metadata
            schema.metadata = {
                "db_type": self.db_type,
                "database": self.database,
                "tables": tables
            }
            
            return schema
            
        except Exception as e:
            logger.error(f"Error discovering schema: {e}")
            return Schema()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """
        Execute a query against the SQL database.
        
        Args:
            query: The SQL query to execute.
            params: Optional parameters for the query.
            
        Returns:
            The result of the query.
        """
        try:
            if not self.is_connected:
                success = self.connect()
                if not success:
                    return QueryResult(
                        data=None,
                        error="Not connected to database",
                        execution_time=0.0
                    )
            
            start_time = time.time()
            
            # Execute the query
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            # Get the results
            if query.strip().upper().startswith(("SELECT", "SHOW", "DESCRIBE", "EXPLAIN")):
                # For queries that return data
                columns = [desc[0] for desc in self.cursor.description]
                
                if self.db_type in ["mysql", "postgresql"]:
                    # These drivers can return dictionaries directly
                    data = self.cursor.fetchall()
                else:
                    # Convert to list of dictionaries
                    rows = self.cursor.fetchall()
                    data = []
                    for row in rows:
                        data.append(dict(zip(columns, row)))
                
                # Create a simple schema for the result
                result_schema = Schema()
                for col in columns:
                    result_schema.elements[col] = SchemaElement(
                        name=col,
                        data_type="unknown"  # We don't have type information for result columns
                    )
                
            else:
                # For queries that don't return data (INSERT, UPDATE, DELETE)
                self.connection.commit()
                data = {
                    "affected_rows": self.cursor.rowcount
                }
                result_schema = None
            
            execution_time = time.time() - start_time
            
            return QueryResult(
                data=data,
                schema=result_schema,
                metadata={
                    "query": query,
                    "params": params
                },
                execution_time=execution_time
            )
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            
            return QueryResult(
                data=None,
                error=str(e),
                execution_time=time.time() - start_time if 'start_time' in locals() else 0.0
            )
    
    def translate_query(self, natural_language_query: str) -> str:
        """
        Translate a natural language query to SQL.
        
        Args:
            natural_language_query: The natural language query.
            
        Returns:
            The translated SQL query.
        """
        # In a real implementation, this would use an LLM to translate
        # For now, we'll implement a simple rule-based approach for demonstration
        
        query = natural_language_query.lower()
        
        # Get the schema to help with translation
        schema = self.get_schema()
        tables = schema.metadata.get("tables", [])
        
        if not tables:
            raise ValueError("No tables found in database schema")
        
        # Simple pattern matching for common query types
        if "count" in query and "where" in query:
            # Count query with condition
            for table in tables:
                if table.lower() in query:
                    condition = query.split("where")[1].strip()
                    # Very simplistic condition parsing
                    condition = self._parse_condition(condition, table)
                    return f"SELECT COUNT(*) FROM {table} WHERE {condition}"
        
        elif "count" in query:
            # Simple count query
            for table in tables:
                if table.lower() in query:
                    return f"SELECT COUNT(*) FROM {table}"
        
        elif "average" in query or "avg" in query:
            # Average query
            for table in tables:
                if table.lower() in query:
                    # Try to find the column to average
                    columns = [col.split(".")[1] for col in schema.elements.keys() if col.startswith(f"{table}.")]
                    for col in columns:
                        if col.lower() in query:
                            return f"SELECT AVG({col}) FROM {table}"
        
        elif "sum" in query:
            # Sum query
            for table in tables:
                if table.lower() in query:
                    # Try to find the column to sum
                    columns = [col.split(".")[1] for col in schema.elements.keys() if col.startswith(f"{table}.")]
                    for col in columns:
                        if col.lower() in query:
                            return f"SELECT SUM({col}) FROM {table}"
        
        elif "select" in query or "show" in query or "get" in query or "find" in query:
            # Select query
            for table in tables:
                if table.lower() in query:
                    # Try to find columns
                    columns = [col.split(".")[1] for col in schema.elements.keys() if col.startswith(f"{table}.")]
                    selected_columns = []
                    
                    for col in columns:
                        if col.lower() in query:
                            selected_columns.append(col)
                    
                    if not selected_columns:
                        selected_columns = ["*"]
                    
                    # Check for conditions
                    if "where" in query:
                        condition = query.split("where")[1].strip()
                        condition = self._parse_condition(condition, table)
                        return f"SELECT {', '.join(selected_columns)} FROM {table} WHERE {condition}"
                    else:
                        return f"SELECT {', '.join(selected_columns)} FROM {table}"
        
        # Default to a simple select all query for the first table
        return f"SELECT * FROM {tables[0]} LIMIT 10"
    
    def _get_tables(self) -> List[str]:
        """
        Get the tables in the database.
        
        Returns:
            A list of table names.
        """
        try:
            tables = []
            
            if self.db_type == "sqlite":
                self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                for row in self.cursor.fetchall():
                    tables.append(row[0])
                
            elif self.db_type == "mysql":
                self.cursor.execute("SHOW TABLES")
                for row in self.cursor.fetchall():
                    tables.append(list(row.values())[0])
                
            elif self.db_type == "postgresql":
                self.cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                for row in self.cursor.fetchall():
                    tables.append(row[0])
                
            elif self.db_type == "mssql":
                self.cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
                for row in self.cursor.fetchall():
                    tables.append(row[0])
            
            return tables
            
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
    
    def _get_columns(self, table: str) -> List[Dict[str, Any]]:
        """
        Get the columns of a table.
        
        Args:
            table: The table name.
            
        Returns:
            A list of column dictionaries.
        """
        try:
            columns = []
            
            if self.db_type == "sqlite":
                self.cursor.execute(f"PRAGMA table_info({table})")
                for row in self.cursor.fetchall():
                    columns.append({
                        "name": row[1],
                        "data_type": row[2],
                        "is_nullable": not row[3],  # NOT NULL constraint
                        "is_primary_key": bool(row[5])  # Primary key
                    })
                
            elif self.db_type == "mysql":
                self.cursor.execute(f"DESCRIBE {table}")
                for row in self.cursor.fetchall():
                    columns.append({
                        "name": row["Field"],
                        "data_type": row["Type"],
                        "is_nullable": row["Null"] == "YES",
                        "is_primary_key": row["Key"] == "PRI"
                    })
                
            elif self.db_type == "postgresql":
                self.cursor.execute(f"""
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable,
                        (
                            SELECT 
                                COUNT(*)
                            FROM 
                                information_schema.table_constraints tc
                                JOIN information_schema.key_column_usage kcu
                                ON tc.constraint_name = kcu.constraint_name
                                AND tc.table_schema = kcu.table_schema
                                AND tc.table_name = kcu.table_name
                            WHERE 
                                tc.constraint_type = 'PRIMARY KEY'
                                AND tc.table_name = '{table}'
                                AND kcu.column_name = c.column_name
                        ) as is_primary_key
                    FROM 
                        information_schema.columns c
                    WHERE 
                        table_name = '{table}'
                """)
                
                for row in self.cursor.fetchall():
                    columns.append({
                        "name": row[0],
                        "data_type": row[1],
                        "is_nullable": row[2] == "YES",
                        "is_primary_key": row[3] > 0
                    })
                
            elif self.db_type == "mssql":
                self.cursor.execute(f"""
                    SELECT 
                        c.COLUMN_NAME, 
                        c.DATA_TYPE, 
                        c.IS_NULLABLE,
                        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
                    FROM 
                        INFORMATION_SCHEMA.COLUMNS c
                        LEFT JOIN (
                            SELECT 
                                ku.TABLE_CATALOG,
                                ku.TABLE_SCHEMA,
                                ku.TABLE_NAME,
                                ku.COLUMN_NAME
                            FROM 
                                INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                        ) pk
                        ON c.TABLE_CATALOG = pk.TABLE_CATALOG
                        AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                        AND c.TABLE_NAME = pk.TABLE_NAME
                        AND c.COLUMN_NAME = pk.COLUMN_NAME
                    WHERE 
                        c.TABLE_NAME = '{table}'
                """)
                
                for row in self.cursor.fetchall():
                    columns.append({
                        "name": row[0],
                        "data_type": row[1],
                        "is_nullable": row[2] == "YES",
                        "is_primary_key": bool(row[3])
                    })
            
            return columns
            
        except Exception as e:
            logger.error(f"Error getting columns for table {table}: {e}")
            return []
    
    def _get_relationships(self) -> List[Dict[str, Any]]:
        """
        Get the relationships between tables.
        
        Returns:
            A list of relationship dictionaries.
        """
        try:
            relationships = []
            
            if self.db_type == "sqlite":
                # SQLite doesn't store foreign key information in system tables
                # We would need to parse the table creation SQL
                pass
                
            elif self.db_type == "mysql":
                self.cursor.execute("""
                    SELECT 
                        TABLE_NAME,
                        COLUMN_NAME,
                        REFERENCED_TABLE_NAME,
                        REFERENCED_COLUMN_NAME
                    FROM
                        INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE
                        REFERENCED_TABLE_SCHEMA = DATABASE()
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                """)
                
                for row in self.cursor.fetchall():
                    relationships.append({
                        "table": row["TABLE_NAME"],
                        "column": row["COLUMN_NAME"],
                        "referenced_table": row["REFERENCED_TABLE_NAME"],
                        "referenced_column": row["REFERENCED_COLUMN_NAME"]
                    })
                
            elif self.db_type == "postgresql":
                self.cursor.execute("""
                    SELECT
                        tc.table_name, 
                        kcu.column_name, 
                        ccu.table_name AS referenced_table,
                        ccu.column_name AS referenced_column
                    FROM 
                        information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu 
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                """)
                
                for row in self.cursor.fetchall():
                    relationships.append({
                        "table": row[0],
                        "column": row[1],
                        "referenced_table": row[2],
                        "referenced_column": row[3]
                    })
                
            elif self.db_type == "mssql":
                self.cursor.execute("""
                    SELECT 
                        fk.name AS FK_NAME,
                        tp.name AS PARENT_TABLE,
                        cp.name AS PARENT_COLUMN,
                        tr.name AS REFERENCED_TABLE,
                        cr.name AS REFERENCED_COLUMN
                    FROM 
                        sys.foreign_keys fk
                        INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
                        INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
                        INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                        INNER JOIN sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
                        INNER JOIN sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
                """)
                
                for row in self.cursor.fetchall():
                    relationships.append({
                        "table": row[1],
                        "column": row[2],
                        "referenced_table": row[3],
                        "referenced_column": row[4]
                    })
            
            return relationships
            
        except Exception as e:
            logger.error(f"Error getting relationships: {e}")
            return []
    
    def _parse_condition(self, condition: str, table: str) -> str:
        """
        Parse a natural language condition into SQL.
        
        Args:
            condition: The natural language condition.
            table: The table name.
            
        Returns:
            The SQL condition.
        """
        # This is a very simplistic implementation
        # In a real system, you would use an LLM or more sophisticated NLP
        
        # Get columns for the table
        columns = self._get_column_names(table)
        
        # Look for column names in the condition
        for col in columns:
            if col.lower() in condition:
                # Look for comparison operators
                if "greater than" in condition or "more than" in condition:
                    value = re.search(r'greater than (\d+)|more than (\d+)', condition)
                    if value:
                        value = value.group(1) or value.group(2)
                        return f"{col} > {value}"
                
                elif "less than" in condition or "fewer than" in condition:
                    value = re.search(r'less than (\d+)|fewer than (\d+)', condition)
                    if value:
                        value = value.group(1) or value.group(2)
                        return f"{col} < {value}"
                
                elif "equal to" in condition or "equals" in condition or "is" in condition:
                    # Check for numeric values
                    value = re.search(r'equal to (\d+)|equals (\d+)|is (\d+)', condition)
                    if value:
                        value = value.group(1) or value.group(2) or value.group(3)
                        return f"{col} = {value}"
                    
                    # Check for string values
                    value = re.search(r'equal to [\'"](.+?)[\'"]|equals [\'"](.+?)[\'"]|is [\'"](.+?)[\'"]', condition)
                    if value:
                        value = value.group(1) or value.group(2) or value.group(3)
                        return f"{col} = '{value}'"
                
                elif "contains" in condition or "like" in condition:
                    value = re.search(r'contains [\'"](.+?)[\'"]|like [\'"](.+?)[\'"]', condition)
                    if value:
                        value = value.group(1) or value.group(2)
                        return f"{col} LIKE '%{value}%'"
        
        # If we couldn't parse the condition, return it as is
        return condition
    
    def _get_column_names(self, table: str) -> List[str]:
        """
        Get the column names of a table.
        
        Args:
            table: The table name.
            
        Returns:
            A list of column names.
        """
        columns = self._get_columns(table)
        return [column["name"] for column in columns] 