"""
File system connector for the data source connector system.
"""

import logging
import time
import os
import json
import csv
import re
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
import shutil

from .core import (
    DataSourceConnector, ConnectorConfig, Schema, SchemaElement, QueryResult, ConnectorType
)

logger = logging.getLogger(__name__)

class FileSystemConnector(DataSourceConnector):
    """
    Connector for file systems.
    """
    
    def __init__(self, config: ConnectorConfig):
        """
        Initialize the file system connector.
        
        Args:
            config: The connector configuration.
        """
        super().__init__(config)
        
        # Extract file system-specific configuration
        self.root_path = self.config.connection_params.get("root_path")
        self.file_types = self.config.connection_params.get("file_types", ["csv", "json", "txt"])
        self.encoding = self.config.connection_params.get("encoding", "utf-8")
        self.csv_delimiter = self.config.connection_params.get("csv_delimiter", ",")
        self.csv_quotechar = self.config.connection_params.get("csv_quotechar", '"')
        
        # Initialize connection
        self.connection = None
    
    def connect(self) -> bool:
        """
        Connect to the file system.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            # For file system, just check if the path exists
            if not os.path.isdir(self.root_path):
                logger.error(f"File system path does not exist: {self.root_path}")
                return False
            
            self.connection = {"path": self.root_path}
            self.is_connected = True
            logger.info(f"Connected to file system: {self.root_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to file system: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from the file system.
        
        Returns:
            True if the disconnection is successful, False otherwise.
        """
        try:
            self.connection = None
            self.is_connected = False
            logger.info(f"Disconnected from file system: {self.root_path}")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from file system: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test the connection to the file system.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            return os.path.isdir(self.root_path) and os.access(self.root_path, os.R_OK)
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def discover_schema(self) -> Schema:
        """
        Discover the schema of the file system.
        
        Returns:
            The schema of the file system.
        """
        schema = Schema()
        
        try:
            # For file systems, we'll create a schema based on the directory structure
            # and sample files
            
            # First, add basic file metadata elements
            schema.elements["path"] = SchemaElement(
                name="path",
                data_type="string",
                description="Relative path to the file"
            )
            
            schema.elements["name"] = SchemaElement(
                name="name",
                data_type="string",
                description="File name"
            )
            
            schema.elements["extension"] = SchemaElement(
                name="extension",
                data_type="string",
                description="File extension"
            )
            
            schema.elements["size"] = SchemaElement(
                name="size",
                data_type="integer",
                description="File size in bytes"
            )
            
            schema.elements["created_at"] = SchemaElement(
                name="created_at",
                data_type="datetime",
                description="File creation time"
            )
            
            schema.elements["modified_at"] = SchemaElement(
                name="modified_at",
                data_type="datetime",
                description="File modification time"
            )
            
            # Try to infer schema from sample files
            sample_files = self._get_sample_files()
            
            for file_path in sample_files:
                file_extension = os.path.splitext(file_path)[1].lower()[1:]
                
                if file_extension == "csv":
                    # Try to infer schema from CSV file
                    try:
                        with open(file_path, 'r', encoding=self.encoding) as f:
                            reader = csv.DictReader(f, delimiter=self.csv_delimiter, quotechar=self.csv_quotechar)
                            headers = reader.fieldnames
                            
                            if headers:
                                # Read a few rows to infer types
                                rows = []
                                for i, row in enumerate(reader):
                                    rows.append(row)
                                    if i >= 5:  # Sample 5 rows
                                        break
                                
                                # Infer types for each column
                                for header in headers:
                                    if header not in schema.elements:
                                        # Get values for this column
                                        values = [row.get(header) for row in rows if row.get(header)]
                                        
                                        if values:
                                            # Infer type from the first non-empty value
                                            data_type = self._infer_type(values[0])
                                            
                                            schema.elements[header] = SchemaElement(
                                                name=header,
                                                data_type=data_type,
                                                description=f"Column from CSV file"
                                            )
                    except Exception as e:
                        logger.warning(f"Error inferring schema from CSV file {file_path}: {e}")
                
                elif file_extension == "json":
                    # Try to infer schema from JSON file
                    try:
                        with open(file_path, 'r', encoding=self.encoding) as f:
                            data = json.load(f)
                            
                            if isinstance(data, dict):
                                # Add each top-level field to the schema
                                for key, value in data.items():
                                    if key not in schema.elements:
                                        data_type = self._infer_type(value)
                                        
                                        schema.elements[key] = SchemaElement(
                                            name=key,
                                            data_type=data_type,
                                            description=f"Field from JSON file"
                                        )
                            elif isinstance(data, list) and data:
                                # If it's a list of objects, use the first item
                                if isinstance(data[0], dict):
                                    for key, value in data[0].items():
                                        if key not in schema.elements:
                                            data_type = self._infer_type(value)
                                            
                                            schema.elements[key] = SchemaElement(
                                                name=key,
                                                data_type=data_type,
                                                description=f"Field from JSON array"
                                            )
                    except Exception as e:
                        logger.warning(f"Error inferring schema from JSON file {file_path}: {e}")
            
            # Add metadata
            schema.metadata["root_path"] = self.root_path
            schema.metadata["file_types"] = self.file_types
            
            return schema
            
        except Exception as e:
            logger.error(f"Error discovering schema: {e}")
            return schema
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """
        Execute a query against the file system.
        
        Args:
            query: The query to execute.
            params: Optional parameters for the query.
            
        Returns:
            The result of the query.
        """
        if not self.is_connected:
            return QueryResult(
                data=None,
                error="Not connected to file system",
                execution_time=0.0
            )
        
        try:
            start_time = time.time()
            
            # Parse the query
            operation, args = self._parse_query(query)
            
            # Merge with provided params
            if params:
                args.update(params)
            
            # Execute the operation
            if operation == "LIST":
                data = self._list_files(args)
            elif operation == "READ":
                data = self._read_file(args)
            elif operation == "WRITE":
                data = self._write_file(args)
            elif operation == "DELETE":
                data = self._delete_file(args)
            elif operation == "SEARCH":
                data = self._search_files(args)
            else:
                return QueryResult(
                    data=None,
                    error=f"Unsupported operation: {operation}",
                    execution_time=time.time() - start_time
                )
            
            # Create metadata
            metadata = {
                "operation": operation,
                "args": args,
                "root_path": self.root_path
            }
            
            return QueryResult(
                data=data,
                metadata=metadata,
                execution_time=time.time() - start_time
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
        Translate a natural language query to the native query format.
        
        Args:
            natural_language_query: The natural language query.
            
        Returns:
            The translated query in the native format.
        """
        query = natural_language_query.lower()
        
        # Check for list operations
        if "list" in query or "show" in query or "get" in query:
            # Look for directory
            dir_match = re.search(r'(?:in|from) (?:directory|folder|path) ["\']?([^"\']+)["\']?', query)
            if dir_match:
                return f"LIST directory={dir_match.group(1)}"
            
            # Look for extension filter
            ext_match = re.search(r'(?:with extension|files of type) ["\']?([^"\']+)["\']?', query)
            if ext_match:
                return f"LIST extension={ext_match.group(1)}"
            
            # Check for recursive flag
            if "recursively" in query or "recursive" in query or "all subdirectories" in query:
                return "LIST recursive=true"
            
            return "LIST"
        
        # Check for read operations
        if "read" in query or "open" in query or "content of" in query:
            # Look for file path
            file_match = re.search(r'(?:file|path) ["\']?([^"\']+)["\']?', query)
            if file_match:
                return f"READ path={file_match.group(1)}"
            
            return "READ"
        
        # Check for write operations
        if "write" in query or "save" in query or "create" in query:
            # Look for file path
            file_match = re.search(r'(?:file|path) ["\']?([^"\']+)["\']?', query)
            if file_match:
                return f"WRITE path={file_match.group(1)}"
            
            return "WRITE"
        
        # Check for delete operations
        if "delete" in query or "remove" in query:
            # Look for file path
            file_match = re.search(r'(?:file|path) ["\']?([^"\']+)["\']?', query)
            if file_match:
                return f"DELETE path={file_match.group(1)}"
            
            return "DELETE"
        
        # Check for search operations
        if "search" in query or "find" in query or "containing" in query:
            # Look for search term
            term_match = re.search(r'(?:containing|with|having) ["\']?([^"\']+)["\']?', query)
            if term_match:
                return f"SEARCH content={term_match.group(1)}"
            
            return "SEARCH"
        
        # Default to listing files
        return "LIST"
    
    def _parse_query(self, query: str) -> Tuple[str, Dict[str, Any]]:
        """
        Parse a query string into operation and arguments.
        
        Args:
            query: The query string.
            
        Returns:
            A tuple of (operation, arguments).
        """
        parts = query.strip().split(" ", 1)
        operation = parts[0].upper()
        
        args = {}
        if len(parts) > 1:
            # Parse arguments
            arg_parts = parts[1].split(" ")
            
            for arg in arg_parts:
                if "=" in arg:
                    key, value = arg.split("=", 1)
                    args[key] = value
        
        return operation, args
    
    def _get_sample_files(self, max_files: int = 5) -> List[str]:
        """
        Get a list of sample files for schema discovery.
        
        Args:
            max_files: Maximum number of files to return.
            
        Returns:
            A list of file paths.
        """
        sample_files = []
        
        for root, _, files in os.walk(self.root_path):
            for file in files:
                file_extension = os.path.splitext(file)[1].lower()[1:]
                
                if file_extension in self.file_types:
                    file_path = os.path.join(root, file)
                    sample_files.append(file_path)
                    
                    if len(sample_files) >= max_files:
                        return sample_files
        
        return sample_files
    
    def _list_files(self, args: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        List files in the file system.
        
        Args:
            args: Query arguments.
            
        Returns:
            A list of file metadata.
        """
        results = []
        
        # Get directory to list
        directory = args.get("directory", "")
        dir_path = os.path.join(self.root_path, directory)
        
        # Get extension filter
        extension = args.get("extension")
        
        # Get recursive flag
        recursive = args.get("recursive", "false").lower() == "true"
        
        # Get limit
        limit = int(args.get("limit", "100"))
        
        if recursive:
            # Walk through the directory recursively
            for root, _, files in os.walk(dir_path):
                for file in files:
                    # Check extension filter
                    if extension and not file.lower().endswith(f".{extension.lower()}"):
                        continue
                    
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, self.root_path)
                    
                    # Get file stats
                    stats = os.stat(file_path)
                    
                    results.append({
                        "path": rel_path,
                        "name": file,
                        "extension": os.path.splitext(file)[1].lower()[1:],
                        "size": stats.st_size,
                        "created_at": datetime.fromtimestamp(stats.st_ctime).isoformat(),
                        "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat()
                    })
                    
                    if len(results) >= limit:
                        break
                
                if len(results) >= limit:
                    break
        else:
            # List only the specified directory
            for file in os.listdir(dir_path):
                file_path = os.path.join(dir_path, file)
                
                # Skip directories
                if os.path.isdir(file_path):
                    continue
                
                # Check extension filter
                if extension and not file.lower().endswith(f".{extension.lower()}"):
                    continue
                
                rel_path = os.path.relpath(file_path, self.root_path)
                
                # Get file stats
                stats = os.stat(file_path)
                
                results.append({
                    "path": rel_path,
                    "name": file,
                    "extension": os.path.splitext(file)[1].lower()[1:],
                    "size": stats.st_size,
                    "created_at": datetime.fromtimestamp(stats.st_ctime).isoformat(),
                    "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat()
                })
                
                if len(results) >= limit:
                    break
        
        return results
    
    def _read_file(self, args: Dict[str, Any]) -> Any:
        """
        Read a file from the file system.
        
        Args:
            args: Query arguments.
            
        Returns:
            The file content.
        """
        # Get file path
        path = args.get("path")
        if not path:
            raise ValueError("Path must be provided")
        
        file_path = os.path.join(self.root_path, path)
        
        # Check if the file exists
        if not os.path.exists(file_path) or os.path.isdir(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Get file extension
        file_extension = os.path.splitext(file_path)[1].lower()[1:]
        
        # Read the file
        if file_extension == "csv":
            with open(file_path, 'r', encoding=self.encoding, newline='') as f:
                reader = csv.DictReader(f, delimiter=self.csv_delimiter, quotechar=self.csv_quotechar)
                return list(reader)
        elif file_extension == "json":
            with open(file_path, 'r', encoding=self.encoding) as f:
                return json.load(f)
        else:
            # Default to reading as text
            with open(file_path, 'r', encoding=self.encoding) as f:
                return f.read()
    
    def _write_file(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Write a file to the file system.
        
        Args:
            args: Query arguments.
            
        Returns:
            Metadata about the written file.
        """
        # Get file path
        path = args.get("path")
        if not path:
            raise ValueError("Path must be provided")
        
        file_path = os.path.join(self.root_path, path)
        
        # Get content
        content = args.get("content")
        if content is None:
            raise ValueError("Content must be provided")
        
        # Get file extension
        file_extension = os.path.splitext(file_path)[1].lower()[1:]
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Write the file
        if file_extension == "csv" and isinstance(content, list) and content and isinstance(content[0], dict):
            with open(file_path, 'w', encoding=self.encoding, newline='') as f:
                writer = csv.DictWriter(f, fieldnames=content[0].keys(), delimiter=self.csv_delimiter, quotechar=self.csv_quotechar)
                writer.writeheader()
                writer.writerows(content)
        elif file_extension == "json":
            with open(file_path, 'w', encoding=self.encoding) as f:
                json.dump(content, f, indent=2)
        else:
            # Default to writing as text
            with open(file_path, 'w', encoding=self.encoding) as f:
                f.write(str(content))
        
        # Get file stats
        stats = os.stat(file_path)
        
        return {
            "path": path,
            "name": os.path.basename(file_path),
            "extension": file_extension,
            "size": stats.st_size,
            "created_at": datetime.fromtimestamp(stats.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat()
        }
    
    def _delete_file(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete a file from the file system.
        
        Args:
            args: Query arguments.
            
        Returns:
            Metadata about the deleted file.
        """
        # Get file path
        path = args.get("path")
        if not path:
            raise ValueError("Path must be provided")
        
        file_path = os.path.join(self.root_path, path)
        
        # Check if the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Get file stats before deletion
        stats = os.stat(file_path)
        file_extension = os.path.splitext(file_path)[1].lower()[1:]
        file_name = os.path.basename(file_path)
        
        # Delete the file
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)
        else:
            os.remove(file_path)
        
        return {
            "path": path,
            "name": file_name,
            "extension": file_extension,
            "size": stats.st_size,
            "created_at": datetime.fromtimestamp(stats.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat(),
            "deleted": True
        }
    
    def _search_files(self, args: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Search for files in the file system.
        
        Args:
            args: Query arguments.
            
        Returns:
            A list of matching file metadata.
        """
        results = []
        
        # Get search parameters
        content = args.get("content")
        name = args.get("name")
        extension = args.get("extension")
        
        # Get limit
        limit = int(args.get("limit", "100"))
        
        # Walk through the directory
        for root, _, files in os.walk(self.root_path):
            for file in files:
                # Check name filter
                if name and name.lower() not in file.lower():
                    continue
                
                # Check extension filter
                if extension and not file.lower().endswith(f".{extension.lower()}"):
                    continue
                
                file_path = os.path.join(root, file)
                
                # Check content filter
                if content:
                    try:
                        with open(file_path, 'r', encoding=self.encoding) as f:
                            file_content = f.read()
                            if content.lower() not in file_content.lower():
                                continue
                    except (UnicodeDecodeError, IOError):
                        # Skip binary files or files that can't be read
                        continue
                
                # Get file stats
                stats = os.stat(file_path)
                rel_path = os.path.relpath(file_path, self.root_path)
                
                results.append({
                    "path": rel_path,
                    "name": file,
                    "extension": os.path.splitext(file)[1].lower()[1:],
                    "size": stats.st_size,
                    "created_at": datetime.fromtimestamp(stats.st_ctime).isoformat(),
                    "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat()
                })
                
                if len(results) >= limit:
                    break
            
            if len(results) >= limit:
                break
        
        return results
    
    def _infer_type(self, value: Any) -> str:
        """
        Infer the data type of a value.
        
        Args:
            value: The value to infer the type of.
            
        Returns:
            The inferred data type.
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, str):
            # Check if it's a date or datetime
            if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                return "date"
            elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
                return "datetime"
            else:
                return "string"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return "unknown" 