"""
Document repository connector for the data source connector system.
"""

import logging
import time
import os
import json
from typing import Dict, List, Any, Optional, Union, Tuple
import re
from datetime import datetime

from .core import (
    DataSourceConnector, ConnectorConfig, Schema, SchemaElement, QueryResult, ConnectorType
)

logger = logging.getLogger(__name__)

class DocumentRepositoryConnector(DataSourceConnector):
    """
    Connector for document repositories.
    """
    
    def __init__(self, config: ConnectorConfig):
        """
        Initialize the document repository connector.
        
        Args:
            config: The connector configuration.
        """
        super().__init__(config)
        
        # Extract repository-specific configuration
        self.repo_type = self.config.connection_params.get("repo_type", "local").lower()
        self.root_path = self.config.connection_params.get("root_path")
        self.api_key = self.config.auth_params.get("api_key")
        self.api_endpoint = self.config.connection_params.get("api_endpoint")
        
        # Initialize connection
        self.connection = None
    
    def connect(self) -> bool:
        """
        Connect to the document repository.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            if self.repo_type == "local":
                # For local repositories, just check if the path exists
                if not os.path.isdir(self.root_path):
                    logger.error(f"Local repository path does not exist: {self.root_path}")
                    return False
                
                self.connection = {"type": "local", "path": self.root_path}
                
            elif self.repo_type == "google_drive":
                try:
                    from googleapiclient.discovery import build
                    from google.oauth2 import service_account
                    
                    # Load service account credentials
                    credentials_path = self.config.auth_params.get("credentials_path")
                    if not credentials_path:
                        logger.error("No credentials path provided for Google Drive")
                        return False
                    
                    credentials = service_account.Credentials.from_service_account_file(
                        credentials_path, 
                        scopes=['https://www.googleapis.com/auth/drive.readonly']
                    )
                    
                    # Build the Drive service
                    service = build('drive', 'v3', credentials=credentials)
                    
                    self.connection = {
                        "type": "google_drive",
                        "service": service,
                        "root_folder_id": self.root_path  # In Google Drive, this would be a folder ID
                    }
                    
                except ImportError:
                    logger.error("Google API client not installed. Please install it with: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")
                    return False
                
            elif self.repo_type == "sharepoint":
                try:
                    from office365.runtime.auth.client_credential import ClientCredential
                    from office365.sharepoint.client_context import ClientContext
                    
                    # Get SharePoint credentials
                    client_id = self.config.auth_params.get("client_id")
                    client_secret = self.config.auth_params.get("client_secret")
                    site_url = self.config.connection_params.get("site_url")
                    
                    if not all([client_id, client_secret, site_url]):
                        logger.error("Missing required SharePoint credentials")
                        return False
                    
                    # Connect to SharePoint
                    credentials = ClientCredential(client_id, client_secret)
                    ctx = ClientContext(site_url).with_credentials(credentials)
                    
                    self.connection = {
                        "type": "sharepoint",
                        "context": ctx,
                        "site_url": site_url,
                        "root_folder": self.root_path
                    }
                    
                except ImportError:
                    logger.error("Office365 REST Python client not installed. Please install it with: pip install Office365-REST-Python-Client")
                    return False
                
            else:
                logger.error(f"Unsupported repository type: {self.repo_type}")
                return False
            
            self.is_connected = True
            logger.info(f"Connected to {self.repo_type} document repository")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to {self.repo_type} document repository: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from the document repository.
        
        Returns:
            True if the disconnection is successful, False otherwise.
        """
        try:
            self.connection = None
            self.is_connected = False
            logger.info(f"Disconnected from {self.repo_type} document repository")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from {self.repo_type} document repository: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test the connection to the document repository.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            if not self.is_connected:
                success = self.connect()
                if not success:
                    return False
            
            if self.repo_type == "local":
                return os.path.isdir(self.root_path)
            elif self.repo_type == "google_drive":
                service = self.connection["service"]
                # Try to list files in the root folder
                results = service.files().list(
                    q=f"'{self.connection['root_folder_id']}' in parents",
                    pageSize=1
                ).execute()
                return True
            elif self.repo_type == "sharepoint":
                ctx = self.connection["context"]
                # Try to get the web
                web = ctx.web
                ctx.load(web)
                ctx.execute_query()
                return True
            
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def discover_schema(self) -> Schema:
        """
        Discover the schema of the document repository.
        
        Returns:
            The schema of the document repository.
        """
        try:
            if not self.is_connected:
                success = self.connect()
                if not success:
                    return Schema()
            
            schema = Schema()
            
            # Add document metadata fields
            schema.elements["document.id"] = SchemaElement(
                name="id",
                data_type="string",
                description="Document ID",
                is_primary_key=True
            )
            
            schema.elements["document.name"] = SchemaElement(
                name="name",
                data_type="string",
                description="Document name"
            )
            
            schema.elements["document.path"] = SchemaElement(
                name="path",
                data_type="string",
                description="Document path"
            )
            
            schema.elements["document.type"] = SchemaElement(
                name="type",
                data_type="string",
                description="Document type/extension"
            )
            
            schema.elements["document.size"] = SchemaElement(
                name="size",
                data_type="integer",
                description="Document size in bytes"
            )
            
            schema.elements["document.created_at"] = SchemaElement(
                name="created_at",
                data_type="datetime",
                description="Document creation date"
            )
            
            schema.elements["document.modified_at"] = SchemaElement(
                name="modified_at",
                data_type="datetime",
                description="Document last modification date"
            )
            
            schema.elements["document.content"] = SchemaElement(
                name="content",
                data_type="text",
                description="Document content"
            )
            
            # Add repository-specific metadata
            schema.metadata = {
                "repository_type": self.repo_type,
                "root_path": self.root_path
            }
            
            return schema
            
        except Exception as e:
            logger.error(f"Error discovering schema: {e}")
            return Schema()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """
        Execute a query against the document repository.
        
        Args:
            query: The query to execute.
            params: Optional parameters for the query.
            
        Returns:
            The result of the query.
        """
        start_time = time.time()
        
        try:
            if not self.is_connected:
                success = self.connect()
                if not success:
                    return QueryResult(
                        data=None,
                        error="Not connected to document repository",
                        execution_time=time.time() - start_time
                    )
            
            # Parse the query
            query_type, query_params = self._parse_query(query)
            
            # Execute the query based on its type
            if query_type == "list":
                data = self._list_documents(query_params)
            elif query_type == "get":
                data = self._get_document(query_params)
            elif query_type == "search":
                data = self._search_documents(query_params)
            else:
                return QueryResult(
                    data=None,
                    error=f"Unsupported query type: {query_type}",
                    execution_time=time.time() - start_time
                )
            
            # Create result schema
            result_schema = Schema()
            for key in data[0].keys() if data else []:
                result_schema.elements[f"document.{key}"] = SchemaElement(
                    name=key,
                    data_type=self._infer_type(data[0][key]),
                    description=f"Document {key}"
                )
            
            execution_time = time.time() - start_time
            
            return QueryResult(
                data=data,
                schema=result_schema,
                metadata={
                    "query": query,
                    "params": params,
                    "query_type": query_type,
                    "document_count": len(data)
                },
                execution_time=execution_time
            )
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            
            return QueryResult(
                data=None,
                error=str(e),
                execution_time=time.time() - start_time
            )
    
    def translate_query(self, natural_language_query: str) -> str:
        """
        Translate a natural language query to the native query format.
        
        Args:
            natural_language_query: The natural language query.
            
        Returns:
            The translated query in the native format.
        """
        # This is a simplistic implementation
        # In a real system, you would use an LLM or more sophisticated NLP
        
        query = natural_language_query.lower()
        
        # Check for list queries
        if "list" in query or "show" in query or "get all" in query:
            if "pdf" in query or ".pdf" in query:
                return "LIST type=pdf"
            elif "doc" in query or ".doc" in query or ".docx" in query:
                return "LIST type=doc,docx"
            elif "text" in query or ".txt" in query:
                return "LIST type=txt"
            elif "image" in query or "photo" in query or ".jpg" in query or ".png" in query:
                return "LIST type=jpg,png,gif"
            else:
                return "LIST"
        
        # Check for get queries
        if "get" in query or "show" in query or "find" in query:
            # Look for document names or IDs
            name_match = re.search(r'(called|named|titled) ["\'](.+?)["\']', query)
            if name_match:
                return f"GET name={name_match.group(2)}"
            
            id_match = re.search(r'(id|identifier) ["\'](.+?)["\']', query)
            if id_match:
                return f"GET id={id_match.group(2)}"
            
            path_match = re.search(r'(path|location) ["\'](.+?)["\']', query)
            if path_match:
                return f"GET path={path_match.group(2)}"
        
        # Check for search queries
        if "search" in query or "find" in query or "containing" in query or "about" in query:
            # Look for search terms
            term_match = re.search(r'(containing|about|with) ["\'](.+?)["\']', query)
            if term_match:
                return f"SEARCH content={term_match.group(2)}"
            
            # Look for date filters
            date_match = re.search(r'(created|modified) (before|after|on) ["\'](.+?)["\']', query)
            if date_match:
                field = "created_at" if date_match.group(1) == "created" else "modified_at"
                operator = date_match.group(2)
                date_str = date_match.group(3)
                
                if operator == "before":
                    return f"SEARCH {field}<{date_str}"
                elif operator == "after":
                    return f"SEARCH {field}>{date_str}"
                else:  # on
                    return f"SEARCH {field}={date_str}"
        
        # Default to a simple list query
        return "LIST limit=10"
    
    def _parse_query(self, query: str) -> Tuple[str, Dict[str, Any]]:
        """
        Parse a query string into a query type and parameters.
        
        Args:
            query: The query string.
            
        Returns:
            A tuple of (query_type, parameters).
        """
        parts = query.strip().split(maxsplit=1)
        query_type = parts[0].upper()
        
        params = {}
        if len(parts) > 1:
            param_str = parts[1]
            for param in param_str.split():
                if "=" in param:
                    key, value = param.split("=", 1)
                    
                    # Handle comma-separated values
                    if "," in value:
                        params[key] = value.split(",")
                    else:
                        # Handle operators in the key
                        if "<" in key:
                            base_key = key.replace("<", "")
                            params[base_key] = {"operator": "<", "value": value}
                        elif ">" in key:
                            base_key = key.replace(">", "")
                            params[base_key] = {"operator": ">", "value": value}
                        else:
                            params[key] = value
        
        return query_type, params
    
    def _list_documents(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        List documents in the repository.
        
        Args:
            params: Query parameters.
            
        Returns:
            A list of document metadata.
        """
        if self.repo_type == "local":
            return self._list_local_documents(params)
        elif self.repo_type == "google_drive":
            return self._list_google_drive_documents(params)
        elif self.repo_type == "sharepoint":
            return self._list_sharepoint_documents(params)
        else:
            return []
    
    def _list_local_documents(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        List documents in a local repository.
        
        Args:
            params: Query parameters.
            
        Returns:
            A list of document metadata.
        """
        documents = []
        
        # Get file types filter
        file_types = params.get("type")
        if file_types and not isinstance(file_types, list):
            file_types = [file_types]
        
        # Get limit
        limit = int(params.get("limit", 100))
        
        # Walk through the directory
        for root, _, files in os.walk(self.root_path):
            for file in files:
                # Check file type if filter is applied
                if file_types:
                    ext = file.split(".")[-1].lower()
                    if ext not in file_types:
                        continue
                
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, self.root_path)
                
                # Get file stats
                stats = os.stat(file_path)
                
                documents.append({
                    "id": rel_path,
                    "name": file,
                    "path": rel_path,
                    "type": file.split(".")[-1].lower(),
                    "size": stats.st_size,
                    "created_at": datetime.fromtimestamp(stats.st_ctime).isoformat(),
                    "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat()
                })
                
                # Check limit
                if len(documents) >= limit:
                    break
            
            # Check limit again after processing a directory
            if len(documents) >= limit:
                break
        
        return documents
    
    def _list_google_drive_documents(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        List documents in a Google Drive repository.
        
        Args:
            params: Query parameters.
            
        Returns:
            A list of document metadata.
        """
        documents = []
        
        # Get file types filter
        file_types = params.get("type")
        if file_types and not isinstance(file_types, list):
            file_types = [file_types]
        
        # Get limit
        limit = int(params.get("limit", 100))
        
        # Build query
        query = f"'{self.connection['root_folder_id']}' in parents and trashed = false"
        if file_types:
            mime_types = []
            for file_type in file_types:
                if file_type == "pdf":
                    mime_types.append("application/pdf")
                elif file_type in ["doc", "docx"]:
                    mime_types.append("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                    mime_types.append("application/msword")
                elif file_type == "txt":
                    mime_types.append("text/plain")
                elif file_type in ["jpg", "jpeg"]:
                    mime_types.append("image/jpeg")
                elif file_type == "png":
                    mime_types.append("image/png")
            
            if mime_types:
                mime_query = " or ".join([f"mimeType='{mime}'" for mime in mime_types])
                query += f" and ({mime_query})"
        
        # Execute query
        service = self.connection["service"]
        results = service.files().list(
            q=query,
            pageSize=limit,
            fields="files(id, name, mimeType, size, createdTime, modifiedTime, webViewLink)"
        ).execute()
        
        # Process results
        for file in results.get("files", []):
            documents.append({
                "id": file["id"],
                "name": file["name"],
                "path": file.get("webViewLink", ""),
                "type": self._mime_to_extension(file["mimeType"]),
                "size": int(file.get("size", 0)),
                "created_at": file.get("createdTime", ""),
                "modified_at": file.get("modifiedTime", "")
            })
        
        return documents
    
    def _list_sharepoint_documents(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        List documents in a SharePoint repository.
        
        Args:
            params: Query parameters.
            
        Returns:
            A list of document metadata.
        """
        documents = []
        
        # Get file types filter
        file_types = params.get("type")
        if file_types and not isinstance(file_types, list):
            file_types = [file_types]
        
        # Get limit
        limit = int(params.get("limit", 100))
        
        # Get the context and root folder
        ctx = self.connection["context"]
        root_folder = self.connection["root_folder"]
        
        # Get the folder
        folder = ctx.web.get_folder_by_server_relative_url(root_folder)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()
        
        # Process files
        for file in files:
            # Check file type if filter is applied
            if file_types:
                ext = file.properties["Name"].split(".")[-1].lower()
                if ext not in file_types:
                    continue
            
            documents.append({
                "id": file.properties["UniqueId"],
                "name": file.properties["Name"],
                "path": file.properties["ServerRelativeUrl"],
                "type": file.properties["Name"].split(".")[-1].lower(),
                "size": file.properties["Length"],
                "created_at": file.properties["TimeCreated"],
                "modified_at": file.properties["TimeLastModified"]
            })
            
            # Check limit
            if len(documents) >= limit:
                break
        
        return documents
    
    def _get_document(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get a specific document from the repository.
        
        Args:
            params: Query parameters.
            
        Returns:
            A list containing the document metadata and content.
        """
        # Determine how to identify the document
        doc_id = params.get("id")
        doc_name = params.get("name")
        doc_path = params.get("path")
        
        if not any([doc_id, doc_name, doc_path]):
            return []
        
        if self.repo_type == "local":
            return self._get_local_document(doc_id, doc_name, doc_path)
        elif self.repo_type == "google_drive":
            return self._get_google_drive_document(doc_id, doc_name, doc_path)
        elif self.repo_type == "sharepoint":
            return self._get_sharepoint_document(doc_id, doc_name, doc_path)
        else:
            return []
    
    def _get_local_document(self, doc_id: Optional[str], doc_name: Optional[str], doc_path: Optional[str]) -> List[Dict[str, Any]]:
        """
        Get a specific document from a local repository.
        
        Args:
            doc_id: Document ID (for local, this is the relative path).
            doc_name: Document name.
            doc_path: Document path.
            
        Returns:
            A list containing the document metadata and content.
        """
        # For local repository, doc_id and doc_path are the same
        path = doc_id or doc_path
        
        if path:
            file_path = os.path.join(self.root_path, path)
        elif doc_name:
            # Search for the file by name
            for root, _, files in os.walk(self.root_path):
                if doc_name in files:
                    file_path = os.path.join(root, doc_name)
                    break
            else:
                return []
        else:
            return []
        
        if not os.path.isfile(file_path):
            return []
        
        # Get file stats
        stats = os.stat(file_path)
        rel_path = os.path.relpath(file_path, self.root_path)
        
        # Read file content
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            # Binary file
            content = "[Binary content]"
        
        return [{
            "id": rel_path,
            "name": os.path.basename(file_path),
            "path": rel_path,
            "type": file_path.split(".")[-1].lower(),
            "size": stats.st_size,
            "created_at": datetime.fromtimestamp(stats.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat(),
            "content": content
        }]
    
    def _search_documents(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Search for documents in the repository.
        
        Args:
            params: Query parameters.
            
        Returns:
            A list of document metadata.
        """
        # Get search parameters
        content_search = params.get("content")
        name_search = params.get("name")
        
        # Get date filters
        created_at = params.get("created_at")
        modified_at = params.get("modified_at")
        
        # Get limit
        limit = int(params.get("limit", 10))
        
        if self.repo_type == "local":
            return self._search_local_documents(content_search, name_search, created_at, modified_at, limit)
        elif self.repo_type == "google_drive":
            return self._search_google_drive_documents(content_search, name_search, created_at, modified_at, limit)
        elif self.repo_type == "sharepoint":
            return self._search_sharepoint_documents(content_search, name_search, created_at, modified_at, limit)
        else:
            return []
    
    def _search_local_documents(self, content_search: Optional[str], name_search: Optional[str], 
                               created_at: Optional[Dict[str, Any]], modified_at: Optional[Dict[str, Any]], 
                               limit: int) -> List[Dict[str, Any]]:
        """
        Search for documents in a local repository.
        
        Args:
            content_search: Text to search for in document content.
            name_search: Text to search for in document names.
            created_at: Filter for creation date.
            modified_at: Filter for modification date.
            limit: Maximum number of results.
            
        Returns:
            A list of document metadata.
        """
        results = []
        
        # Walk through the directory
        for root, _, files in os.walk(self.root_path):
            for file in files:
                file_path = os.path.join(root, file)
                
                # Check name filter
                if name_search and name_search.lower() not in file.lower():
                    continue
                
                # Get file stats
                stats = os.stat(file_path)
                
                # Check date filters
                if created_at:
                    file_created = datetime.fromtimestamp(stats.st_ctime)
                    if isinstance(created_at, dict):
                        op = created_at.get("operator")
                        value = datetime.fromisoformat(created_at.get("value"))
                        
                        if op == "<" and not file_created < value:
                            continue
                        elif op == ">" and not file_created > value:
                            continue
                        elif op == "=" and not file_created.date() == value.date():
                            continue
                
                if modified_at:
                    file_modified = datetime.fromtimestamp(stats.st_mtime)
                    if isinstance(modified_at, dict):
                        op = modified_at.get("operator")
                        value = datetime.fromisoformat(modified_at.get("value"))
                        
                        if op == "<" and not file_modified < value:
                            continue
                        elif op == ">" and not file_modified > value:
                            continue
                        elif op == "=" and not file_modified.date() == value.date():
                            continue
                
                # Check content filter
                if content_search:
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            if content_search.lower() not in content.lower():
                                continue
                    except (UnicodeDecodeError, IOError):
                        # Skip binary files or files that can't be read
                        continue
                
                # Add to results
                rel_path = os.path.relpath(file_path, self.root_path)
                results.append({
                    "id": rel_path,
                    "name": file,
                    "path": rel_path,
                    "type": file.split(".")[-1].lower(),
                    "size": stats.st_size,
                    "created_at": datetime.fromtimestamp(stats.st_ctime).isoformat(),
                    "modified_at": datetime.fromtimestamp(stats.st_mtime).isoformat()
                })
                
                # Check limit
                if len(results) >= limit:
                    break
            
            # Check limit again after processing a directory
            if len(results) >= limit:
                break
        
        return results
    
    def _mime_to_extension(self, mime_type: str) -> str:
        """
        Convert a MIME type to a file extension.
        
        Args:
            mime_type: The MIME type.
            
        Returns:
            The corresponding file extension.
        """
        mime_map = {
            "application/pdf": "pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
            "application/msword": "doc",
            "text/plain": "txt",
            "image/jpeg": "jpg",
            "image/png": "png",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
            "application/vnd.ms-excel": "xls",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
            "application/vnd.ms-powerpoint": "ppt"
        }
        
        return mime_map.get(mime_type, "unknown")
    
    def _infer_type(self, value: Any) -> str:
        """
        Infer the data type of a value.
        
        Args:
            value: The value to infer the type of.
            
        Returns:
            The inferred data type.
        """
        if isinstance(value, str):
            if len(value) > 255:
                return "text"
            else:
                return "string"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, (list, tuple)):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return "string" 