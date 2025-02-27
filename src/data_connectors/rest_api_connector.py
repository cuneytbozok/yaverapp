"""
REST API connector for the data source connector system.
"""

import logging
import time
import json
import re
from typing import Dict, List, Any, Optional, Union, Tuple
from urllib.parse import urljoin, urlparse
import requests

from .core import (
    DataSourceConnector, ConnectorConfig, Schema, SchemaElement, QueryResult, ConnectorType
)

logger = logging.getLogger(__name__)

class RESTAPIConnector(DataSourceConnector):
    """
    Connector for REST APIs.
    """
    
    def __init__(self, config: ConnectorConfig):
        """
        Initialize the REST API connector.
        
        Args:
            config: The connector configuration.
        """
        super().__init__(config)
        
        # Extract API-specific configuration
        self.base_url = self.config.connection_params.get("base_url")
        self.auth_type = self.config.auth_params.get("auth_type", "none").lower()
        self.api_key = self.config.auth_params.get("api_key")
        self.api_key_header = self.config.auth_params.get("api_key_header", "X-API-Key")
        self.username = self.config.auth_params.get("username")
        self.password = self.config.auth_params.get("password")
        self.oauth_token = self.config.auth_params.get("oauth_token")
        self.oauth_header = self.config.auth_params.get("oauth_header", "Authorization")
        self.default_headers = self.config.connection_params.get("default_headers", {})
        self.timeout = self.config.connection_params.get("timeout", 30)
        
        # Endpoints configuration
        self.endpoints = self.config.connection_params.get("endpoints", {})
        
        # Initialize session
        self.session = None
    
    def connect(self) -> bool:
        """
        Connect to the REST API.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            # Create a new session
            self.session = requests.Session()
            
            # Set default headers
            self.session.headers.update(self.default_headers)
            
            # Set authentication
            if self.auth_type == "api_key":
                if not self.api_key:
                    logger.error("API key not provided")
                    return False
                
                self.session.headers.update({self.api_key_header: self.api_key})
                
            elif self.auth_type == "basic":
                if not self.username or not self.password:
                    logger.error("Username or password not provided for basic auth")
                    return False
                
                self.session.auth = (self.username, self.password)
                
            elif self.auth_type == "oauth":
                if not self.oauth_token:
                    logger.error("OAuth token not provided")
                    return False
                
                self.session.headers.update({self.oauth_header: f"Bearer {self.oauth_token}"})
            
            # Test the connection
            if not self.test_connection():
                logger.error("Connection test failed")
                return False
            
            self.is_connected = True
            logger.info(f"Connected to REST API: {self.base_url}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to REST API: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from the REST API.
        
        Returns:
            True if the disconnection is successful, False otherwise.
        """
        try:
            if self.session:
                self.session.close()
                self.session = None
            
            self.is_connected = False
            logger.info(f"Disconnected from REST API: {self.base_url}")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from REST API: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test the connection to the REST API.
        
        Returns:
            True if the connection is successful, False otherwise.
        """
        try:
            # If a specific health check endpoint is provided, use it
            health_endpoint = self.endpoints.get("health")
            
            if health_endpoint:
                url = urljoin(self.base_url, health_endpoint)
                response = self.session.get(url, timeout=self.timeout)
                return response.status_code < 400
            
            # Otherwise, just try to access the base URL
            response = self.session.get(self.base_url, timeout=self.timeout)
            return response.status_code < 400
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def discover_schema(self) -> Schema:
        """
        Discover the schema of the REST API.
        
        Returns:
            The schema of the REST API.
        """
        schema = Schema()
        
        try:
            # For REST APIs, we'll try to discover the schema by making sample requests
            # to the configured endpoints and analyzing the responses
            
            # First, check if we have a schema endpoint
            schema_endpoint = self.endpoints.get("schema")
            if schema_endpoint:
                url = urljoin(self.base_url, schema_endpoint)
                response = self.session.get(url, timeout=self.timeout)
                
                if response.status_code < 400:
                    try:
                        # Try to parse the schema from the response
                        schema_data = response.json()
                        
                        # Process the schema data
                        if isinstance(schema_data, dict):
                            for key, value in schema_data.items():
                                if isinstance(value, dict) and "type" in value:
                                    schema.elements[key] = SchemaElement(
                                        name=key,
                                        data_type=value["type"],
                                        description=value.get("description"),
                                        is_nullable=value.get("nullable", True)
                                    )
                    except Exception as e:
                        logger.warning(f"Error parsing schema from endpoint: {e}")
            
            # If we don't have a schema yet, try to infer it from sample responses
            if not schema.elements:
                # Try each endpoint in the configuration
                for endpoint_name, endpoint_path in self.endpoints.items():
                    if endpoint_name == "schema" or endpoint_name == "health":
                        continue
                    
                    url = urljoin(self.base_url, endpoint_path)
                    
                    try:
                        response = self.session.get(url, timeout=self.timeout)
                        
                        if response.status_code < 400:
                            data = response.json()
                            
                            # If the response is a list, take the first item
                            if isinstance(data, list) and data:
                                sample = data[0]
                            elif isinstance(data, dict):
                                # If the response has a data field that's a list, take the first item
                                if "data" in data and isinstance(data["data"], list) and data["data"]:
                                    sample = data["data"][0]
                                else:
                                    sample = data
                            else:
                                continue
                            
                            # Infer schema from the sample
                            if isinstance(sample, dict):
                                endpoint_schema = self._infer_schema_from_sample(sample)
                                
                                # Add the endpoint schema to the overall schema
                                for key, element in endpoint_schema.elements.items():
                                    if key not in schema.elements:
                                        schema.elements[key] = element
                    except Exception as e:
                        logger.warning(f"Error inferring schema from endpoint {endpoint_name}: {e}")
            
            # Add metadata
            schema.metadata["base_url"] = self.base_url
            schema.metadata["endpoints"] = list(self.endpoints.keys())
            
            return schema
            
        except Exception as e:
            logger.error(f"Error discovering schema: {e}")
            return schema
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """
        Execute a query against the REST API.
        
        Args:
            query: The query to execute.
            params: Optional parameters for the query.
            
        Returns:
            The result of the query.
        """
        if not self.is_connected:
            return QueryResult(
                data=None,
                error="Not connected to REST API",
                execution_time=0.0
            )
        
        try:
            start_time = time.time()
            
            # Parse the query
            method, endpoint, query_params = self._parse_query(query)
            
            # Merge with provided params
            if params:
                query_params.update(params)
            
            # Build the URL
            url = urljoin(self.base_url, endpoint)
            
            # Execute the request
            response = self._execute_request(method, url, query_params)
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Check for errors
            if response.status_code >= 400:
                return QueryResult(
                    data=None,
                    error=f"API returned error: {response.status_code} - {response.text}",
                    execution_time=execution_time
                )
            
            # Parse the response
            try:
                data = response.json()
            except ValueError:
                # Not JSON, return text
                data = response.text
            
            # Create metadata
            metadata = {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "url": response.url,
                "method": method
            }
            
            return QueryResult(
                data=data,
                metadata=metadata,
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
        Translate a natural language query to the native query format.
        
        Args:
            natural_language_query: The natural language query.
            
        Returns:
            The translated query in the native format.
        """
        query = natural_language_query.lower()
        
        # Check for GET requests
        if "get" in query or "retrieve" in query or "fetch" in query:
            # Look for specific resource
            resource_match = re.search(r'(get|retrieve|fetch) (\w+)', query)
            if resource_match:
                resource = resource_match.group(2)
                
                # Check if it's a specific item by ID
                id_match = re.search(r'with id (\w+)', query)
                if id_match:
                    return f"GET {resource}/{id_match.group(1)}"
                
                # Check for filtering
                filter_match = re.search(r'where (\w+) (is|equals|=) ["\']?([^"\']+)["\']?', query)
                if filter_match:
                    field = filter_match.group(1)
                    value = filter_match.group(3)
                    return f"GET {resource}?{field}={value}"
                
                # Default to listing all
                return f"GET {resource}"
        
        # Check for POST requests
        if "create" in query or "add" in query or "post" in query:
            resource_match = re.search(r'(create|add|post) (\w+)', query)
            if resource_match:
                resource = resource_match.group(2)
                return f"POST {resource}"
        
        # Check for PUT/PATCH requests
        if "update" in query or "modify" in query or "change" in query:
            resource_match = re.search(r'(update|modify|change) (\w+)', query)
            if resource_match:
                resource = resource_match.group(2)
                
                # Check for ID
                id_match = re.search(r'with id (\w+)', query)
                if id_match:
                    return f"PUT {resource}/{id_match.group(1)}"
                
                return f"PUT {resource}"
        
        # Check for DELETE requests
        if "delete" in query or "remove" in query:
            resource_match = re.search(r'(delete|remove) (\w+)', query)
            if resource_match:
                resource = resource_match.group(2)
                
                # Check for ID
                id_match = re.search(r'with id (\w+)', query)
                if id_match:
                    return f"DELETE {resource}/{id_match.group(1)}"
                
                return f"DELETE {resource}"
        
        # Default to a GET on the base endpoint
        return "GET /"
    
    def _parse_query(self, query: str) -> Tuple[str, str, Dict[str, Any]]:
        """
        Parse a query string into method, endpoint, and parameters.
        
        Args:
            query: The query string.
            
        Returns:
            A tuple of (method, endpoint, parameters).
        """
        parts = query.strip().split(" ", 1)
        method = parts[0].upper()
        
        if len(parts) > 1:
            endpoint_parts = parts[1].split("?", 1)
            endpoint = endpoint_parts[0]
            
            # Parse query parameters
            params = {}
            if len(endpoint_parts) > 1:
                param_str = endpoint_parts[1]
                param_pairs = param_str.split("&")
                
                for pair in param_pairs:
                    if "=" in pair:
                        key, value = pair.split("=", 1)
                        params[key] = value
        else:
            endpoint = "/"
            params = {}
        
        return method, endpoint, params
    
    def _execute_request(self, method: str, url: str, params: Dict[str, Any]) -> requests.Response:
        """
        Execute an HTTP request.
        
        Args:
            method: The HTTP method.
            url: The URL.
            params: The query parameters.
            
        Returns:
            The HTTP response.
        """
        if method == "GET":
            return self.session.get(url, params=params, timeout=self.timeout)
        elif method == "POST":
            return self.session.post(url, json=params, timeout=self.timeout)
        elif method == "PUT":
            return self.session.put(url, json=params, timeout=self.timeout)
        elif method == "PATCH":
            return self.session.patch(url, json=params, timeout=self.timeout)
        elif method == "DELETE":
            return self.session.delete(url, params=params, timeout=self.timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
    
    def _infer_schema_from_sample(self, sample: Dict[str, Any]) -> Schema:
        """
        Infer a schema from a sample object.
        
        Args:
            sample: A sample object.
            
        Returns:
            The inferred schema.
        """
        schema = Schema()
        
        for key, value in sample.items():
            data_type = self._infer_type(value)
            
            schema.elements[key] = SchemaElement(
                name=key,
                data_type=data_type,
                is_nullable=value is None
            )
        
        return schema
    
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