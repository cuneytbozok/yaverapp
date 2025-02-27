"""
Audit logging for RBAC operations.
"""

import logging
import json
import os
import time
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import uuid
import threading

from .core import ResourceType, PermissionLevel

logger = logging.getLogger(__name__)

class AuditLogger:
    """
    Logs RBAC-related operations for auditing purposes.
    """
    
    def __init__(self, log_dir: str, max_file_size_mb: int = 10, 
                max_files: int = 10, async_logging: bool = True):
        """
        Initialize the audit logger.
        
        Args:
            log_dir: The directory to store audit logs.
            max_file_size_mb: The maximum size of a log file in MB.
            max_files: The maximum number of log files to keep.
            async_logging: Whether to log asynchronously.
        """
        self.log_dir = log_dir
        self.max_file_size = max_file_size_mb * 1024 * 1024  # Convert to bytes
        self.max_files = max_files
        self.async_logging = async_logging
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Current log file
        self.current_log_file = os.path.join(log_dir, f"rbac_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Queue for async logging
        if async_logging:
            self.queue = []
            self.queue_lock = threading.Lock()
            self.stop_event = threading.Event()
            self.worker_thread = threading.Thread(target=self._log_worker)
            self.worker_thread.daemon = True
            self.worker_thread.start()
    
    def log_permission_check(self, user_id: str, resource_type: ResourceType, 
                           resource_id: str, required_level: PermissionLevel, 
                           granted: bool, client_ip: Optional[str] = None,
                           request_id: Optional[str] = None) -> None:
        """
        Log a permission check.
        
        Args:
            user_id: The ID of the user.
            resource_type: The type of resource.
            resource_id: The ID of the resource.
            required_level: The required permission level.
            granted: Whether the permission was granted.
            client_ip: The client IP address.
            request_id: The request ID.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "permission_check",
            "user_id": user_id,
            "resource_type": resource_type.value,
            "resource_id": resource_id,
            "required_level": required_level.name,
            "granted": granted,
            "client_ip": client_ip,
            "request_id": request_id or str(uuid.uuid4())
        }
        
        self._log_event(event)
    
    def log_role_assignment(self, user_id: str, role_id: str, 
                          assigned_by: str, client_ip: Optional[str] = None,
                          request_id: Optional[str] = None) -> None:
        """
        Log a role assignment.
        
        Args:
            user_id: The ID of the user.
            role_id: The ID of the role.
            assigned_by: The ID of the user who assigned the role.
            client_ip: The client IP address.
            request_id: The request ID.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "role_assignment",
            "user_id": user_id,
            "role_id": role_id,
            "assigned_by": assigned_by,
            "client_ip": client_ip,
            "request_id": request_id or str(uuid.uuid4())
        }
        
        self._log_event(event)
    
    def log_role_removal(self, user_id: str, role_id: str, 
                       removed_by: str, client_ip: Optional[str] = None,
                       request_id: Optional[str] = None) -> None:
        """
        Log a role removal.
        
        Args:
            user_id: The ID of the user.
            role_id: The ID of the role.
            removed_by: The ID of the user who removed the role.
            client_ip: The client IP address.
            request_id: The request ID.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "role_removal",
            "user_id": user_id,
            "role_id": role_id,
            "removed_by": removed_by,
            "client_ip": client_ip,
            "request_id": request_id or str(uuid.uuid4())
        }
        
        self._log_event(event)
    
    def log_role_creation(self, role_id: str, created_by: str, 
                        client_ip: Optional[str] = None,
                        request_id: Optional[str] = None) -> None:
        """
        Log a role creation.
        
        Args:
            role_id: The ID of the role.
            created_by: The ID of the user who created the role.
            client_ip: The client IP address.
            request_id: The request ID.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "role_creation",
            "role_id": role_id,
            "created_by": created_by,
            "client_ip": client_ip,
            "request_id": request_id or str(uuid.uuid4())
        }
        
        self._log_event(event)
    
    def log_role_update(self, role_id: str, updated_by: str, 
                      changes: Dict[str, Any], client_ip: Optional[str] = None,
                      request_id: Optional[str] = None) -> None:
        """
        Log a role update.
        
        Args:
            role_id: The ID of the role.
            updated_by: The ID of the user who updated the role.
            changes: The changes made to the role.
            client_ip: The client IP address.
            request_id: The request ID.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "role_update",
            "role_id": role_id,
            "updated_by": updated_by,
            "changes": changes,
            "client_ip": client_ip,
            "request_id": request_id or str(uuid.uuid4())
        }
        
        self._log_event(event)
    
    def log_role_deletion(self, role_id: str, deleted_by: str, 
                        client_ip: Optional[str] = None,
                        request_id: Optional[str] = None) -> None:
        """
        Log a role deletion.
        
        Args:
            role_id: The ID of the role.
            deleted_by: The ID of the user who deleted the role.
            client_ip: The client IP address.
            request_id: The request ID.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "role_deletion",
            "role_id": role_id,
            "deleted_by": deleted_by,
            "client_ip": client_ip,
            "request_id": request_id or str(uuid.uuid4())
        }
        
        self._log_event(event)
    
    def log_query_execution(self, user_id: str, query_type: str, 
                          data_source_id: str, original_query: str,
                          modified_query: str, execution_time_ms: float,
                          client_ip: Optional[str] = None,
                          request_id: Optional[str] = None) -> None:
        """
        Log a query execution.
        
        Args:
            user_id: The ID of the user.
            query_type: The type of query (SQL, NoSQL, etc.).
            data_source_id: The ID of the data source.
            original_query: The original query.
            modified_query: The modified query with security applied.
            execution_time_ms: The execution time in milliseconds.
            client_ip: The client IP address.
            request_id: The request ID.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "query_execution",
            "user_id": user_id,
            "query_type": query_type,
            "data_source_id": data_source_id,
            "original_query": original_query,
            "modified_query": modified_query,
            "execution_time_ms": execution_time_ms,
            "client_ip": client_ip,
            "request_id": request_id or str(uuid.uuid4())
        }
        
        self._log_event(event)
    
    def _log_event(self, event: Dict[str, Any]) -> None:
        """
        Log an event.
        
        Args:
            event: The event to log.
        """
        if self.async_logging:
            with self.queue_lock:
                self.queue.append(event)
        else:
            self._write_log(event)
    
    def _write_log(self, event: Dict[str, Any]) -> None:
        """
        Write an event to the log file.
        
        Args:
            event: The event to write.
        """
        with self.lock:
            # Check if we need to rotate the log file
            if os.path.exists(self.current_log_file) and os.path.getsize(self.current_log_file) >= self.max_file_size:
                self._rotate_logs()
            
            # Write the event to the log file
            try:
                with open(self.current_log_file, 'a') as f:
                    f.write(json.dumps(event) + '\n')
            except Exception as e:
                logger.error(f"Error writing to audit log: {e}")
    
    def _rotate_logs(self) -> None:
        """
        Rotate log files.
        """
        # Get all log files
        log_files = [f for f in os.listdir(self.log_dir) if f.startswith("rbac_audit_") and f.endswith(".log")]
        log_files.sort()
        
        # Delete oldest files if we have too many
        while len(log_files) >= self.max_files:
            oldest_file = os.path.join(self.log_dir, log_files[0])
            try:
                os.remove(oldest_file)
                log_files.pop(0)
            except Exception as e:
                logger.error(f"Error deleting old audit log file {oldest_file}: {e}")
                break
        
        # Create a new log file
        self.current_log_file = os.path.join(self.log_dir, f"rbac_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    def _log_worker(self) -> None:
        """
        Worker thread for async logging.
        """
        while not self.stop_event.is_set():
            events_to_log = []
            
            # Get events from the queue
            with self.queue_lock:
                if self.queue:
                    events_to_log = self.queue.copy()
                    self.queue.clear()
            
            # Log events
            for event in events_to_log:
                self._write_log(event)
            
            # Sleep for a short time
            time.sleep(0.1)
    
    def shutdown(self) -> None:
        """
        Shut down the audit logger.
        """
        if self.async_logging:
            self.stop_event.set()
            self.worker_thread.join(timeout=5.0)
            
            # Log any remaining events
            with self.queue_lock:
                events_to_log = self.queue.copy()
                self.queue.clear()
            
            for event in events_to_log:
                self._write_log(event) 