"""
Manager for RBAC functionality.
"""

import logging
import os
from typing import Dict, List, Any, Optional, Union
import json

from .core import (
    Role, Permission, ResourceType, PermissionLevel, 
    PermissionEvaluator, PermissionDeniedError
)
from .storage import FileRBACStorage, RBACStorage
from .query_modifier import QueryModifier
from .audit import AuditLogger
from .integration import RBACQueryProcessor

logger = logging.getLogger(__name__)

class RBACManager:
    """
    Manager for RBAC functionality.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the RBAC manager.
        
        Args:
            config: The RBAC configuration.
        """
        self.config = config
        
        # Initialize storage
        storage_config = config.get("storage", {})
        self._init_storage(storage_config)
        
        # Initialize permission evaluator
        self.permission_evaluator = PermissionEvaluator(self.storage)
        
        # Initialize query modifier
        self.query_modifier = QueryModifier(self.permission_evaluator)
        
        # Initialize audit logger
        audit_config = config.get("audit", {})
        self._init_audit_logger(audit_config)
    
    def _init_storage(self, config: Dict[str, Any]) -> None:
        """
        Initialize RBAC storage.
        
        Args:
            config: The storage configuration.
        """
        storage_type = config.get("type", "file")
        
        if storage_type == "file":
            directory = config.get("directory", "data/rbac")
            self.storage = FileRBACStorage(directory)
        else:
            raise ValueError(f"Unsupported RBAC storage type: {storage_type}")
    
    def _init_audit_logger(self, config: Dict[str, Any]) -> None:
        """
        Initialize the audit logger.
        
        Args:
            config: The audit configuration.
        """
        if config.get("enabled", True):
            log_dir = config.get("log_dir", "logs/rbac")
            max_file_size_mb = config.get("max_file_size_mb", 10)
            max_files = config.get("max_files", 10)
            async_logging = config.get("async_logging", True)
            
            self.audit_logger = AuditLogger(
                log_dir=log_dir,
                max_file_size_mb=max_file_size_mb,
                max_files=max_files,
                async_logging=async_logging
            )
        else:
            self.audit_logger = None
    
    def wrap_query_processor(self, query_processor) -> RBACQueryProcessor:
        """
        Wrap a query processor with RBAC security.
        
        Args:
            query_processor: The query processor to wrap.
            
        Returns:
            The wrapped query processor.
        """
        return RBACQueryProcessor(
            query_processor=query_processor,
            permission_evaluator=self.permission_evaluator,
            audit_logger=self.audit_logger
        )
    
    def create_role(self, role: Role, created_by: str, 
                  client_ip: Optional[str] = None,
                  request_id: Optional[str] = None) -> bool:
        """
        Create a new role.
        
        Args:
            role: The role to create.
            created_by: The ID of the user who created the role.
            client_ip: The client IP address.
            request_id: The request ID.
            
        Returns:
            True if the role was created, False otherwise.
        """
        result = self.storage.create_role(role)
        
        if result and self.audit_logger:
            self.audit_logger.log_role_creation(
                role_id=role.id,
                created_by=created_by,
                client_ip=client_ip,
                request_id=request_id
            )
        
        # Clear permission cache
        self.permission_evaluator.clear_cache()
        
        return result
    
    def update_role(self, role: Role, updated_by: str, 
                  changes: Dict[str, Any],
                  client_ip: Optional[str] = None,
                  request_id: Optional[str] = None) -> bool:
        """
        Update a role.
        
        Args:
            role: The role to update.
            updated_by: The ID of the user who updated the role.
            changes: The changes made to the role.
            client_ip: The client IP address.
            request_id: The request ID.
            
        Returns:
            True if the role was updated, False otherwise.
        """
        result = self.storage.update_role(role)
        
        if result and self.audit_logger:
            self.audit_logger.log_role_update(
                role_id=role.id,
                updated_by=updated_by,
                changes=changes,
                client_ip=client_ip,
                request_id=request_id
            )
        
        # Clear permission cache
        self.permission_evaluator.clear_cache()
        
        return result
    
    def delete_role(self, role_id: str, deleted_by: str, 
                  client_ip: Optional[str] = None,
                  request_id: Optional[str] = None) -> bool:
        """
        Delete a role.
        
        Args:
            role_id: The ID of the role to delete.
            deleted_by: The ID of the user who deleted the role.
            client_ip: The client IP address.
            request_id: The request ID.
            
        Returns:
            True if the role was deleted, False otherwise.
        """
        result = self.storage.delete_role(role_id)
        
        if result and self.audit_logger:
            self.audit_logger.log_role_deletion(
                role_id=role_id,
                deleted_by=deleted_by,
                client_ip=client_ip,
                request_id=request_id
            )
        
        # Clear permission cache
        self.permission_evaluator.clear_cache()
        
        return result
    
    def assign_role_to_user(self, user_id: str, role_id: str, 
                          assigned_by: str, client_ip: Optional[str] = None,
                          request_id: Optional[str] = None) -> bool:
        """
        Assign a role to a user.
        
        Args:
            user_id: The ID of the user.
            role_id: The ID of the role.
            assigned_by: The ID of the user who assigned the role.
            client_ip: The client IP address.
            request_id: The request ID.
            
        Returns:
            True if the role was assigned, False otherwise.
        """
        result = self.storage.assign_role_to_user(user_id, role_id)
        
        if result and self.audit_logger:
            self.audit_logger.log_role_assignment(
                user_id=user_id,
                role_id=role_id,
                assigned_by=assigned_by,
                client_ip=client_ip,
                request_id=request_id
            )
        
        # Clear permission cache for the user
        self.permission_evaluator.clear_cache(user_id)
        
        return result
    
    def remove_role_from_user(self, user_id: str, role_id: str, 
                            removed_by: str, client_ip: Optional[str] = None,
                            request_id: Optional[str] = None) -> bool:
        """
        Remove a role from a user.
        
        Args:
            user_id: The ID of the user.
            role_id: The ID of the role.
            removed_by: The ID of the user who removed the role.
            client_ip: The client IP address.
            request_id: The request ID.
            
        Returns:
            True if the role was removed, False otherwise.
        """
        result = self.storage.remove_role_from_user(user_id, role_id)
        
        if result and self.audit_logger:
            self.audit_logger.log_role_removal(
                user_id=user_id,
                role_id=role_id,
                removed_by=removed_by,
                client_ip=client_ip,
                request_id=request_id
            )
        
        # Clear permission cache for the user
        self.permission_evaluator.clear_cache(user_id)
        
        return result
    
    def get_user_roles(self, user_id: str) -> List[Role]:
        """
        Get the roles assigned to a user.
        
        Args:
            user_id: The ID of the user.
            
        Returns:
            A list of roles assigned to the user.
        """
        role_ids = self.storage.get_user_roles(user_id)
        roles = []
        
        for role_id in role_ids:
            role = self.storage.get_role(role_id)
            if role:
                roles.append(role)
        
        return roles
    
    def check_permission(self, user_id: str, resource_type: ResourceType, 
                       resource_id: str, required_level: PermissionLevel,
                       client_ip: Optional[str] = None,
                       request_id: Optional[str] = None) -> bool:
        """
        Check if a user has a permission.
        
        Args:
            user_id: The ID of the user.
            resource_type: The type of resource.
            resource_id: The ID of the resource.
            required_level: The required permission level.
            client_ip: The client IP address.
            request_id: The request ID.
            
        Returns:
            True if the user has the permission, False otherwise.
        """
        try:
            self.permission_evaluator.check_permission(
                user_id, resource_type, resource_id, required_level
            )
            
            if self.audit_logger:
                self.audit_logger.log_permission_check(
                    user_id=user_id,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    required_level=required_level,
                    granted=True,
                    client_ip=client_ip,
                    request_id=request_id
                )
            
            return True
        
        except PermissionDeniedError:
            if self.audit_logger:
                self.audit_logger.log_permission_check(
                    user_id=user_id,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    required_level=required_level,
                    granted=False,
                    client_ip=client_ip,
                    request_id=request_id
                )
            
            return False
    
    def shutdown(self) -> None:
        """
        Shut down the RBAC manager.
        """
        if self.audit_logger:
            self.audit_logger.shutdown() 