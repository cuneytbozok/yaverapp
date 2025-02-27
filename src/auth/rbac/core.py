"""
Core role-based access control (RBAC) module for the AI-powered data retrieval application.
This module defines the base interfaces and common functionality for RBAC.
"""

import logging
import time
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Dict, List, Any, Optional, Union, Set, Tuple, Callable
from dataclasses import dataclass, field
import json
import re
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PermissionLevel(Enum):
    """Enum representing different permission levels."""
    NONE = 0
    READ = 10
    WRITE = 20
    ADMIN = 30

class ResourceType(Enum):
    """Enum representing different resource types."""
    SYSTEM = "system"
    FEATURE = "feature"
    DATA_SOURCE = "data_source"
    TABLE = "table"
    COLLECTION = "collection"
    FIELD = "field"
    RECORD = "record"

@dataclass
class Permission:
    """Permission data."""
    resource_type: ResourceType
    resource_id: str
    level: PermissionLevel
    conditions: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash((self.resource_type, self.resource_id, self.level))

@dataclass
class Role:
    """Role data."""
    id: str
    name: str
    description: str = ""
    permissions: List[Permission] = field(default_factory=list)
    parent_roles: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash(self.id)

class PermissionDeniedError(Exception):
    """Exception raised when a permission is denied."""
    
    def __init__(self, user_id: str, resource_type: ResourceType, resource_id: str, 
                 required_level: PermissionLevel, message: Optional[str] = None):
        self.user_id = user_id
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.required_level = required_level
        self.message = message or f"User {user_id} does not have {required_level.name} permission for {resource_type.value} {resource_id}"
        super().__init__(self.message)

class RBACStorage(ABC):
    """
    Abstract base class for RBAC storage.
    """
    
    @abstractmethod
    def get_role(self, role_id: str) -> Optional[Role]:
        """
        Get a role by ID.
        
        Args:
            role_id: The ID of the role to get.
            
        Returns:
            The role, or None if not found.
        """
        pass
    
    @abstractmethod
    def get_all_roles(self) -> List[Role]:
        """
        Get all roles.
        
        Returns:
            A list of all roles.
        """
        pass
    
    @abstractmethod
    def create_role(self, role: Role) -> bool:
        """
        Create a new role.
        
        Args:
            role: The role to create.
            
        Returns:
            True if the role was created, False otherwise.
        """
        pass
    
    @abstractmethod
    def update_role(self, role: Role) -> bool:
        """
        Update a role.
        
        Args:
            role: The role to update.
            
        Returns:
            True if the role was updated, False otherwise.
        """
        pass
    
    @abstractmethod
    def delete_role(self, role_id: str) -> bool:
        """
        Delete a role.
        
        Args:
            role_id: The ID of the role to delete.
            
        Returns:
            True if the role was deleted, False otherwise.
        """
        pass
    
    @abstractmethod
    def get_user_roles(self, user_id: str) -> List[str]:
        """
        Get the roles assigned to a user.
        
        Args:
            user_id: The ID of the user.
            
        Returns:
            A list of role IDs assigned to the user.
        """
        pass
    
    @abstractmethod
    def assign_role_to_user(self, user_id: str, role_id: str) -> bool:
        """
        Assign a role to a user.
        
        Args:
            user_id: The ID of the user.
            role_id: The ID of the role.
            
        Returns:
            True if the role was assigned, False otherwise.
        """
        pass
    
    @abstractmethod
    def remove_role_from_user(self, user_id: str, role_id: str) -> bool:
        """
        Remove a role from a user.
        
        Args:
            user_id: The ID of the user.
            role_id: The ID of the role.
            
        Returns:
            True if the role was removed, False otherwise.
        """
        pass

class PermissionEvaluator:
    """
    Evaluates permissions for users based on their roles.
    """
    
    def __init__(self, rbac_storage: RBACStorage):
        """
        Initialize the permission evaluator.
        
        Args:
            rbac_storage: The RBAC storage.
        """
        self.rbac_storage = rbac_storage
        self.role_cache: Dict[str, Role] = {}
        self.user_roles_cache: Dict[str, List[str]] = {}
        self.permission_cache: Dict[str, Dict[Tuple[ResourceType, str], PermissionLevel]] = {}
        
        # Load all roles into cache
        self._refresh_role_cache()
    
    def _refresh_role_cache(self) -> None:
        """Refresh the role cache."""
        roles = self.rbac_storage.get_all_roles()
        self.role_cache = {role.id: role for role in roles}
    
    def _get_user_roles(self, user_id: str) -> List[Role]:
        """
        Get all roles assigned to a user, including parent roles.
        
        Args:
            user_id: The ID of the user.
            
        Returns:
            A list of roles assigned to the user.
        """
        # Check cache first
        if user_id in self.user_roles_cache:
            role_ids = self.user_roles_cache[user_id]
        else:
            # Get roles from storage
            role_ids = self.rbac_storage.get_user_roles(user_id)
            self.user_roles_cache[user_id] = role_ids
        
        # Get role objects
        roles = []
        processed_roles = set()
        
        def add_role_with_parents(role_id: str) -> None:
            if role_id in processed_roles:
                return
            
            processed_roles.add(role_id)
            
            role = self.role_cache.get(role_id)
            if not role:
                # Try to get from storage
                role = self.rbac_storage.get_role(role_id)
                if role:
                    self.role_cache[role_id] = role
            
            if role:
                roles.append(role)
                
                # Process parent roles
                for parent_id in role.parent_roles:
                    add_role_with_parents(parent_id)
        
        # Process all roles
        for role_id in role_ids:
            add_role_with_parents(role_id)
        
        return roles
    
    def get_permission_level(self, user_id: str, resource_type: ResourceType, 
                            resource_id: str) -> PermissionLevel:
        """
        Get the permission level for a user on a resource.
        
        Args:
            user_id: The ID of the user.
            resource_type: The type of resource.
            resource_id: The ID of the resource.
            
        Returns:
            The permission level.
        """
        # Check cache first
        cache_key = (user_id, (resource_type, resource_id))
        if user_id in self.permission_cache and cache_key in self.permission_cache[user_id]:
            return self.permission_cache[user_id][cache_key]
        
        # Get user roles
        roles = self._get_user_roles(user_id)
        
        # Find highest permission level
        highest_level = PermissionLevel.NONE
        
        for role in roles:
            for permission in role.permissions:
                # Check if permission applies to this resource
                if permission.resource_type == resource_type:
                    # Check exact match or wildcard
                    if permission.resource_id == resource_id or permission.resource_id == "*":
                        if permission.level.value > highest_level.value:
                            highest_level = permission.level
                
                # Check if permission applies to parent resources
                if self._is_parent_resource(permission.resource_type, permission.resource_id,
                                          resource_type, resource_id):
                    if permission.level.value > highest_level.value:
                        highest_level = permission.level
        
        # Cache the result
        if user_id not in self.permission_cache:
            self.permission_cache[user_id] = {}
        
        self.permission_cache[user_id][cache_key] = highest_level
        
        return highest_level
    
    def has_permission(self, user_id: str, resource_type: ResourceType, 
                      resource_id: str, required_level: PermissionLevel) -> bool:
        """
        Check if a user has the required permission level for a resource.
        
        Args:
            user_id: The ID of the user.
            resource_type: The type of resource.
            resource_id: The ID of the resource.
            required_level: The required permission level.
            
        Returns:
            True if the user has the required permission level, False otherwise.
        """
        actual_level = self.get_permission_level(user_id, resource_type, resource_id)
        return actual_level.value >= required_level.value
    
    def check_permission(self, user_id: str, resource_type: ResourceType, 
                        resource_id: str, required_level: PermissionLevel) -> None:
        """
        Check if a user has the required permission level for a resource.
        Raises PermissionDeniedError if the user does not have the required permission level.
        
        Args:
            user_id: The ID of the user.
            resource_type: The type of resource.
            resource_id: The ID of the resource.
            required_level: The required permission level.
            
        Raises:
            PermissionDeniedError: If the user does not have the required permission level.
        """
        if not self.has_permission(user_id, resource_type, resource_id, required_level):
            raise PermissionDeniedError(user_id, resource_type, resource_id, required_level)
    
    def get_accessible_resources(self, user_id: str, resource_type: ResourceType, 
                               min_level: PermissionLevel = PermissionLevel.READ) -> List[str]:
        """
        Get all resources of a specific type that a user has access to.
        
        Args:
            user_id: The ID of the user.
            resource_type: The type of resource.
            min_level: The minimum permission level required.
            
        Returns:
            A list of resource IDs that the user has access to.
        """
        # Get user roles
        roles = self._get_user_roles(user_id)
        
        # Find all resources with sufficient permission level
        accessible_resources = set()
        
        for role in roles:
            for permission in role.permissions:
                # Check if permission applies to this resource type
                if permission.resource_type == resource_type and permission.level.value >= min_level.value:
                    if permission.resource_id == "*":
                        # Wildcard permission - need to get all resources of this type
                        # This would typically be handled by the specific resource manager
                        continue
                    else:
                        accessible_resources.add(permission.resource_id)
                
                # Check if permission applies to parent resources
                elif self._is_parent_resource_type(permission.resource_type, resource_type) and permission.level.value >= min_level.value:
                    # This is a parent resource type, so the permission might apply to child resources
                    # This would typically be handled by the specific resource manager
                    continue
        
        return list(accessible_resources)
    
    def get_field_permissions(self, user_id: str, table_id: str) -> Dict[str, PermissionLevel]:
        """
        Get permissions for all fields in a table.
        
        Args:
            user_id: The ID of the user.
            table_id: The ID of the table.
            
        Returns:
            A dictionary mapping field IDs to permission levels.
        """
        # Get user roles
        roles = self._get_user_roles(user_id)
        
        # Find permissions for all fields
        field_permissions: Dict[str, PermissionLevel] = {}
        
        for role in roles:
            for permission in role.permissions:
                # Check table-level permissions
                if permission.resource_type == ResourceType.TABLE and (permission.resource_id == table_id or permission.resource_id == "*"):
                    # Table-level permission applies to all fields
                    for field_id in self._get_table_fields(table_id):
                        if field_id not in field_permissions or permission.level.value > field_permissions[field_id].value:
                            field_permissions[field_id] = permission.level
                
                # Check field-level permissions
                elif permission.resource_type == ResourceType.FIELD:
                    # Parse field ID (format: table_id.field_id)
                    parts = permission.resource_id.split(".")
                    if len(parts) == 2 and parts[0] == table_id:
                        field_id = parts[1]
                        if field_id not in field_permissions or permission.level.value > field_permissions[field_id].value:
                            field_permissions[field_id] = permission.level
        
        return field_permissions
    
    def get_record_filter(self, user_id: str, table_id: str) -> Dict[str, Any]:
        """
        Get a filter expression for records in a table based on user permissions.
        
        Args:
            user_id: The ID of the user.
            table_id: The ID of the table.
            
        Returns:
            A filter expression that can be applied to queries.
        """
        # Get user roles
        roles = self._get_user_roles(user_id)
        
        # Collect all conditions from permissions
        conditions = []
        
        for role in roles:
            for permission in role.permissions:
                # Check record-level permissions
                if permission.resource_type == ResourceType.RECORD and permission.resource_id.startswith(f"{table_id}."):
                    if permission.conditions:
                        conditions.append(permission.conditions)
        
        # Combine conditions with OR (user needs to satisfy any of the conditions)
        if conditions:
            if len(conditions) == 1:
                return conditions[0]
            else:
                return {"$or": conditions}
        
        # No specific record-level conditions
        return {}
    
    def clear_cache(self, user_id: Optional[str] = None) -> None:
        """
        Clear the permission cache.
        
        Args:
            user_id: The ID of the user to clear the cache for, or None to clear all caches.
        """
        if user_id:
            if user_id in self.user_roles_cache:
                del self.user_roles_cache[user_id]
            if user_id in self.permission_cache:
                del self.permission_cache[user_id]
        else:
            self.user_roles_cache.clear()
            self.permission_cache.clear()
            self._refresh_role_cache()
    
    def _is_parent_resource(self, parent_type: ResourceType, parent_id: str,
                          child_type: ResourceType, child_id: str) -> bool:
        """
        Check if one resource is a parent of another.
        
        Args:
            parent_type: The type of the potential parent resource.
            parent_id: The ID of the potential parent resource.
            child_type: The type of the potential child resource.
            child_id: The ID of the potential child resource.
            
        Returns:
            True if the first resource is a parent of the second, False otherwise.
        """
        # Define resource type hierarchy
        hierarchy = {
            ResourceType.SYSTEM: [ResourceType.FEATURE, ResourceType.DATA_SOURCE],
            ResourceType.DATA_SOURCE: [ResourceType.TABLE, ResourceType.COLLECTION],
            ResourceType.TABLE: [ResourceType.FIELD, ResourceType.RECORD],
            ResourceType.COLLECTION: [ResourceType.FIELD, ResourceType.RECORD]
        }
        
        # Check if child_type is a descendant of parent_type
        if parent_type not in hierarchy or child_type not in hierarchy.get(parent_type, []):
            return False
        
        # Check if child_id indicates it's a child of parent_id
        if parent_type == ResourceType.DATA_SOURCE and child_type in [ResourceType.TABLE, ResourceType.COLLECTION]:
            # Format: data_source_id.table_id
            return child_id.startswith(f"{parent_id}.")
        
        if parent_type in [ResourceType.TABLE, ResourceType.COLLECTION] and child_type == ResourceType.FIELD:
            # Format: table_id.field_id
            return child_id.startswith(f"{parent_id}.")
        
        if parent_type in [ResourceType.TABLE, ResourceType.COLLECTION] and child_type == ResourceType.RECORD:
            # Format: table_id.record_condition
            return child_id.startswith(f"{parent_id}.")
        
        return False
    
    def _is_parent_resource_type(self, parent_type: ResourceType, child_type: ResourceType) -> bool:
        """
        Check if one resource type is a parent of another.
        
        Args:
            parent_type: The potential parent resource type.
            child_type: The potential child resource type.
            
        Returns:
            True if the first resource type is a parent of the second, False otherwise.
        """
        # Define resource type hierarchy
        hierarchy = {
            ResourceType.SYSTEM: [ResourceType.FEATURE, ResourceType.DATA_SOURCE],
            ResourceType.DATA_SOURCE: [ResourceType.TABLE, ResourceType.COLLECTION],
            ResourceType.TABLE: [ResourceType.FIELD, ResourceType.RECORD],
            ResourceType.COLLECTION: [ResourceType.FIELD, ResourceType.RECORD]
        }
        
        # Check direct parent-child relationship
        if child_type in hierarchy.get(parent_type, []):
            return True
        
        # Check indirect parent-child relationship (grandparent, etc.)
        for intermediate_type in hierarchy.get(parent_type, []):
            if self._is_parent_resource_type(intermediate_type, child_type):
                return True
        
        return False
    
    def _get_table_fields(self, table_id: str) -> List[str]:
        """
        Get all fields in a table.
        
        Args:
            table_id: The ID of the table.
            
        Returns:
            A list of field IDs.
        """
        # This would typically be handled by a schema manager or similar
        # For now, return an empty list
        return [] 