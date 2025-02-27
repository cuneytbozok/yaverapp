"""
Storage implementations for RBAC.
"""

import logging
import json
import os
from typing import Dict, List, Any, Optional, Set
import uuid
from datetime import datetime

from .core import Role, Permission, RBACStorage, ResourceType, PermissionLevel

logger = logging.getLogger(__name__)

class FileRBACStorage(RBACStorage):
    """
    File-based storage for RBAC data.
    """
    
    def __init__(self, directory: str):
        """
        Initialize the file RBAC storage.
        
        Args:
            directory: The directory to store files in.
        """
        self.directory = directory
        self.roles_dir = os.path.join(directory, "roles")
        self.user_roles_dir = os.path.join(directory, "user_roles")
        
        # Create directories if they don't exist
        os.makedirs(self.roles_dir, exist_ok=True)
        os.makedirs(self.user_roles_dir, exist_ok=True)
    
    def get_role(self, role_id: str) -> Optional[Role]:
        """
        Get a role by ID.
        
        Args:
            role_id: The ID of the role to get.
            
        Returns:
            The role, or None if not found.
        """
        file_path = os.path.join(self.roles_dir, f"{role_id}.json")
        
        if not os.path.exists(file_path):
            return None
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Convert permission data to Permission objects
            permissions = []
            for perm_data in data.get("permissions", []):
                permissions.append(Permission(
                    resource_type=ResourceType(perm_data["resource_type"]),
                    resource_id=perm_data["resource_id"],
                    level=PermissionLevel(perm_data["level"]),
                    conditions=perm_data.get("conditions", {})
                ))
            
            return Role(
                id=data["id"],
                name=data["name"],
                description=data.get("description", ""),
                permissions=permissions,
                parent_roles=data.get("parent_roles", []),
                metadata=data.get("metadata", {})
            )
        except Exception as e:
            logger.error(f"Error reading role file {file_path}: {e}")
            return None
    
    def get_all_roles(self) -> List[Role]:
        """
        Get all roles.
        
        Returns:
            A list of all roles.
        """
        roles = []
        
        for filename in os.listdir(self.roles_dir):
            if not filename.endswith(".json"):
                continue
            
            role_id = filename[:-5]  # Remove .json extension
            role = self.get_role(role_id)
            
            if role:
                roles.append(role)
        
        return roles
    
    def create_role(self, role: Role) -> bool:
        """
        Create a new role.
        
        Args:
            role: The role to create.
            
        Returns:
            True if the role was created, False otherwise.
        """
        file_path = os.path.join(self.roles_dir, f"{role.id}.json")
        
        if os.path.exists(file_path):
            logger.error(f"Role with ID {role.id} already exists")
            return False
        
        try:
            # Convert Role object to dictionary
            data = {
                "id": role.id,
                "name": role.name,
                "description": role.description,
                "permissions": [
                    {
                        "resource_type": perm.resource_type.value,
                        "resource_id": perm.resource_id,
                        "level": perm.level.value,
                        "conditions": perm.conditions
                    }
                    for perm in role.permissions
                ],
                "parent_roles": role.parent_roles,
                "metadata": role.metadata
            }
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Error creating role file {file_path}: {e}")
            return False
    
    def update_role(self, role: Role) -> bool:
        """
        Update a role.
        
        Args:
            role: The role to update.
            
        Returns:
            True if the role was updated, False otherwise.
        """
        file_path = os.path.join(self.roles_dir, f"{role.id}.json")
        
        if not os.path.exists(file_path):
            logger.error(f"Role with ID {role.id} does not exist")
            return False
        
        try:
            # Convert Role object to dictionary
            data = {
                "id": role.id,
                "name": role.name,
                "description": role.description,
                "permissions": [
                    {
                        "resource_type": perm.resource_type.value,
                        "resource_id": perm.resource_id,
                        "level": perm.level.value,
                        "conditions": perm.conditions
                    }
                    for perm in role.permissions
                ],
                "parent_roles": role.parent_roles,
                "metadata": role.metadata
            }
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Error updating role file {file_path}: {e}")
            return False
    
    def delete_role(self, role_id: str) -> bool:
        """
        Delete a role.
        
        Args:
            role_id: The ID of the role to delete.
            
        Returns:
            True if the role was deleted, False otherwise.
        """
        file_path = os.path.join(self.roles_dir, f"{role_id}.json")
        
        if not os.path.exists(file_path):
            logger.error(f"Role with ID {role_id} does not exist")
            return False
        
        try:
            os.remove(file_path)
            
            # Remove role from all users
            for filename in os.listdir(self.user_roles_dir):
                if not filename.endswith(".json"):
                    continue
                
                user_id = filename[:-5]  # Remove .json extension
                self.remove_role_from_user(user_id, role_id)
            
            return True
        except Exception as e:
            logger.error(f"Error deleting role file {file_path}: {e}")
            return False
    
    def get_user_roles(self, user_id: str) -> List[str]:
        """
        Get the roles assigned to a user.
        
        Args:
            user_id: The ID of the user.
            
        Returns:
            A list of role IDs assigned to the user.
        """
        file_path = os.path.join(self.user_roles_dir, f"{user_id}.json")
        
        if not os.path.exists(file_path):
            return []
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            return data.get("roles", [])
        except Exception as e:
            logger.error(f"Error reading user roles file {file_path}: {e}")
            return []
    
    def assign_role_to_user(self, user_id: str, role_id: str) -> bool:
        """
        Assign a role to a user.
        
        Args:
            user_id: The ID of the user.
            role_id: The ID of the role.
            
        Returns:
            True if the role was assigned, False otherwise.
        """
        # Check if role exists
        if not os.path.exists(os.path.join(self.roles_dir, f"{role_id}.json")):
            logger.error(f"Role with ID {role_id} does not exist")
            return False
        
        file_path = os.path.join(self.user_roles_dir, f"{user_id}.json")
        
        try:
            # Get current roles
            roles = []
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                roles = data.get("roles", [])
            
            # Add role if not already assigned
            if role_id not in roles:
                roles.append(role_id)
            
            # Write updated roles
            with open(file_path, 'w') as f:
                json.dump({"roles": roles}, f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Error assigning role to user: {e}")
            return False
    
    def remove_role_from_user(self, user_id: str, role_id: str) -> bool:
        """
        Remove a role from a user.
        
        Args:
            user_id: The ID of the user.
            role_id: The ID of the role.
            
        Returns:
            True if the role was removed, False otherwise.
        """
        file_path = os.path.join(self.user_roles_dir, f"{user_id}.json")
        
        if not os.path.exists(file_path):
            return True  # User doesn't have any roles
        
        try:
            # Get current roles
            with open(file_path, 'r') as f:
                data = json.load(f)
            roles = data.get("roles", [])
            
            # Remove role if assigned
            if role_id in roles:
                roles.remove(role_id)
            
            # Write updated roles
            with open(file_path, 'w') as f:
                json.dump({"roles": roles}, f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Error removing role from user: {e}")
            return False 