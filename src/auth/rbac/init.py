"""
Initialization utilities for RBAC.
"""

import logging
import os
import json
from typing import Dict, List, Any, Optional

from .core import Role, Permission, ResourceType, PermissionLevel
from .manager import RBACManager

logger = logging.getLogger(__name__)

def create_default_roles(rbac_manager: RBACManager) -> None:
    """
    Create default roles.
    
    Args:
        rbac_manager: The RBAC manager.
    """
    # Admin role
    admin_role = Role(
        id="admin",
        name="Administrator",
        description="Full administrative access to all resources",
        permissions=[
            Permission(
                resource_type=ResourceType.SYSTEM,
                resource_id="system",
                level=PermissionLevel.ADMIN
            )
        ]
    )
    
    # Data Scientist role
    data_scientist_role = Role(
        id="data_scientist",
        name="Data Scientist",
        description="Read/write access to all data sources",
        permissions=[
            Permission(
                resource_type=ResourceType.DATA_SOURCE,
                resource_id="*",
                level=PermissionLevel.WRITE
            )
        ]
    )
    
    # Analyst role
    analyst_role = Role(
        id="analyst",
        name="Analyst",
        description="Read-only access to all data sources",
        permissions=[
            Permission(
                resource_type=ResourceType.DATA_SOURCE,
                resource_id="*",
                level=PermissionLevel.READ
            )
        ]
    )
    
    # Create roles
    rbac_manager.create_role(admin_role, "system", None, None)
    rbac_manager.create_role(data_scientist_role, "system", None, None)
    rbac_manager.create_role(analyst_role, "system", None, None)

def init_rbac_from_config(config_file: str) -> RBACManager:
    """
    Initialize RBAC from a configuration file.
    
    Args:
        config_file: The path to the configuration file.
        
    Returns:
        The initialized RBAC manager.
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        rbac_manager = RBACManager(config)
        
        # Create default roles if enabled
        if config.get("create_default_roles", True):
            create_default_roles(rbac_manager)
        
        return rbac_manager
    
    except Exception as e:
        logger.error(f"Error initializing RBAC: {e}")
        raise 