"""
Storage providers for authentication data.
"""

import logging
import json
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)

class StorageProvider(ABC):
    """
    Abstract base class for storage providers.
    """
    
    @abstractmethod
    def get(self, id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by ID.
        
        Args:
            id: The ID of the item to get.
            
        Returns:
            The item, or None if not found.
        """
        pass
    
    @abstractmethod
    def create(self, id: Optional[str], data: Dict[str, Any]) -> Optional[str]:
        """
        Create a new item.
        
        Args:
            id: The ID of the item, or None to generate one.
            data: The item data.
            
        Returns:
            The ID of the created item, or None if creation failed.
        """
        pass
    
    @abstractmethod
    def update(self, id: str, data: Dict[str, Any]) -> bool:
        """
        Update an item.
        
        Args:
            id: The ID of the item to update.
            data: The updated item data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def delete(self, id: str) -> bool:
        """
        Delete an item.
        
        Args:
            id: The ID of the item to delete.
            
        Returns:
            True if the deletion was successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def list(self, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List items, optionally filtered.
        
        Args:
            filter: Optional filter criteria.
            
        Returns:
            A list of items.
        """
        pass

class FileStorageProvider(StorageProvider):
    """
    File-based storage provider.
    """
    
    def __init__(self, directory: str):
        """
        Initialize the file storage provider.
        
        Args:
            directory: The directory to store files in.
        """
        self.directory = directory
        
        # Create directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)
    
    def get(self, id: str) -> Optional[Dict[str, Any]]:
        """
        Get an item by ID.
        
        Args:
            id: The ID of the item to get.
            
        Returns:
            The item, or None if not found.
        """
        file_path = os.path.join(self.directory, f"{id}.json")
        
        if not os.path.exists(file_path):
            return None
        
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return None
    
    def create(self, id: Optional[str], data: Dict[str, Any]) -> Optional[str]:
        """
        Create a new item.
        
        Args:
            id: The ID of the item, or None to generate one.
            data: The item data.
            
        Returns:
            The ID of the created item, or None if creation failed.
        """
        if id is None:
            id = str(uuid.uuid4())
        
        # Add ID to data
        data["id"] = id
        
        file_path = os.path.join(self.directory, f"{id}.json")
        
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            return id
        except Exception as e:
            logger.error(f"Error writing file {file_path}: {e}")
            return None
    
    def update(self, id: str, data: Dict[str, Any]) -> bool:
        """
        Update an item.
        
        Args:
            id: The ID of the item to update.
            data: The updated item data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        # Ensure ID in data matches
        data["id"] = id
        
        file_path = os.path.join(self.directory, f"{id}.json")
        
        if not os.path.exists(file_path):
            return False
        
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error writing file {file_path}: {e}")
            return False
    
    def delete(self, id: str) -> bool:
        """
        Delete an item.
        
        Args:
            id: The ID of the item to delete.
            
        Returns:
            True if the deletion was successful, False otherwise.
        """
        file_path = os.path.join(self.directory, f"{id}.json")
        
        if not os.path.exists(file_path):
            return False
        
        try:
            os.remove(file_path)
            return True
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {e}")
            return False
    
    def list(self, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List items, optionally filtered.
        
        Args:
            filter: Optional filter criteria.
            
        Returns:
            A list of items.
        """
        items = []
        
        for filename in os.listdir(self.directory):
            if not filename.endswith(".json"):
                continue
            
            file_path = os.path.join(self.directory, filename)
            
            try:
                with open(file_path, 'r') as f:
                    item = json.load(f)
                
                # Apply filter if provided
                if filter:
                    match = True
                    for key, value in filter.items():
                        if key not in item or item[key] != value:
                            match = False
                            break
                    
                    if not match:
                        continue
                
                items.append(item)
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")
        
        return items

class UserStorage:
    """
    Storage for user data.
    """
    
    def __init__(self, storage_provider: StorageProvider):
        """
        Initialize the user storage.
        
        Args:
            storage_provider: The storage provider to use.
        """
        self.storage_provider = storage_provider
    
    def get(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by ID.
        
        Args:
            user_id: The ID of the user to get.
            
        Returns:
            The user data, or None if not found.
        """
        return self.storage_provider.get(user_id)
    
    def create(self, user_data: Dict[str, Any]) -> Optional[str]:
        """
        Create a new user.
        
        Args:
            user_data: The user data.
            
        Returns:
            The ID of the created user, or None if creation failed.
        """
        # Add created_at timestamp
        user_data["created_at"] = datetime.now().isoformat()
        user_data["updated_at"] = user_data["created_at"]
        
        return self.storage_provider.create(None, user_data)
    
    def update(self, user_id: str, user_data: Dict[str, Any]) -> bool:
        """
        Update a user.
        
        Args:
            user_id: The ID of the user to update.
            user_data: The updated user data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        # Update updated_at timestamp
        user_data["updated_at"] = datetime.now().isoformat()
        
        return self.storage_provider.update(user_id, user_data)
    
    def delete(self, user_id: str) -> bool:
        """
        Delete a user.
        
        Args:
            user_id: The ID of the user to delete.
            
        Returns:
            True if the deletion was successful, False otherwise.
        """
        return self.storage_provider.delete(user_id)
    
    def list(self, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List users, optionally filtered.
        
        Args:
            filter: Optional filter criteria.
            
        Returns:
            A list of users.
        """
        return self.storage_provider.list(filter)
    
    def find_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Find a user by username.
        
        Args:
            username: The username to search for.
            
        Returns:
            The user data, or None if not found.
        """
        users = self.storage_provider.list({"username": username})
        
        if not users:
            return None
        
        return users[0]
    
    def find_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Find a user by email.
        
        Args:
            email: The email to search for.
            
        Returns:
            The user data, or None if not found.
        """
        users = self.storage_provider.list({"email": email})
        
        if not users:
            return None
        
        return users[0]
    
    def find_by_reset_token(self, token: str) -> List[Dict[str, Any]]:
        """
        Find users by reset token.
        
        Args:
            token: The reset token to search for.
            
        Returns:
            A list of matching users.
        """
        all_users = self.storage_provider.list()
        matching_users = []
        
        for user in all_users:
            reset_token = user.get("reset_token", {})
            if reset_token.get("token") == token:
                matching_users.append(user)
        
        return matching_users

class SessionStorage:
    """
    Storage for session data.
    """
    
    def __init__(self, storage_provider: StorageProvider):
        """
        Initialize the session storage.
        
        Args:
            storage_provider: The storage provider to use.
        """
        self.storage_provider = storage_provider
    
    def get(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a session by ID.
        
        Args:
            session_id: The ID of the session to get.
            
        Returns:
            The session data, or None if not found.
        """
        return self.storage_provider.get(session_id)
    
    def create(self, session_data: Dict[str, Any]) -> Optional[str]:
        """
        Create a new session.
        
        Args:
            session_data: The session data.
            
        Returns:
            The ID of the created session, or None if creation failed.
        """
        session_id = session_data.get("session_id")
        
        return self.storage_provider.create(session_id, session_data)
    
    def update(self, session_id: str, session_data: Dict[str, Any]) -> bool:
        """
        Update a session.
        
        Args:
            session_id: The ID of the session to update.
            session_data: The updated session data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        return self.storage_provider.update(session_id, session_data)
    
    def delete(self, session_id: str) -> bool:
        """
        Delete a session.
        
        Args:
            session_id: The ID of the session to delete.
            
        Returns:
            True if the deletion was successful, False otherwise.
        """
        return self.storage_provider.delete(session_id)
    
    def list(self, filter: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List sessions, optionally filtered.
        
        Args:
            filter: Optional filter criteria.
            
        Returns:
            A list of sessions.
        """
        return self.storage_provider.list(filter)
    
    def find_by_user_id(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Find sessions by user ID.
        
        Args:
            user_id: The user ID to search for.
            
        Returns:
            A list of matching sessions.
        """
        return self.storage_provider.list({"user_id": user_id})
    
    def cleanup_expired_sessions(self) -> int:
        """
        Clean up expired sessions.
        
        Returns:
            The number of sessions deleted.
        """
        all_sessions = self.storage_provider.list()
        now = datetime.now()
        deleted_count = 0
        
        for session in all_sessions:
            expires_at = session.get("expires_at")
            
            if expires_at:
                try:
                    expiration = datetime.fromisoformat(expires_at)
                    
                    if now > expiration:
                        if self.storage_provider.delete(session["id"]):
                            deleted_count += 1
                except Exception as e:
                    logger.error(f"Error parsing expiration date: {e}")
        
        return deleted_count 