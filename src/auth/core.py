"""
Core authentication module for the AI-powered data retrieval application.
This module defines the base interfaces and common functionality for all authentication methods.
"""

import logging
import time
import secrets
import hashlib
import base64
import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, field
import json
from datetime import datetime, timedelta
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AuthMethod(Enum):
    """Enum representing different authentication methods."""
    PASSWORD = "password"
    OAUTH = "oauth"
    SAML = "saml"
    MFA = "mfa"
    TOKEN = "token"
    API_KEY = "api_key"

class AuthStatus(Enum):
    """Enum representing authentication status."""
    SUCCESS = "success"
    FAILURE = "failure"
    PENDING = "pending"
    LOCKED = "locked"
    EXPIRED = "expired"
    REQUIRES_MFA = "requires_mfa"

@dataclass
class UserCredentials:
    """User credentials data."""
    username: str
    password_hash: Optional[str] = None
    salt: Optional[str] = None
    oauth_tokens: Dict[str, Any] = field(default_factory=dict)
    saml_attributes: Dict[str, Any] = field(default_factory=dict)
    mfa_enabled: bool = False
    mfa_secret: Optional[str] = None
    api_keys: List[Dict[str, Any]] = field(default_factory=list)
    last_password_change: Optional[datetime] = None
    password_history: List[str] = field(default_factory=list)

@dataclass
class UserSession:
    """User session data."""
    session_id: str
    user_id: str
    created_at: datetime
    expires_at: datetime
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    is_active: bool = True
    last_activity: Optional[datetime] = None
    mfa_verified: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class AuthResult:
    """Result of an authentication attempt."""
    status: AuthStatus
    user_id: Optional[str] = None
    session: Optional[UserSession] = None
    message: Optional[str] = None
    requires_mfa: bool = False
    mfa_methods: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

class AuthProvider(ABC):
    """
    Abstract base class for authentication providers.
    """
    
    @abstractmethod
    def authenticate(self, credentials: Dict[str, Any]) -> AuthResult:
        """
        Authenticate a user with the provided credentials.
        
        Args:
            credentials: The credentials to authenticate with.
            
        Returns:
            The result of the authentication attempt.
        """
        pass
    
    @abstractmethod
    def validate_session(self, session_id: str) -> bool:
        """
        Validate a user session.
        
        Args:
            session_id: The session ID to validate.
            
        Returns:
            True if the session is valid, False otherwise.
        """
        pass
    
    @abstractmethod
    def logout(self, session_id: str) -> bool:
        """
        Log out a user session.
        
        Args:
            session_id: The session ID to log out.
            
        Returns:
            True if the logout was successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def create_user(self, user_data: Dict[str, Any]) -> Optional[str]:
        """
        Create a new user.
        
        Args:
            user_data: The user data.
            
        Returns:
            The ID of the created user, or None if creation failed.
        """
        pass
    
    @abstractmethod
    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> bool:
        """
        Update a user.
        
        Args:
            user_id: The ID of the user to update.
            user_data: The updated user data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def delete_user(self, user_id: str) -> bool:
        """
        Delete a user.
        
        Args:
            user_id: The ID of the user to delete.
            
        Returns:
            True if the deletion was successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by ID.
        
        Args:
            user_id: The ID of the user to get.
            
        Returns:
            The user data, or None if the user was not found.
        """
        pass
    
    @abstractmethod
    def find_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Find a user by username.
        
        Args:
            username: The username to search for.
            
        Returns:
            The user data, or None if the user was not found.
        """
        pass
    
    @abstractmethod
    def change_password(self, user_id: str, old_password: str, new_password: str) -> bool:
        """
        Change a user's password.
        
        Args:
            user_id: The ID of the user.
            old_password: The old password.
            new_password: The new password.
            
        Returns:
            True if the password change was successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def reset_password(self, user_id: str, new_password: str) -> bool:
        """
        Reset a user's password.
        
        Args:
            user_id: The ID of the user.
            new_password: The new password.
            
        Returns:
            True if the password reset was successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def generate_password_reset_token(self, username: str) -> Optional[str]:
        """
        Generate a password reset token for a user.
        
        Args:
            username: The username of the user.
            
        Returns:
            The password reset token, or None if generation failed.
        """
        pass
    
    @abstractmethod
    def validate_password_reset_token(self, token: str) -> Optional[str]:
        """
        Validate a password reset token.
        
        Args:
            token: The password reset token.
            
        Returns:
            The user ID if the token is valid, None otherwise.
        """
        pass

class PasswordHasher:
    """
    Utility class for password hashing and verification.
    """
    
    @staticmethod
    def generate_salt() -> str:
        """
        Generate a random salt.
        
        Returns:
            A random salt as a base64-encoded string.
        """
        return base64.b64encode(os.urandom(32)).decode('utf-8')
    
    @staticmethod
    def hash_password(password: str, salt: Optional[str] = None) -> Tuple[str, str]:
        """
        Hash a password with a salt.
        
        Args:
            password: The password to hash.
            salt: The salt to use, or None to generate a new one.
            
        Returns:
            A tuple of (password_hash, salt).
        """
        if salt is None:
            salt = PasswordHasher.generate_salt()
        else:
            # Ensure salt is a string
            salt = str(salt)
        
        # Use a strong hashing algorithm (Argon2 would be better, but using PBKDF2 for compatibility)
        key = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000  # Number of iterations
        )
        
        password_hash = base64.b64encode(key).decode('utf-8')
        
        return password_hash, salt
    
    @staticmethod
    def verify_password(password: str, password_hash: str, salt: str) -> bool:
        """
        Verify a password against a hash.
        
        Args:
            password: The password to verify.
            password_hash: The hash to verify against.
            salt: The salt used to generate the hash.
            
        Returns:
            True if the password matches the hash, False otherwise.
        """
        calculated_hash, _ = PasswordHasher.hash_password(password, salt)
        
        # Use constant-time comparison to prevent timing attacks
        return secrets.compare_digest(calculated_hash, password_hash)

class TokenGenerator:
    """
    Utility class for generating and validating tokens.
    """
    
    @staticmethod
    def generate_token(length: int = 32) -> str:
        """
        Generate a random token.
        
        Args:
            length: The length of the token in bytes.
            
        Returns:
            A random token as a hex string.
        """
        return secrets.token_hex(length)
    
    @staticmethod
    def generate_session_id() -> str:
        """
        Generate a session ID.
        
        Returns:
            A random session ID.
        """
        return str(uuid.uuid4())
    
    @staticmethod
    def generate_api_key() -> str:
        """
        Generate an API key.
        
        Returns:
            A random API key.
        """
        # Format: prefix.random_string
        prefix = "api_"
        random_part = secrets.token_urlsafe(32)
        
        return f"{prefix}{random_part}" 