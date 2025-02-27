"""
Password-based authentication provider for the AI-powered data retrieval application.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
import json
import os
import re

from .core import (
    AuthProvider, AuthResult, AuthStatus, UserSession, UserCredentials,
    PasswordHasher, TokenGenerator
)
from .storage import UserStorage, SessionStorage

logger = logging.getLogger(__name__)

class PasswordPolicy:
    """
    Password policy configuration.
    """
    
    def __init__(self, 
                min_length: int = 8,
                require_uppercase: bool = True,
                require_lowercase: bool = True,
                require_numbers: bool = True,
                require_special_chars: bool = True,
                max_age_days: int = 90,
                prevent_reuse: int = 5,
                max_failed_attempts: int = 5,
                lockout_duration_minutes: int = 30):
        """
        Initialize the password policy.
        
        Args:
            min_length: Minimum password length.
            require_uppercase: Whether to require uppercase letters.
            require_lowercase: Whether to require lowercase letters.
            require_numbers: Whether to require numbers.
            require_special_chars: Whether to require special characters.
            max_age_days: Maximum password age in days.
            prevent_reuse: Number of previous passwords to prevent reuse.
            max_failed_attempts: Maximum number of failed login attempts before lockout.
            lockout_duration_minutes: Duration of account lockout in minutes.
        """
        self.min_length = min_length
        self.require_uppercase = require_uppercase
        self.require_lowercase = require_lowercase
        self.require_numbers = require_numbers
        self.require_special_chars = require_special_chars
        self.max_age_days = max_age_days
        self.prevent_reuse = prevent_reuse
        self.max_failed_attempts = max_failed_attempts
        self.lockout_duration_minutes = lockout_duration_minutes
    
    def validate_password(self, password: str) -> Tuple[bool, Optional[str]]:
        """
        Validate a password against the policy.
        
        Args:
            password: The password to validate.
            
        Returns:
            A tuple of (is_valid, error_message).
        """
        if len(password) < self.min_length:
            return False, f"Password must be at least {self.min_length} characters long"
        
        if self.require_uppercase and not any(c.isupper() for c in password):
            return False, "Password must contain at least one uppercase letter"
        
        if self.require_lowercase and not any(c.islower() for c in password):
            return False, "Password must contain at least one lowercase letter"
        
        if self.require_numbers and not any(c.isdigit() for c in password):
            return False, "Password must contain at least one number"
        
        if self.require_special_chars and not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False, "Password must contain at least one special character"
        
        return True, None
    
    def is_password_expired(self, last_change_date: datetime) -> bool:
        """
        Check if a password is expired.
        
        Args:
            last_change_date: The date the password was last changed.
            
        Returns:
            True if the password is expired, False otherwise.
        """
        if self.max_age_days <= 0:
            return False
        
        expiry_date = last_change_date + timedelta(days=self.max_age_days)
        return datetime.now() > expiry_date
    
    def can_reuse_password(self, new_password: str, password_history: List[Dict[str, str]]) -> bool:
        """
        Check if a password can be reused.
        
        Args:
            new_password: The new password.
            password_history: The password history.
            
        Returns:
            True if the password can be reused, False otherwise.
        """
        if self.prevent_reuse <= 0:
            return True
        
        # Check only the most recent passwords up to prevent_reuse
        recent_passwords = password_history[-self.prevent_reuse:] if password_history else []
        
        for old_password in recent_passwords:
            if PasswordHasher.verify_password(
                new_password, 
                old_password["hash"], 
                old_password["salt"]
            ):
                return False
        
        return True

class PasswordAuthProvider(AuthProvider):
    """
    Password-based authentication provider.
    """
    
    def __init__(self, 
                user_storage: UserStorage,
                session_storage: SessionStorage,
                password_policy: Optional[PasswordPolicy] = None,
                session_duration_minutes: int = 60,
                remember_me_duration_days: int = 30):
        """
        Initialize the password authentication provider.
        
        Args:
            user_storage: The user storage.
            session_storage: The session storage.
            password_policy: The password policy.
            session_duration_minutes: The session duration in minutes.
            remember_me_duration_days: The "remember me" session duration in days.
        """
        self.user_storage = user_storage
        self.session_storage = session_storage
        self.password_policy = password_policy or PasswordPolicy()
        self.session_duration_minutes = session_duration_minutes
        self.remember_me_duration_days = remember_me_duration_days
        
        # Failed login attempts tracking
        self.failed_attempts = {}  # username -> {"count": int, "last_attempt": datetime}
    
    def authenticate(self, credentials: Dict[str, Any]) -> AuthResult:
        """
        Authenticate a user with the provided credentials.
        
        Args:
            credentials: The credentials to authenticate with.
            
        Returns:
            The result of the authentication attempt.
        """
        username = credentials.get("username")
        password = credentials.get("password")
        ip_address = credentials.get("ip_address")
        user_agent = credentials.get("user_agent")
        remember_me = credentials.get("remember_me", False)
        
        if not username or not password:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="Username and password are required"
            )
        
        # Check if account is locked
        if self._is_account_locked(username):
            return AuthResult(
                status=AuthStatus.LOCKED,
                message=f"Account is locked due to too many failed login attempts. Try again later."
            )
        
        # Find user by username
        user = self.user_storage.find_by_username(username)
        if not user:
            # Record failed attempt
            self._record_failed_attempt(username)
            
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="Invalid username or password"
            )
        
        # Get credentials
        user_credentials = user.get("credentials", {})
        password_hash = user_credentials.get("password_hash")
        salt = user_credentials.get("salt")
        
        if not password_hash or not salt:
            logger.error(f"User {username} has no password hash or salt")
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="Invalid username or password"
            )
        
        # Verify password
        if not PasswordHasher.verify_password(password, password_hash, salt):
            # Record failed attempt
            self._record_failed_attempt(username)
            
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="Invalid username or password"
            )
        
        # Reset failed attempts
        self._reset_failed_attempts(username)
        
        # Check if password is expired
        last_password_change = user_credentials.get("last_password_change")
        if last_password_change:
            last_change_date = datetime.fromisoformat(last_password_change)
            if self.password_policy.is_password_expired(last_change_date):
                return AuthResult(
                    status=AuthStatus.EXPIRED,
                    user_id=user["id"],
                    message="Password has expired and must be changed"
                )
        
        # Check if MFA is required
        mfa_enabled = user_credentials.get("mfa_enabled", False)
        if mfa_enabled:
            return AuthResult(
                status=AuthStatus.REQUIRES_MFA,
                user_id=user["id"],
                message="Multi-factor authentication required",
                requires_mfa=True,
                mfa_methods=["totp"]  # Could be extended to support multiple methods
            )
        
        # Create session
        session = self._create_session(user["id"], ip_address, user_agent, remember_me)
        
        # Store session
        session_dict = {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "created_at": session.created_at.isoformat(),
            "expires_at": session.expires_at.isoformat(),
            "ip_address": session.ip_address,
            "user_agent": session.user_agent,
            "is_active": session.is_active,
            "last_activity": datetime.now().isoformat(),
            "mfa_verified": session.mfa_verified,
            "metadata": session.metadata
        }
        
        if not self.session_storage.create(session.session_id, session_dict):
            logger.error(f"Failed to create session for user {username}")
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="Failed to create session"
            )
        
        return AuthResult(
            status=AuthStatus.SUCCESS,
            user_id=user["id"],
            session=session,
            message="Authentication successful"
        )
    
    def validate_session(self, session_id: str) -> bool:
        """
        Validate a user session.
        
        Args:
            session_id: The session ID to validate.
            
        Returns:
            True if the session is valid, False otherwise.
        """
        session = self.session_storage.get(session_id)
        if not session:
            return False
        
        # Check if session is active
        if not session.get("is_active", False):
            return False
        
        # Check if session has expired
        expires_at = session.get("expires_at")
        if not expires_at:
            return False
        
        expiration = datetime.fromisoformat(expires_at)
        if datetime.now() > expiration:
            return False
        
        # Update last activity
        session["last_activity"] = datetime.now().isoformat()
        self.session_storage.update(session_id, session)
        
        return True
    
    def logout(self, session_id: str) -> bool:
        """
        Log out a user session.
        
        Args:
            session_id: The session ID to log out.
            
        Returns:
            True if the logout was successful, False otherwise.
        """
        session = self.session_storage.get(session_id)
        if not session:
            return False
        
        # Mark session as inactive
        session["is_active"] = False
        
        return self.session_storage.update(session_id, session)
    
    def create_user(self, user_data: Dict[str, Any]) -> Optional[str]:
        """
        Create a new user.
        
        Args:
            user_data: The user data.
            
        Returns:
            The ID of the created user, or None if creation failed.
        """
        username = user_data.get("username")
        password = user_data.get("password")
        
        if not username or not password:
            logger.error("Username and password are required")
            return None
        
        # Check if username already exists
        existing_user = self.user_storage.find_by_username(username)
        if existing_user:
            logger.error(f"Username '{username}' already exists")
            return None
        
        # Validate password
        is_valid, error_message = self.password_policy.validate_password(password)
        if not is_valid:
            logger.error(f"Password validation failed: {error_message}")
            return None
        
        # Hash password
        password_hash, salt = PasswordHasher.hash_password(password)
        
        # Create credentials
        credentials = {
            "username": username,
            "password_hash": password_hash,
            "salt": salt,
            "last_password_change": datetime.now().isoformat(),
            "password_history": [],
            "mfa_enabled": False,
            "api_keys": []
        }
        
        # Create user
        user = {
            "username": username,
            "email": user_data.get("email"),
            "first_name": user_data.get("first_name"),
            "last_name": user_data.get("last_name"),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "credentials": credentials,
            "roles": user_data.get("roles", []),
            "is_active": True,
            "metadata": user_data.get("metadata", {})
        }
        
        return self.user_storage.create(user)
    
    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> bool:
        """
        Update a user.
        
        Args:
            user_id: The ID of the user to update.
            user_data: The updated user data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        # Get user
        user = self.user_storage.get(user_id)
        if not user:
            logger.error(f"User with ID '{user_id}' not found")
            return False
        
        # Update fields
        for key, value in user_data.items():
            if key != "credentials" and key != "id":
                user[key] = value
        
        # Update timestamp
        user["updated_at"] = datetime.now().isoformat()
        
        return self.user_storage.update(user_id, user)
    
    def delete_user(self, user_id: str) -> bool:
        """
        Delete a user.
        
        Args:
            user_id: The ID of the user to delete.
            
        Returns:
            True if the deletion was successful, False otherwise.
        """
        return self.user_storage.delete(user_id)
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by ID.
        
        Args:
            user_id: The ID of the user to get.
            
        Returns:
            The user data, or None if the user was not found.
        """
        return self.user_storage.get(user_id)
    
    def find_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Find a user by username.
        
        Args:
            username: The username to search for.
            
        Returns:
            The user data, or None if the user was not found.
        """
        return self.user_storage.find_by_username(username)
    
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
        # Get user
        user = self.user_storage.get(user_id)
        if not user:
            logger.error(f"User with ID '{user_id}' not found")
            return False
        
        # Get credentials
        credentials = user.get("credentials", {})
        password_hash = credentials.get("password_hash")
        salt = credentials.get("salt")
        
        if not password_hash or not salt:
            logger.error("User has no password set")
            return False
        
        if not PasswordHasher.verify_password(old_password, password_hash, salt):
            logger.error("Old password is incorrect")
            return False
        
        # Validate new password
        is_valid, error_message = self.password_policy.validate_password(new_password)
        if not is_valid:
            logger.error(f"Password validation failed: {error_message}")
            return False
        
        # Check password history
        password_history = credentials.get("password_history", [])
        if not self.password_policy.can_reuse_password(new_password, password_history):
            logger.error("New password cannot be the same as a recent password")
            return False
        
        # Hash new password
        new_password_hash, new_salt = PasswordHasher.hash_password(new_password)
        
        # Update password history
        password_history.append({
            "hash": password_hash,
            "salt": salt,
            "changed_at": credentials.get("last_password_change")
        })
        
        # Keep only the most recent passwords
        if len(password_history) > self.password_policy.prevent_reuse:
            password_history = password_history[-self.password_policy.prevent_reuse:]
        
        # Update credentials
        credentials["password_hash"] = new_password_hash
        credentials["salt"] = new_salt
        credentials["last_password_change"] = datetime.now().isoformat()
        credentials["password_history"] = password_history
        
        # Update user
        user["credentials"] = credentials
        user["updated_at"] = datetime.now().isoformat()
        
        return self.user_storage.update(user_id, user)
    
    def reset_password(self, user_id: str, new_password: str) -> bool:
        """
        Reset a user's password.
        
        Args:
            user_id: The ID of the user.
            new_password: The new password.
            
        Returns:
            True if the password reset was successful, False otherwise.
        """
        # Get user
        user = self.user_storage.get(user_id)
        if not user:
            logger.error(f"User with ID '{user_id}' not found")
            return False
        
        # Validate new password
        is_valid, error_message = self.password_policy.validate_password(new_password)
        if not is_valid:
            logger.error(f"Password validation failed: {error_message}")
            return False
        
        # Hash new password
        new_password_hash, new_salt = PasswordHasher.hash_password(new_password)
        
        # Update credentials
        credentials = user.get("credentials", {})
        credentials["password_hash"] = new_password_hash
        credentials["salt"] = new_salt
        credentials["last_password_change"] = datetime.now().isoformat()
        
        # Update user
        user["credentials"] = credentials
        user["updated_at"] = datetime.now().isoformat()
        
        return self.user_storage.update(user_id, user)
    
    def generate_password_reset_token(self, username: str) -> Optional[str]:
        """
        Generate a password reset token for a user.
        
        Args:
            username: The username of the user.
            
        Returns:
            The password reset token, or None if generation failed.
        """
        # Find user by username
        user = self.user_storage.find_by_username(username)
        if not user:
            logger.error(f"User with username '{username}' not found")
            return None
        
        # Generate token
        token = TokenGenerator.generate_token()
        
        # Store token with expiration
        expiration = datetime.now() + timedelta(hours=24)
        
        # Update user with reset token
        user["reset_token"] = {
            "token": token,
            "expires_at": expiration.isoformat()
        }
        
        if not self.user_storage.update(user["id"], user):
            logger.error(f"Failed to update user with reset token")
            return None
        
        return token
    
    def validate_password_reset_token(self, token: str) -> Optional[str]:
        """
        Validate a password reset token.
        
        Args:
            token: The password reset token.
            
        Returns:
            The user ID if the token is valid, None otherwise.
        """
        # Find user by reset token
        users = self.user_storage.find_by_reset_token(token)
        
        if not users or len(users) == 0:
            logger.error(f"No user found with the given reset token")
            return None
        
        user = users[0]
        
        # Check if token has expired
        reset_token = user.get("reset_token", {})
        expires_at = reset_token.get("expires_at")
        
        if not expires_at:
            logger.error(f"Reset token has no expiration")
            return None
        
        expiration = datetime.fromisoformat(expires_at)
        if datetime.now() > expiration:
            logger.error(f"Reset token has expired")
            return None
        
        return user["id"]
    
    def _create_session(self, user_id: str, ip_address: Optional[str], 
                       user_agent: Optional[str], remember_me: bool) -> UserSession:
        """
        Create a new user session.
        
        Args:
            user_id: The ID of the user.
            ip_address: The IP address of the client.
            user_agent: The user agent of the client.
            remember_me: Whether to create a long-lived session.
            
        Returns:
            The created session.
        """
        session_id = TokenGenerator.generate_session_id()
        created_at = datetime.now()
        
        # Set expiration based on remember_me
        if remember_me:
            expires_at = created_at + timedelta(days=self.remember_me_duration_days)
        else:
            expires_at = created_at + timedelta(minutes=self.session_duration_minutes)
        
        session = UserSession(
            session_id=session_id,
            user_id=user_id,
            created_at=created_at,
            expires_at=expires_at,
            ip_address=ip_address,
            user_agent=user_agent,
            is_active=True,
            last_activity=created_at,
            mfa_verified=False
        )
        
        return session
    
    def _is_account_locked(self, username: str) -> bool:
        """
        Check if an account is locked due to too many failed login attempts.
        
        Args:
            username: The username to check.
            
        Returns:
            True if the account is locked, False otherwise.
        """
        if username not in self.failed_attempts:
            return False
        
        attempts = self.failed_attempts[username]
        
        # Check if number of attempts exceeds the limit
        if attempts["count"] < self.password_policy.max_failed_attempts:
            return False
        
        # Check if lockout period has expired
        last_attempt = attempts["last_attempt"]
        lockout_expiry = last_attempt + timedelta(minutes=self.password_policy.lockout_duration_minutes)
        
        if datetime.now() > lockout_expiry:
            # Reset attempts if lockout period has expired
            self._reset_failed_attempts(username)
            return False
        
        return True
    
    def _record_failed_attempt(self, username: str) -> None:
        """
        Record a failed login attempt.
        
        Args:
            username: The username to record the attempt for.
        """
        now = datetime.now()
        
        if username not in self.failed_attempts:
            self.failed_attempts[username] = {
                "count": 1,
                "last_attempt": now
            }
        else:
            self.failed_attempts[username]["count"] += 1
            self.failed_attempts[username]["last_attempt"] = now
    
    def _reset_failed_attempts(self, username: str) -> None:
        """
        Reset failed login attempts for a username.
        
        Args:
            username: The username to reset attempts for.
        """
        if username in self.failed_attempts:
            del self.failed_attempts[username]
