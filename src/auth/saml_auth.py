"""
SAML authentication provider for the AI-powered data retrieval application.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
import json
import os
import re
import base64
import secrets
import hashlib
from urllib.parse import urlencode
import xml.etree.ElementTree as ET
import zlib

from .core import (
    AuthProvider, AuthResult, AuthStatus, UserSession, UserCredentials,
    TokenGenerator
)
from .storage import UserStorage, SessionStorage

try:
    import saml2
    from saml2.client import Saml2Client
    from saml2.config import Config as Saml2Config
    from saml2.metadata import entity_descriptor
    SAML_AVAILABLE = True
except ImportError:
    SAML_AVAILABLE = False

logger = logging.getLogger(__name__)

class SAMLProvider:
    """
    Configuration for a SAML provider.
    """
    
    def __init__(self, 
                provider_id: str,
                name: str,
                metadata_url: Optional[str] = None,
                metadata_file: Optional[str] = None,
                entity_id: str = "",
                acs_url: str = "",
                attribute_mapping: Optional[Dict[str, str]] = None,
                additional_params: Optional[Dict[str, Any]] = None):
        """
        Initialize the SAML provider configuration.
        
        Args:
            provider_id: The provider ID.
            name: The provider name.
            metadata_url: The metadata URL.
            metadata_file: The metadata file path.
            entity_id: The entity ID.
            acs_url: The Assertion Consumer Service URL.
            attribute_mapping: Mapping of SAML attributes to user attributes.
            additional_params: Additional parameters for the SAML flow.
        """
        self.provider_id = provider_id
        self.name = name
        self.metadata_url = metadata_url
        self.metadata_file = metadata_file
        self.entity_id = entity_id
        self.acs_url = acs_url
        self.attribute_mapping = attribute_mapping or {
            "NameID": "username",
            "email": "email",
            "givenName": "first_name",
            "surname": "last_name"
        }
        self.additional_params = additional_params or {}

class SAMLAuthProvider(AuthProvider):
    """
    SAML authentication provider.
    """
    
    def __init__(self, 
                user_storage: UserStorage,
                session_storage: SessionStorage,
                saml_providers: List[SAMLProvider],
                session_duration_minutes: int = 60):
        """
        Initialize the SAML authentication provider.
        
        Args:
            user_storage: The user storage.
            session_storage: The session storage.
            saml_providers: The SAML providers.
            session_duration_minutes: The session duration in minutes.
        """
        if not SAML_AVAILABLE:
            logger.warning("SAML support is not available. Install python3-saml package.")
        
        self.user_storage = user_storage
        self.session_storage = session_storage
        self.saml_providers = {provider.provider_id: provider for provider in saml_providers}
        self.session_duration_minutes = session_duration_minutes
        
        # SAML clients
        self.saml_clients = {}
        
        # Initialize SAML clients
        for provider_id, provider in self.saml_providers.items():
            self._init_saml_client(provider)
    
    def _init_saml_client(self, provider: SAMLProvider) -> None:
        """
        Initialize a SAML client for a provider.
        
        Args:
            provider: The SAML provider.
        """
        if not SAML_AVAILABLE:
            return
        
        try:
            config = Saml2Config()
            config.load({
                "entityid": provider.entity_id,
                "service": {
                    "sp": {
                        "endpoints": {
                            "assertion_consumer_service": [
                                (provider.acs_url, saml2.BINDING_HTTP_POST)
                            ],
                        },
                        "allow_unsolicited": True,
                        "authn_requests_signed": False,
                        "want_assertions_signed": True,
                        "want_response_signed": True,
                    },
                },
                "metadata": {
                    "remote": [{"url": provider.metadata_url}] if provider.metadata_url else [],
                    "local": [provider.metadata_file] if provider.metadata_file else []
                },
                "debug": True,
                "key_file": provider.additional_params.get("key_file"),
                "cert_file": provider.additional_params.get("cert_file"),
            })
            
            self.saml_clients[provider.provider_id] = Saml2Client(config=config)
            
        except Exception as e:
            logger.error(f"Error initializing SAML client for {provider.provider_id}: {e}")
    
    def authenticate(self, credentials: Dict[str, Any]) -> AuthResult:
        """
        Authenticate a user with the provided credentials.
        
        Args:
            credentials: The credentials to authenticate with.
            
        Returns:
            The result of the authentication attempt.
        """
        # SAML authentication is handled through the SAML flow
        # This method is used for direct authentication (not typically used with SAML)
        
        return AuthResult(
            status=AuthStatus.FAILURE,
            message="Direct authentication not supported for SAML"
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
        
        try:
            expiration = datetime.fromisoformat(expires_at)
            if datetime.now() > expiration:
                return False
        except Exception as e:
            logger.error(f"Error parsing expiration date: {e}")
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
        # SAML users are created through the SAML flow
        return None
    
    def update_user(self, user_id: str, user_data: Dict[str, Any]) -> bool:
        """
        Update a user.
        
        Args:
            user_id: The ID of the user to update.
            user_data: The updated user data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        # Get existing user
        user = self.user_storage.get(user_id)
        if not user:
            return False
        
        # Update user data
        for key, value in user_data.items():
            user[key] = value
        
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
    
    def change_password(self, user_id: str, new_password: str) -> bool:
        """
        Change a user's password.
        
        Args:
            user_id: The ID of the user.
            new_password: The new password.
            
        Returns:
            True if the password change was successful, False otherwise.
        """
        # Password changes are not supported for SAML users
        return False
    
    def reset_password(self, user_id: str, new_password: str) -> bool:
        """
        Reset a user's password.
        
        Args:
            user_id: The ID of the user.
            new_password: The new password.
            
        Returns:
            True if the password reset was successful, False otherwise.
        """
        # Password resets are not supported for SAML users
        return False
    
    def generate_password_reset_token(self, username: str) -> Optional[str]:
        """
        Generate a password reset token for a user.
        
        Args:
            username: The username of the user.
            
        Returns:
            The password reset token, or None if generation failed.
        """
        # Password reset tokens are not supported for SAML users
        return None
    
    def validate_password_reset_token(self, token: str) -> Optional[str]:
        """
        Validate a password reset token.
        
        Args:
            token: The password reset token.
            
        Returns:
            The user ID if the token is valid, None otherwise.
        """
        # Password reset tokens are not supported for SAML users
        return None
    
    def generate_auth_request(self, provider_id: str, relay_state: Optional[str] = None) -> Tuple[str, str]:
        """
        Generate a SAML authentication request.
        
        Args:
            provider_id: The provider ID.
            relay_state: Optional relay state.
            
        Returns:
            A tuple of (auth_request_url, relay_state).
        """
        if not SAML_AVAILABLE:
            raise RuntimeError("SAML support is not available")
        
        provider = self.saml_providers.get(provider_id)
        if not provider:
            raise ValueError(f"Unknown SAML provider: {provider_id}")
        
        client = self.saml_clients.get(provider_id)
        if not client:
            raise ValueError(f"SAML client not initialized for provider: {provider_id}")
        
        # Generate relay state if not provided
        if relay_state is None:
            relay_state = TokenGenerator.generate_token()
        
        # Generate authentication request
        reqid, info = client.prepare_for_authenticate(relay_state=relay_state)
        
        # Get the URL for the authentication request
        auth_request_url = info["url"]
        
        return auth_request_url, relay_state
    
    def handle_response(self, provider_id: str, saml_response: str, relay_state: Optional[str] = None) -> AuthResult:
        """
        Handle a SAML response.
        
        Args:
            provider_id: The provider ID.
            saml_response: The SAML response.
            relay_state: Optional relay state.
            
        Returns:
            The result of the authentication attempt.
        """
        if not SAML_AVAILABLE:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="SAML support is not available"
            )
        
        provider = self.saml_providers.get(provider_id)
        if not provider:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message=f"Unknown SAML provider: {provider_id}"
            )
        
        client = self.saml_clients.get(provider_id)
        if not client:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message=f"SAML client not initialized for provider: {provider_id}"
            )
        
        try:
            # Process the SAML response
            authn_response = client.parse_authn_request_response(
                saml_response,
                saml2.BINDING_HTTP_POST
            )
            
            if authn_response is None:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message="Invalid SAML response"
                )
            
            # Get the identity from the response
            identity = authn_response.get_identity()
            if not identity:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message="No identity in SAML response"
                )
            
            # Get the name ID from the response
            name_id = authn_response.get_subject().text
            if not name_id:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message="No name ID in SAML response"
                )
            
            # Map SAML attributes to user attributes
            user_attrs = {}
            for saml_attr, user_attr in provider.attribute_mapping.items():
                if saml_attr == "NameID":
                    user_attrs[user_attr] = name_id
                elif saml_attr in identity:
                    user_attrs[user_attr] = identity[saml_attr][0]
            
            # Find or create user
            user = self._find_or_create_user(provider_id, name_id, user_attrs)
            
            if not user:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message="Failed to find or create user"
                )
            
            # Create session
            session = self._create_session(user["id"], None, None)
            
            # Store session
            session_dict = {
                "session_id": session.session_id,
                "user_id": session.user_id,
                "created_at": session.created_at.isoformat(),
                "expires_at": session.expires_at.isoformat(),
                "ip_address": session.ip_address,
                "user_agent": session.user_agent,
                "is_active": session.is_active,
                "last_activity": session.last_activity.isoformat() if session.last_activity else None,
                "mfa_verified": session.mfa_verified,
                "metadata": session.metadata
            }
            
            if not self.session_storage.create(session_dict):
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
            
        except Exception as e:
            logger.error(f"Error handling SAML response: {e}")
            
            return AuthResult(
                status=AuthStatus.FAILURE,
                message=f"SAML response error: {str(e)}"
            )
    
    def _find_or_create_user(self, provider_id: str, name_id: str, attributes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Find or create a user based on SAML attributes.
        
        Args:
            provider_id: The provider ID.
            name_id: The SAML name ID.
            attributes: The SAML attributes.
            
        Returns:
            The user data, or None if not found or created.
        """
        # Try to find existing user
        users = self.user_storage.list({
            f"saml_providers.{provider_id}.name_id": name_id
        })
        
        if users:
            user = users[0]
            
            # Update SAML attributes
            if "saml_providers" not in user:
                user["saml_providers"] = {}
            
            if provider_id not in user["saml_providers"]:
                user["saml_providers"][provider_id] = {}
            
            user["saml_providers"][provider_id]["attributes"] = attributes
            user["saml_providers"][provider_id]["updated_at"] = datetime.now().isoformat()
            
            self.user_storage.update(user["id"], user)
            
            return user
        
        # Create new user
        username = attributes.get("username", name_id)
        email = attributes.get("email")
        
        user_data = {
            "username": username,
            "email": email,
            "name": f"{attributes.get('first_name', '')} {attributes.get('last_name', '')}".strip(),
            "saml_providers": {
                provider_id: {
                    "name_id": name_id,
                    "attributes": attributes,
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
            },
            "is_active": True,
            "roles": ["user"]
        }
        
        user_id = self.user_storage.create(None, user_data)
        
        if not user_id:
            return None
        
        return self.user_storage.get(user_id)
    
    def _create_session(self, user_id: str, ip_address: Optional[str], user_agent: Optional[str]) -> UserSession:
        """
        Create a new user session.
        
        Args:
            user_id: The ID of the user.
            ip_address: The IP address of the client.
            user_agent: The user agent of the client.
            
        Returns:
            The created session.
        """
        session_id = TokenGenerator.generate_session_id()
        created_at = datetime.now()
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