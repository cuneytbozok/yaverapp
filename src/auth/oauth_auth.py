"""
OAuth 2.0 authentication provider for the AI-powered data retrieval application.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
import json
import os
import re
import requests
import base64
import secrets
import hashlib
from urllib.parse import urlencode

from .core import (
    AuthProvider, AuthResult, AuthStatus, UserSession, UserCredentials,
    TokenGenerator
)
from .storage import UserStorage, SessionStorage

logger = logging.getLogger(__name__)

class OAuthProvider:
    """
    Configuration for an OAuth provider.
    """
    
    def __init__(self, 
                provider_id: str,
                name: str,
                client_id: str,
                client_secret: str,
                authorize_url: str,
                token_url: str,
                userinfo_url: str,
                scope: str,
                redirect_uri: str,
                additional_params: Optional[Dict[str, Any]] = None):
        """
        Initialize the OAuth provider configuration.
        
        Args:
            provider_id: The provider ID.
            name: The provider name.
            client_id: The OAuth client ID.
            client_secret: The OAuth client secret.
            authorize_url: The authorization URL.
            token_url: The token URL.
            userinfo_url: The user info URL.
            scope: The OAuth scope.
            redirect_uri: The redirect URI.
            additional_params: Additional parameters for the OAuth flow.
        """
        self.provider_id = provider_id
        self.name = name
        self.client_id = client_id
        self.client_secret = client_secret
        self.authorize_url = authorize_url
        self.token_url = token_url
        self.userinfo_url = userinfo_url
        self.scope = scope
        self.redirect_uri = redirect_uri
        self.additional_params = additional_params or {}

class OAuthAuthProvider(AuthProvider):
    """
    OAuth 2.0 authentication provider.
    """
    
    def __init__(self, 
                user_storage: UserStorage,
                session_storage: SessionStorage,
                oauth_providers: List[OAuthProvider],
                session_duration_minutes: int = 60):
        """
        Initialize the OAuth authentication provider.
        
        Args:
            user_storage: The user storage.
            session_storage: The session storage.
            oauth_providers: The OAuth providers.
            session_duration_minutes: The session duration in minutes.
        """
        self.user_storage = user_storage
        self.session_storage = session_storage
        self.oauth_providers = {provider.provider_id: provider for provider in oauth_providers}
        self.session_duration_minutes = session_duration_minutes
        
        # State and PKCE storage
        self.state_storage = {}  # state -> {"provider_id": str, "created_at": datetime, "redirect_uri": str, "code_verifier": str}
    
    def authenticate(self, credentials: Dict[str, Any]) -> AuthResult:
        """
        Authenticate a user with the provided credentials.
        
        Args:
            credentials: The credentials to authenticate with.
            
        Returns:
            The result of the authentication attempt.
        """
        # OAuth authentication is handled through the OAuth flow
        # This method is used for token-based authentication
        
        access_token = credentials.get("access_token")
        provider_id = credentials.get("provider_id")
        ip_address = credentials.get("ip_address")
        user_agent = credentials.get("user_agent")
        
        if not access_token or not provider_id:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="Access token and provider ID are required"
            )
        
        # Get provider
        provider = self.oauth_providers.get(provider_id)
        if not provider:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message=f"Unknown OAuth provider: {provider_id}"
            )
        
        try:
            # Get user info
            userinfo = self._get_userinfo(provider, access_token)
            
            if not userinfo:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message="Failed to get user info"
                )
            
            # Get user ID from provider-specific field
            user_id_field = self._get_user_id_field(provider_id)
            provider_user_id = userinfo.get(user_id_field)
            
            if not provider_user_id:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message=f"User info does not contain {user_id_field}"
                )
            
            # Find or create user
            user = self._find_or_create_user(provider_id, provider_user_id, userinfo)
            
            if not user:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message="Failed to find or create user"
                )
            
            # Create session
            session = self._create_session(user["id"], ip_address, user_agent)
            
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
            logger.error(f"Error authenticating with OAuth: {e}")
            
            return AuthResult(
                status=AuthStatus.FAILURE,
                message=f"Authentication error: {str(e)}"
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
        # OAuth users are created through the OAuth flow
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
        user = self.user_storage.get(user_id)
        
        if not user:
            return False
        
        # Update user data
        for key, value in user_data.items():
            if key not in ["id", "created_at", "credentials"]:
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
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by ID.
        
        Args:
            user_id: The ID of the user to get.
            
        Returns:
            The user data, or None if not found.
        """
        return self.user_storage.get(user_id)
    
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
        # OAuth users don't have passwords
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
        # OAuth users don't have passwords
        return False
    
    def generate_password_reset_token(self, username: str) -> Optional[str]:
        """
        Generate a password reset token for a user.
        
        Args:
            username: The username of the user.
            
        Returns:
            The password reset token, or None if generation failed.
        """
        # OAuth users don't have passwords
        return None
    
    def validate_password_reset_token(self, token: str) -> Optional[str]:
        """
        Validate a password reset token.
        
        Args:
            token: The password reset token.
            
        Returns:
            The user ID if the token is valid, None otherwise.
        """
        # OAuth users don't have passwords
        return None
    
    def generate_authorization_url(self, provider_id: str, redirect_uri: Optional[str] = None) -> Tuple[str, str]:
        """
        Generate an authorization URL for an OAuth provider.
        
        Args:
            provider_id: The provider ID.
            redirect_uri: Optional override for the redirect URI.
            
        Returns:
            A tuple of (authorization_url, state).
        """
        provider = self.oauth_providers.get(provider_id)
        if not provider:
            raise ValueError(f"Unknown OAuth provider: {provider_id}")
        
        # Generate state
        state = secrets.token_urlsafe(32)
        
        # Generate PKCE code verifier and challenge
        code_verifier = secrets.token_urlsafe(64)
        code_challenge = self._generate_code_challenge(code_verifier)
        
        # Store state
        self.state_storage[state] = {
            "provider_id": provider_id,
            "created_at": datetime.now(),
            "redirect_uri": redirect_uri or provider.redirect_uri,
            "code_verifier": code_verifier
        }
        
        # Build authorization URL
        params = {
            "client_id": provider.client_id,
            "redirect_uri": redirect_uri or provider.redirect_uri,
            "response_type": "code",
            "scope": provider.scope,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256"
        }
        
        # Add additional params
        params.update(provider.additional_params)
        
        authorization_url = f"{provider.authorize_url}?{urlencode(params)}"
        
        return authorization_url, state
    
    def handle_callback(self, provider_id: str, code: str, state: str, redirect_uri: Optional[str] = None) -> AuthResult:
        """
        Handle an OAuth callback.
        
        Args:
            provider_id: The provider ID.
            code: The authorization code.
            state: The state parameter.
            redirect_uri: Optional override for the redirect URI.
            
        Returns:
            The result of the authentication attempt.
        """
        # Validate state
        if state not in self.state_storage:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="Invalid state parameter"
            )
        
        state_data = self.state_storage[state]
        
        # Check if state has expired (10 minutes)
        if datetime.now() - state_data["created_at"] > timedelta(minutes=10):
            del self.state_storage[state]
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="State parameter has expired"
            )
        
        # Check if provider matches
        if state_data["provider_id"] != provider_id:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message="Provider mismatch"
            )
        
        # Get provider
        provider = self.oauth_providers.get(provider_id)
        if not provider:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message=f"Unknown OAuth provider: {provider_id}"
            )
        
        try:
            # Exchange code for tokens
            token_data = self._exchange_code(
                provider, 
                code, 
                redirect_uri or state_data["redirect_uri"],
                state_data["code_verifier"]
            )
            
            if not token_data or "access_token" not in token_data:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message="Failed to exchange code for tokens"
                )
            
            # Get user info
            userinfo = self._get_userinfo(provider, token_data["access_token"])
            
            if not userinfo:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message="Failed to get user info"
                )
            
            # Get user ID from provider-specific field
            user_id_field = self._get_user_id_field(provider_id)
            provider_user_id = userinfo.get(user_id_field)
            
            if not provider_user_id:
                return AuthResult(
                    status=AuthStatus.FAILURE,
                    message=f"User info does not contain {user_id_field}"
                )
            
            # Find or create user
            user = self._find_or_create_user(provider_id, provider_user_id, userinfo, token_data)
            
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
            
            # Clean up state
            del self.state_storage[state]
            
            return AuthResult(
                status=AuthStatus.SUCCESS,
                user_id=user["id"],
                session=session,
                message="Authentication successful"
            )
            
        except Exception as e:
            logger.error(f"Error handling OAuth callback: {e}")
            
            return AuthResult(
                status=AuthStatus.FAILURE,
                message=f"Callback error: {str(e)}"
            )
    
    def _exchange_code(self, provider: OAuthProvider, code: str, redirect_uri: str, code_verifier: str) -> Dict[str, Any]:
        """
        Exchange an authorization code for tokens.
        
        Args:
            provider: The OAuth provider.
            code: The authorization code.
            redirect_uri: The redirect URI.
            code_verifier: The PKCE code verifier.
            
        Returns:
            The token data.
        """
        params = {
            "client_id": provider.client_id,
            "client_secret": provider.client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
            "code_verifier": code_verifier
        }
        
        response = requests.post(provider.token_url, data=params, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"Error exchanging code: {response.text}")
            return {}
        
        return response.json()
    
    def _get_userinfo(self, provider: OAuthProvider, access_token: str) -> Dict[str, Any]:
        """
        Get user info from an OAuth provider.
        
        Args:
            provider: The OAuth provider.
            access_token: The access token.
            
        Returns:
            The user info.
        """
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        
        response = requests.get(provider.userinfo_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"Error getting user info: {response.text}")
            return {}
        
        return response.json()
    
    def _find_or_create_user(self, provider_id: str, provider_user_id: str, 
                           userinfo: Dict[str, Any], token_data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Find or create a user based on OAuth user info.
        
        Args:
            provider_id: The provider ID.
            provider_user_id: The provider-specific user ID.
            userinfo: The user info from the provider.
            token_data: Optional token data.
            
        Returns:
            The user data, or None if not found or created.
        """
        # Try to find existing user
        users = self.user_storage.list({
            f"oauth_providers.{provider_id}.id": provider_user_id
        })
        
        if users:
            user = users[0]
            
            # Update OAuth tokens if provided
            if token_data:
                if "oauth_providers" not in user:
                    user["oauth_providers"] = {}
                
                if provider_id not in user["oauth_providers"]:
                    user["oauth_providers"][provider_id] = {}
                
                user["oauth_providers"][provider_id]["tokens"] = token_data
                user["oauth_providers"][provider_id]["updated_at"] = datetime.now().isoformat()
                
                self.user_storage.update(user["id"], user)
            
            return user
        
        # Create new user
        email = self._get_email_from_userinfo(provider_id, userinfo)
        name = self._get_name_from_userinfo(provider_id, userinfo)
        
        user_data = {
            "username": email or f"{provider_id}_{provider_user_id}",
            "email": email,
            "name": name,
            "oauth_providers": {
                provider_id: {
                    "id": provider_user_id,
                    "userinfo": userinfo,
                    "tokens": token_data,
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
            },
            "is_active": True,
            "roles": ["user"]
        }
        
        user_id = self.user_storage.create(user_data)
        
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
    
    def _generate_code_challenge(self, code_verifier: str) -> str:
        """
        Generate a PKCE code challenge from a code verifier.
        
        Args:
            code_verifier: The code verifier.
            
        Returns:
            The code challenge.
        """
        code_challenge = hashlib.sha256(code_verifier.encode()).digest()
        return base64.urlsafe_b64encode(code_challenge).decode().rstrip("=")
    
    def _get_user_id_field(self, provider_id: str) -> str:
        """
        Get the user ID field for a provider.
        
        Args:
            provider_id: The provider ID.
            
        Returns:
            The user ID field.
        """
        # Provider-specific mappings
        mappings = {
            "google": "sub",
            "facebook": "id",
            "github": "id",
            "microsoft": "sub",
            "apple": "sub"
        }
        
        return mappings.get(provider_id, "id")
    
    def _get_email_from_userinfo(self, provider_id: str, userinfo: Dict[str, Any]) -> Optional[str]:
        """
        Get the email from user info.
        
        Args:
            provider_id: The provider ID.
            userinfo: The user info.
            
        Returns:
            The email, or None if not found.
        """
        # Provider-specific mappings
        mappings = {
            "google": "email",
            "facebook": "email",
            "github": "email",
            "microsoft": "email",
            "apple": "email"
        }
        
        field = mappings.get(provider_id, "email")
        return userinfo.get(field)
    
    def _get_name_from_userinfo(self, provider_id: str, userinfo: Dict[str, Any]) -> Optional[str]:
        """
        Get the name from user info.
        
        Args:
            provider_id: The provider ID.
            userinfo: The user info.
            
        Returns:
            The name, or None if not found.
        """
        # Provider-specific mappings
        mappings = {
            "google": "name",
            "facebook": "name",
            "github": "name",
            "microsoft": "name",
            "apple": "name"
        }
        
        field = mappings.get(provider_id, "name")
        return userinfo.get(field) 