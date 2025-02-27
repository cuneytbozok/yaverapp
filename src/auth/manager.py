"""
Authentication manager for the AI-powered data retrieval application.
"""

import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta

from .core import (
    AuthProvider, AuthResult, AuthStatus, UserSession, UserCredentials,
    TokenGenerator, AuthMethod
)
from .storage import UserStorage, SessionStorage
from .password_auth import PasswordAuthProvider
from .oauth_auth import OAuthAuthProvider, OAuthProvider
from .saml_auth import SAMLAuthProvider, SAMLProvider
from .mfa import MFAManager, TOTPMethod, EmailMethod

logger = logging.getLogger(__name__)

class AuthManager:
    """
    Authentication manager.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the authentication manager.
        
        Args:
            config: The authentication configuration.
        """
        self.config = config
        
        # Initialize storage
        storage_config = config.get("storage", {})
        self._init_storage(storage_config)
        
        # Initialize authentication providers
        auth_config = config.get("auth", {})
        self._init_auth_providers(auth_config)
        
        # Initialize MFA
        mfa_config = config.get("mfa", {})
        self._init_mfa(mfa_config)
    
    def _init_storage(self, config: Dict[str, Any]) -> None:
        """
        Initialize storage providers.
        
        Args:
            config: The storage configuration.
        """
        from .storage import FileStorageProvider
        
        # User storage
        user_storage_config = config.get("user", {})
        user_storage_dir = user_storage_config.get("directory", "data/users")
        user_storage_provider = FileStorageProvider(user_storage_dir)
        self.user_storage = UserStorage(user_storage_provider)
        
        # Session storage
        session_storage_config = config.get("session", {})
        session_storage_dir = session_storage_config.get("directory", "data/sessions")
        session_storage_provider = FileStorageProvider(session_storage_dir)
        self.session_storage = SessionStorage(session_storage_provider)
    
    def _init_auth_providers(self, config: Dict[str, Any]) -> None:
        """
        Initialize authentication providers.
        
        Args:
            config: The authentication configuration.
        """
        self.auth_providers = {}
        
        # Password authentication
        password_config = config.get("password", {})
        if password_config.get("enabled", True):
            self.auth_providers[AuthMethod.PASSWORD] = PasswordAuthProvider(
                self.user_storage,
                self.session_storage,
                password_config
            )
        
        # OAuth authentication
        oauth_config = config.get("oauth", {})
        if oauth_config.get("enabled", False):
            oauth_providers = []
            
            for provider_config in oauth_config.get("providers", []):
                oauth_providers.append(OAuthProvider(
                    provider_id=provider_config["provider_id"],
                    name=provider_config["name"],
                    client_id=provider_config["client_id"],
                    client_secret=provider_config["client_secret"],
                    authorize_url=provider_config["authorize_url"],
                    token_url=provider_config["token_url"],
                    userinfo_url=provider_config["userinfo_url"],
                    scope=provider_config["scope"],
                    redirect_uri=provider_config["redirect_uri"],
                    additional_params=provider_config.get("additional_params")
                ))
            
            self.auth_providers[AuthMethod.OAUTH] = OAuthAuthProvider(
                self.user_storage,
                self.session_storage,
                oauth_providers,
                oauth_config.get("session_duration_minutes", 60)
            )
        
        # SAML authentication
        saml_config = config.get("saml", {})
        if saml_config.get("enabled", False):
            saml_providers = []
            
            for provider_config in saml_config.get("providers", []):
                saml_providers.append(SAMLProvider(
                    provider_id=provider_config["provider_id"],
                    name=provider_config["name"],
                    metadata_url=provider_config.get("metadata_url"),
                    metadata_file=provider_config.get("metadata_file"),
                    entity_id=provider_config["entity_id"],
                    acs_url=provider_config["acs_url"],
                    attribute_mapping=provider_config.get("attribute_mapping"),
                    additional_params=provider_config.get("additional_params")
                ))
            
            self.auth_providers[AuthMethod.SAML] = SAMLAuthProvider(
                self.user_storage,
                self.session_storage,
                saml_providers,
                saml_config.get("session_duration_minutes", 60)
            )
    
    def _init_mfa(self, config: Dict[str, Any]) -> None:
        """
        Initialize multi-factor authentication.
        
        Args:
            config: The MFA configuration.
        """
        self.mfa_manager = MFAManager(self.user_storage, self.session_storage)
        
        # TOTP method
        totp_config = config.get("totp", {})
        if totp_config.get("enabled", True):
            self.mfa_manager.register_method(TOTPMethod(self.user_storage))
        
        # Email method
        email_config = config.get("email", {})
        if email_config.get("enabled", True):
            from ..email import EmailSender
            email_sender = EmailSender(email_config)
            self.mfa_manager.register_method(EmailMethod(self.user_storage, email_sender))
    
    def authenticate(self, method: AuthMethod, credentials: Dict[str, Any]) -> AuthResult:
        """
        Authenticate a user.
        
        Args:
            method: The authentication method.
            credentials: The credentials to authenticate with.
            
        Returns:
            The result of the authentication attempt.
        """
        # Check if method is supported
        if method not in self.auth_providers:
            return AuthResult(
                status=AuthStatus.FAILURE,
                message=f"Unsupported authentication method: {method}"
            )
        
        provider = self.auth_providers[method]
        
        # Authenticate
        result = provider.authenticate(credentials)
        
        # Check if MFA is required
        if result.status == AuthStatus.SUCCESS and result.user_id:
            user = self.user_storage.get(result.user_id)
            
            if user and user.get("mfa_enabled", False):
                # Get available MFA methods
                mfa_methods = self.mfa_manager.get_available_methods(result.user_id)
                
                if mfa_methods:
                    # Mark result as requiring MFA
                    result.status = AuthStatus.REQUIRES_MFA
                    result.requires_mfa = True
                    result.mfa_methods = mfa_methods
        
        return result
    
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
        
        # Check if MFA is required but not verified
        user_id = session.get("user_id")
        if user_id:
            user = self.user_storage.get(user_id)
            
            if user and user.get("mfa_enabled", False) and not session.get("mfa_verified", False):
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
    
    def generate_mfa_challenge(self, user_id: str, method_id: str) -> Dict[str, Any]:
        """
        Generate an MFA challenge for a user.
        
        Args:
            user_id: The user ID.
            method_id: The MFA method ID.
            
        Returns:
            The challenge data.
        """
        return self.mfa_manager.generate_challenge(user_id, method_id)
    
    def verify_mfa_response(self, user_id: str, response: str) -> bool:
        """
        Verify an MFA response.
        
        Args:
            user_id: The user ID.
            response: The response to verify.
            
        Returns:
            True if the response is valid, False otherwise.
        """
        return self.mfa_manager.verify_response(user_id, response)
    
    def cleanup_expired_sessions(self) -> int:
        """
        Clean up expired sessions.
        
        Returns:
            The number of sessions deleted.
        """
        return self.session_storage.cleanup_expired_sessions() 