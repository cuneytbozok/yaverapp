"""
Multi-factor authentication support for the AI-powered data retrieval application.
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
import hmac
import struct
from urllib.parse import urlencode

from .core import (
    AuthProvider, AuthResult, AuthStatus, UserSession, UserCredentials,
    TokenGenerator
)
from .storage import UserStorage, SessionStorage

logger = logging.getLogger(__name__)

class MFAMethod:
    """
    Base class for MFA methods.
    """
    
    def __init__(self, method_id: str, name: str):
        """
        Initialize the MFA method.
        
        Args:
            method_id: The method ID.
            name: The method name.
        """
        self.method_id = method_id
        self.name = name
    
    def generate_challenge(self, user_id: str) -> Dict[str, Any]:
        """
        Generate a challenge for the user.
        
        Args:
            user_id: The user ID.
            
        Returns:
            The challenge data.
        """
        raise NotImplementedError("Subclasses must implement generate_challenge")
    
    def verify_response(self, user_id: str, challenge: Dict[str, Any], response: str) -> bool:
        """
        Verify a response to a challenge.
        
        Args:
            user_id: The user ID.
            challenge: The challenge data.
            response: The response to verify.
            
        Returns:
            True if the response is valid, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement verify_response")

class TOTPMethod(MFAMethod):
    """
    Time-based One-Time Password (TOTP) MFA method.
    """
    
    def __init__(self, user_storage: UserStorage):
        """
        Initialize the TOTP method.
        
        Args:
            user_storage: The user storage.
        """
        super().__init__("totp", "Time-based One-Time Password")
        self.user_storage = user_storage
    
    def generate_challenge(self, user_id: str) -> Dict[str, Any]:
        """
        Generate a challenge for the user.
        
        Args:
            user_id: The user ID.
            
        Returns:
            The challenge data.
        """
        # For TOTP, we don't need to generate a challenge
        # The user will generate the code based on the shared secret
        return {
            "method_id": self.method_id,
            "timestamp": datetime.now().isoformat()
        }
    
    def verify_response(self, user_id: str, challenge: Dict[str, Any], response: str) -> bool:
        """
        Verify a response to a challenge.
        
        Args:
            user_id: The user ID.
            challenge: The challenge data.
            response: The response to verify.
            
        Returns:
            True if the response is valid, False otherwise.
        """
        # Get user
        user = self.user_storage.get(user_id)
        if not user:
            logger.error(f"User with ID '{user_id}' not found")
            return False
        
        # Get TOTP secret
        mfa_data = user.get("mfa", {})
        totp_data = mfa_data.get("totp", {})
        secret = totp_data.get("secret")
        
        if not secret:
            logger.error(f"TOTP not set up for user '{user_id}'")
            return False
        
        # Verify TOTP code
        return self._verify_totp(secret, response)
    
    def setup(self, user_id: str) -> Dict[str, Any]:
        """
        Set up TOTP for a user.
        
        Args:
            user_id: The user ID.
            
        Returns:
            The setup data.
        """
        # Get user
        user = self.user_storage.get(user_id)
        if not user:
            logger.error(f"User with ID '{user_id}' not found")
            return {}
        
        # Generate secret
        secret = self._generate_secret()
        
        # Get user info for QR code
        username = user.get("username", user_id)
        email = user.get("email", username)
        
        # Create provisioning URI for QR code
        provisioning_uri = self._get_provisioning_uri(secret, email, "AI Data Retrieval")
        
        # Update user with TOTP data
        if "mfa" not in user:
            user["mfa"] = {}
        
        user["mfa"]["totp"] = {
            "secret": secret,
            "created_at": datetime.now().isoformat(),
            "verified": False
        }
        
        if not self.user_storage.update(user_id, user):
            logger.error(f"Failed to update user with TOTP data")
            return {}
        
        return {
            "secret": secret,
            "provisioning_uri": provisioning_uri
        }
    
    def verify_setup(self, user_id: str, code: str) -> bool:
        """
        Verify TOTP setup.
        
        Args:
            user_id: The user ID.
            code: The TOTP code to verify.
            
        Returns:
            True if the setup is verified, False otherwise.
        """
        # Get user
        user = self.user_storage.get(user_id)
        if not user:
            logger.error(f"User with ID '{user_id}' not found")
            return False
        
        # Get TOTP data
        mfa_data = user.get("mfa", {})
        totp_data = mfa_data.get("totp", {})
        secret = totp_data.get("secret")
        
        if not secret:
            logger.error(f"TOTP not set up for user '{user_id}'")
            return False
        
        # Verify TOTP code
        if not self._verify_totp(secret, code):
            return False
        
        # Mark as verified
        user["mfa"]["totp"]["verified"] = True
        user["mfa"]["totp"]["verified_at"] = datetime.now().isoformat()
        
        # Enable MFA for user
        user["mfa_enabled"] = True
        
        return self.user_storage.update(user_id, user)
    
    def _generate_secret(self) -> str:
        """
        Generate a TOTP secret.
        
        Returns:
            The generated secret.
        """
        # Generate 20 random bytes
        random_bytes = secrets.token_bytes(20)
        
        # Encode as base32
        return base64.b32encode(random_bytes).decode("utf-8")
    
    def _get_provisioning_uri(self, secret: str, username: str, issuer: str) -> str:
        """
        Get a provisioning URI for a TOTP secret.
        
        Args:
            secret: The TOTP secret.
            username: The username.
            issuer: The issuer.
            
        Returns:
            The provisioning URI.
        """
        params = {
            "secret": secret,
            "issuer": issuer,
            "algorithm": "SHA1",
            "digits": "6",
            "period": "30"
        }
        
        uri = f"otpauth://totp/{issuer}:{username}?{urlencode(params)}"
        
        return uri
    
    def _verify_totp(self, secret: str, code: str) -> bool:
        """
        Verify a TOTP code.
        
        Args:
            secret: The TOTP secret.
            code: The TOTP code to verify.
            
        Returns:
            True if the code is valid, False otherwise.
        """
        # Clean up code
        code = code.strip().replace(" ", "")
        
        # Check if code is valid
        if not code.isdigit() or len(code) != 6:
            return False
        
        # Decode secret
        try:
            key = base64.b32decode(secret)
        except Exception as e:
            logger.error(f"Error decoding TOTP secret: {e}")
            return False
        
        # Get current timestamp
        now = int(time.time())
        
        # Check codes for current time and adjacent intervals
        for offset in [-1, 0, 1]:
            if self._generate_totp(key, now + offset * 30) == code:
                return True
        
        return False
    
    def _generate_totp(self, key: bytes, timestamp: int) -> str:
        """
        Generate a TOTP code.
        
        Args:
            key: The TOTP key.
            timestamp: The timestamp.
            
        Returns:
            The generated TOTP code.
        """
        # Calculate counter value (RFC 6238)
        counter = struct.pack(">Q", timestamp // 30)
        
        # Calculate HMAC-SHA1
        h = hmac.new(key, counter, hashlib.sha1).digest()
        
        # Dynamic truncation (RFC 4226)
        offset = h[-1] & 0x0F
        binary = ((h[offset] & 0x7F) << 24) | ((h[offset + 1] & 0xFF) << 16) | ((h[offset + 2] & 0xFF) << 8) | (h[offset + 3] & 0xFF)
        
        # Generate 6-digit code
        return str(binary % 1000000).zfill(6)

class EmailMethod(MFAMethod):
    """
    Email-based MFA method.
    """
    
    def __init__(self, user_storage: UserStorage, email_sender: Any):
        """
        Initialize the email method.
        
        Args:
            user_storage: The user storage.
            email_sender: The email sender.
        """
        super().__init__("email", "Email Verification")
        self.user_storage = user_storage
        self.email_sender = email_sender
        
        # Code storage
        self.code_storage = {}  # user_id -> {"code": str, "expires_at": datetime}
    
    def generate_challenge(self, user_id: str) -> Dict[str, Any]:
        """
        Generate a challenge for the user.
        
        Args:
            user_id: The user ID.
            
        Returns:
            The challenge data.
        """
        # Get user
        user = self.user_storage.get(user_id)
        if not user:
            logger.error(f"User with ID '{user_id}' not found")
            return {}
        
        # Get email
        email = user.get("email")
        if not email:
            logger.error(f"User with ID '{user_id}' has no email")
            return {}
        
        # Generate code
        code = self._generate_code()
        
        # Store code with expiration
        expiration = datetime.now() + timedelta(minutes=10)
        self.code_storage[user_id] = {
            "code": code,
            "expires_at": expiration
        }
        
        # Send email
        try:
            self.email_sender.send_mfa_code(email, code)
        except Exception as e:
            logger.error(f"Error sending MFA code email: {e}")
            return {}
        
        return {
            "method_id": self.method_id,
            "email": self._mask_email(email),
            "expires_at": expiration.isoformat()
        }
    
    def verify_response(self, user_id: str, challenge: Dict[str, Any], response: str) -> bool:
        """
        Verify a response to a challenge.
        
        Args:
            user_id: The user ID.
            challenge: The challenge data.
            response: The response to verify.
            
        Returns:
            True if the response is valid, False otherwise.
        """
        # Check if we have a code for this user
        if user_id not in self.code_storage:
            logger.error(f"No MFA code found for user '{user_id}'")
            return False
        
        code_data = self.code_storage[user_id]
        
        # Check if code has expired
        if datetime.now() > code_data["expires_at"]:
            del self.code_storage[user_id]
            logger.error(f"MFA code for user '{user_id}' has expired")
            return False
        
        # Clean up response
        response = response.strip().replace(" ", "")
        
        # Verify code
        if response != code_data["code"]:
            return False
        
        # Clean up code
        del self.code_storage[user_id]
        
        return True
    
    def _generate_code(self) -> str:
        """
        Generate a verification code.
        
        Returns:
            The generated code.
        """
        # Generate 6-digit code
        return str(secrets.randbelow(1000000)).zfill(6)
    
    def _mask_email(self, email: str) -> str:
        """
        Mask an email address for display.
        
        Args:
            email: The email address.
            
        Returns:
            The masked email address.
        """
        if not email or "@" not in email:
            return email
        
        username, domain = email.split("@", 1)
        
        if len(username) <= 2:
            masked_username = username[0] + "*"
        else:
            masked_username = username[0] + "*" * (len(username) - 2) + username[-1]
        
        return f"{masked_username}@{domain}"

class MFAManager:
    """
    Manager for multi-factor authentication.
    """
    
    def __init__(self, user_storage: UserStorage, session_storage: SessionStorage):
        """
        Initialize the MFA manager.
        
        Args:
            user_storage: The user storage.
            session_storage: The session storage.
        """
        self.user_storage = user_storage
        self.session_storage = session_storage
        self.methods = {}  # method_id -> MFAMethod
        
        # Challenge storage
        self.challenge_storage = {}  # user_id -> {"method_id": str, "challenge": Dict, "expires_at": datetime}
    
    def register_method(self, method: MFAMethod) -> None:
        """
        Register an MFA method.
        
        Args:
            method: The MFA method to register.
        """
        self.methods[method.method_id] = method
    
    def get_available_methods(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Get available MFA methods for a user.
        
        Args:
            user_id: The user ID.
            
        Returns:
            A list of available MFA methods.
        """
        # Get user
        user = self.user_storage.get(user_id)
        if not user:
            logger.error(f"User with ID '{user_id}' not found")
            return []
        
        # Check if MFA is enabled
        if not user.get("mfa_enabled", False):
            return []
        
        # Get MFA data
        mfa_data = user.get("mfa", {})
        
        # Build list of available methods
        available_methods = []
        
        for method_id, method in self.methods.items():
            if method_id in mfa_data and mfa_data[method_id].get("verified", False):
                available_methods.append({
                    "method_id": method_id,
                    "name": method.name
                })
        
        return available_methods
    
    def generate_challenge(self, user_id: str, method_id: str) -> Dict[str, Any]:
        """
        Generate an MFA challenge for a user.
        
        Args:
            user_id: The user ID.
            method_id: The MFA method ID.
            
        Returns:
            The challenge data.
        """
        # Check if method exists
        if method_id not in self.methods:
            logger.error(f"Unknown MFA method: {method_id}")
            return {}
        
        method = self.methods[method_id]
        
        # Generate challenge
        challenge = method.generate_challenge(user_id)
        
        if not challenge:
            return {}
        
        # Store challenge with expiration
        expiration = datetime.now() + timedelta(minutes=10)
        self.challenge_storage[user_id] = {
            "method_id": method_id,
            "challenge": challenge,
            "expires_at": expiration
        }
        
        return challenge
    
    def verify_response(self, user_id: str, response: str) -> bool:
        """
        Verify an MFA response.
        
        Args:
            user_id: The user ID.
            response: The response to verify.
            
        Returns:
            True if the response is valid, False otherwise.
        """
        # Check if we have a challenge for this user
        if user_id not in self.challenge_storage:
            logger.error(f"No MFA challenge found for user '{user_id}'")
            return False
        
        challenge_data = self.challenge_storage[user_id]
        
        # Check if challenge has expired
        if datetime.now() > challenge_data["expires_at"]:
            del self.challenge_storage[user_id]
            logger.error(f"MFA challenge for user '{user_id}' has expired")
            return False
        
        method_id = challenge_data["method_id"]
        challenge = challenge_data["challenge"]
        
        # Check if method exists
        if method_id not in self.methods:
            logger.error(f"Unknown MFA method: {method_id}")
            return False
        
        method = self.methods[method_id]
        
        # Verify response
        if not method.verify_response(user_id, challenge, response):
            return False
        
        # Clean up challenge
        del self.challenge_storage[user_id]
        
        # Mark session as MFA verified
        self._mark_session_mfa_verified(user_id)
        
        return True
    
    def _mark_session_mfa_verified(self, user_id: str) -> None:
        """
        Mark a user's session as MFA verified.
        
        Args:
            user_id: The user ID.
        """
        # Find active sessions for user
        sessions = self.session_storage.find_by_user_id(user_id)
        
        for session in sessions:
            if session.get("is_active", False):
                session["mfa_verified"] = True
                self.session_storage.update(session["id"], session) 