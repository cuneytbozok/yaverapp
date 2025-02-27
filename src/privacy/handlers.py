"""
Privacy handlers for the AI-powered data retrieval application.
"""

import logging
import re
import hashlib
import base64
import uuid
import random
import string
from typing import Dict, List, Any, Optional, Union, Set, Tuple
from datetime import datetime, date

from .core import PrivacyHandler, PrivacyRule, PrivacyAction

logger = logging.getLogger(__name__)

class RedactionHandler(PrivacyHandler):
    """
    Handler for redacting sensitive data.
    """
    
    def apply(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply redaction to a value.
        
        Args:
            value: The value to redact.
            rule: The privacy rule to apply.
            
        Returns:
            The redacted value.
        """
        if value is None:
            return None
        
        # Get replacement value from rule parameters
        replacement = rule.parameters.get("replacement", "[REDACTED]")
        
        # For strings, replace with the replacement value
        if isinstance(value, str):
            return replacement
        
        # For numbers, replace with 0 or specified numeric replacement
        if isinstance(value, (int, float)):
            numeric_replacement = rule.parameters.get("numeric_replacement", 0)
            return numeric_replacement
        
        # For booleans, replace with False or specified boolean replacement
        if isinstance(value, bool):
            boolean_replacement = rule.parameters.get("boolean_replacement", False)
            return boolean_replacement
        
        # For dates, replace with epoch or specified date replacement
        if isinstance(value, (datetime, date)):
            date_replacement_str = rule.parameters.get("date_replacement", "1970-01-01")
            try:
                return datetime.fromisoformat(date_replacement_str)
            except ValueError:
                return datetime(1970, 1, 1)
        
        # For lists, replace each element
        if isinstance(value, list):
            return [self.apply(item, rule) for item in value]
        
        # For other types, convert to string and redact
        return replacement

class MaskingHandler(PrivacyHandler):
    """
    Handler for masking sensitive data.
    """
    
    def apply(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply masking to a value.
        
        Args:
            value: The value to mask.
            rule: The privacy rule to apply.
            
        Returns:
            The masked value.
        """
        if value is None or not isinstance(value, str):
            return value
        
        # Get masking parameters
        mask_char = rule.parameters.get("mask_char", "*")
        visible_prefix = rule.parameters.get("visible_prefix", 0)
        visible_suffix = rule.parameters.get("visible_suffix", 4)
        
        # Apply masking
        if len(value) <= visible_prefix + visible_suffix:
            # Value is too short, mask everything
            return mask_char * len(value)
        
        prefix = value[:visible_prefix] if visible_prefix > 0 else ""
        suffix = value[-visible_suffix:] if visible_suffix > 0 else ""
        masked_length = len(value) - visible_prefix - visible_suffix
        masked_part = mask_char * masked_length
        
        return prefix + masked_part + suffix

class TokenizationHandler(PrivacyHandler):
    """
    Handler for tokenizing sensitive data.
    """
    
    def __init__(self, token_map: Dict[str, str] = None, salt: str = None):
        """
        Initialize the tokenization handler.
        
        Args:
            token_map: Dictionary mapping original values to tokens.
            salt: Salt for token generation.
        """
        self.token_map = token_map or {}
        self.reverse_map = {v: k for k, v in self.token_map.items()}
        self.salt = salt or str(uuid.uuid4())
    
    def apply(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply tokenization to a value.
        
        Args:
            value: The value to tokenize.
            rule: The privacy rule to apply.
            
        Returns:
            The tokenized value.
        """
        if value is None:
            return None
        
        # Convert value to string for tokenization
        str_value = str(value)
        
        # Check if we already have a token for this value
        if str_value in self.token_map:
            return self.token_map[str_value]
        
        # Generate a new token
        token_type = rule.parameters.get("token_type", "uuid")
        
        if token_type == "uuid":
            # Generate a UUID-based token
            token = str(uuid.uuid4())
        elif token_type == "hash":
            # Generate a hash-based token
            hash_obj = hashlib.sha256((str_value + self.salt).encode())
            token = hash_obj.hexdigest()
        elif token_type == "format_preserving":
            # Generate a format-preserving token
            token = self._generate_format_preserving_token(str_value)
        else:
            # Default to UUID
            token = str(uuid.uuid4())
        
        # Store the token mapping
        self.token_map[str_value] = token
        self.reverse_map[token] = str_value
        
        return token
    
    def _generate_format_preserving_token(self, value: str) -> str:
        """
        Generate a format-preserving token.
        
        Args:
            value: The value to tokenize.
            
        Returns:
            A format-preserving token.
        """
        # Simple implementation - in a real system, would use a more sophisticated algorithm
        result = ""
        for char in value:
            if char.isdigit():
                result += random.choice(string.digits)
            elif char.isupper():
                result += random.choice(string.ascii_uppercase)
            elif char.islower():
                result += random.choice(string.ascii_lowercase)
            else:
                result += char
        
        return result
    
    def detokenize(self, token: str) -> Optional[str]:
        """
        Convert a token back to its original value.
        
        Args:
            token: The token to detokenize.
            
        Returns:
            The original value, or None if the token is not found.
        """
        return self.reverse_map.get(token)

class HashingHandler(PrivacyHandler):
    """
    Handler for hashing sensitive data.
    """
    
    def __init__(self, salt: str = None):
        """
        Initialize the hashing handler.
        
        Args:
            salt: Salt for hash generation.
        """
        self.salt = salt or str(uuid.uuid4())
    
    def apply(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply hashing to a value.
        
        Args:
            value: The value to hash.
            rule: The privacy rule to apply.
            
        Returns:
            The hashed value.
        """
        if value is None:
            return None
        
        # Convert value to string for hashing
        str_value = str(value)
        
        # Get hashing parameters
        hash_algorithm = rule.parameters.get("hash_algorithm", "sha256")
        encoding = rule.parameters.get("encoding", "hex")
        
        # Apply hashing
        if hash_algorithm == "sha256":
            hash_obj = hashlib.sha256((str_value + self.salt).encode())
        elif hash_algorithm == "sha512":
            hash_obj = hashlib.sha512((str_value + self.salt).encode())
        elif hash_algorithm == "md5":
            hash_obj = hashlib.md5((str_value + self.salt).encode())
        else:
            # Default to SHA-256
            hash_obj = hashlib.sha256((str_value + self.salt).encode())
        
        # Apply encoding
        if encoding == "hex":
            return hash_obj.hexdigest()
        elif encoding == "base64":
            return base64.b64encode(hash_obj.digest()).decode()
        else:
            # Default to hex
            return hash_obj.hexdigest()

class AggregationHandler(PrivacyHandler):
    """
    Handler for aggregating sensitive data.
    """
    
    def __init__(self, aggregation_cache: Dict[str, Dict[str, Any]] = None):
        """
        Initialize the aggregation handler.
        
        Args:
            aggregation_cache: Cache for aggregation results.
        """
        self.aggregation_cache = aggregation_cache or {}
    
    def apply(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply aggregation to a value.
        
        Args:
            value: The value to aggregate.
            rule: The privacy rule to apply.
            
        Returns:
            The aggregated value.
        """
        if value is None:
            return None
        
        # Get aggregation parameters
        aggregation_type = rule.parameters.get("aggregation_type", "count")
        group_by = rule.parameters.get("group_by", None)
        
        # If no group_by is specified, use a default key
        group_key = str(group_by) if group_by is not None else "default"
        
        # Initialize aggregation cache for this group if needed
        if group_key not in self.aggregation_cache:
            self.aggregation_cache[group_key] = {
                "count": 0,
                "sum": 0,
                "min": float('inf'),
                "max": float('-inf'),
                "values": []
            }
        
        # Update aggregation cache
        cache = self.aggregation_cache[group_key]
        cache["count"] += 1
        
        if isinstance(value, (int, float)):
            cache["sum"] += value
            cache["min"] = min(cache["min"], value)
            cache["max"] = max(cache["max"], value)
        
        cache["values"].append(value)
        
        # Return the requested aggregation
        if aggregation_type == "count":
            return cache["count"]
        elif aggregation_type == "sum" and isinstance(value, (int, float)):
            return cache["sum"]
        elif aggregation_type == "avg" and isinstance(value, (int, float)):
            return cache["sum"] / cache["count"] if cache["count"] > 0 else 0
        elif aggregation_type == "min" and isinstance(value, (int, float)):
            return cache["min"] if cache["min"] != float('inf') else 0
        elif aggregation_type == "max" and isinstance(value, (int, float)):
            return cache["max"] if cache["max"] != float('-inf') else 0
        else:
            # Default to count
            return cache["count"]

class GeneralizationHandler(PrivacyHandler):
    """
    Handler for generalizing sensitive data.
    """
    
    def apply(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply generalization to a value.
        
        Args:
            value: The value to generalize.
            rule: The privacy rule to apply.
            
        Returns:
            The generalized value.
        """
        if value is None:
            return None
        
        # Get generalization parameters
        generalization_type = rule.parameters.get("generalization_type", "default")
        
        # Apply generalization based on value type and generalization type
        if generalization_type == "location":
            return self._generalize_location(value, rule)
        elif generalization_type == "age":
            return self._generalize_age(value, rule)
        elif generalization_type == "zipcode":
            return self._generalize_zipcode(value, rule)
        elif generalization_type == "custom":
            return self._apply_custom_generalization(value, rule)
        else:
            # Default generalization
            return self._default_generalization(value, rule)
    
    def _generalize_location(self, value: str, rule: PrivacyRule) -> str:
        """
        Generalize a location.
        
        Args:
            value: The location to generalize.
            rule: The privacy rule to apply.
            
        Returns:
            The generalized location.
        """
        level = rule.parameters.get("level", "city")
        
        # Simple implementation - in a real system, would use a geocoding service
        parts = value.split(',')
        
        if level == "country" and len(parts) >= 1:
            return parts[-1].strip()
        elif level == "state" and len(parts) >= 2:
            return f"{parts[-2].strip()}, {parts[-1].strip()}"
        elif level == "city" and len(parts) >= 3:
            return f"{parts[-3].strip()}, {parts[-2].strip()}, {parts[-1].strip()}"
        else:
            # Can't generalize further
            return value
    
    def _generalize_age(self, value: Union[int, str], rule: PrivacyRule) -> str:
        """
        Generalize an age.
        
        Args:
            value: The age to generalize.
            rule: The privacy rule to apply.
            
        Returns:
            The generalized age.
        """
        try:
            age = int(value)
        except (ValueError, TypeError):
            return value
        
        range_size = rule.parameters.get("range_size", 10)
        lower_bound = (age // range_size) * range_size
        upper_bound = lower_bound + range_size - 1
        
        return f"{lower_bound}-{upper_bound}"
    
    def _generalize_zipcode(self, value: str, rule: PrivacyRule) -> str:
        """
        Generalize a ZIP code.
        
        Args:
            value: The ZIP code to generalize.
            rule: The privacy rule to apply.
            
        Returns:
            The generalized ZIP code.
        """
        digits = rule.parameters.get("digits", 3)
        
        if not isinstance(value, str):
            value = str(value)
        
        if len(value) <= digits:
            return value
        
        return value[:digits] + "*" * (len(value) - digits)
    
    def _apply_custom_generalization(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply custom generalization.
        
        Args:
            value: The value to generalize.
            rule: The privacy rule to apply.
            
        Returns:
            The generalized value.
        """
        mapping = rule.parameters.get("mapping", {})
        default = rule.parameters.get("default", value)
        
        str_value = str(value)
        return mapping.get(str_value, default)
    
    def _default_generalization(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply default generalization.
        
        Args:
            value: The value to generalize.
            rule: The privacy rule to apply.
            
        Returns:
            The generalized value.
        """
        if isinstance(value, (int, float)):
            # Round to nearest multiple
            multiple = rule.parameters.get("multiple", 5)
            return round(value / multiple) * multiple
        
        elif isinstance(value, str):
            # Truncate string
            length = rule.parameters.get("length", 1)
            return value[:length] + "*" * (len(value) - length) if len(value) > length else value
        
        else:
            # Can't generalize other types
            return value

class PerturbationHandler(PrivacyHandler):
    """
    Handler for perturbing sensitive data.
    """
    
    def apply(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply perturbation to a value.
        
        Args:
            value: The value to perturb.
            rule: The privacy rule to apply.
            
        Returns:
            The perturbed value.
        """
        if value is None:
            return None
        
        # Only apply perturbation to numeric values
        if not isinstance(value, (int, float)):
            return value
        
        # Get perturbation parameters
        method = rule.parameters.get("method", "gaussian")
        
        # Apply perturbation based on method
        if method == "gaussian":
            return self._apply_gaussian_noise(value, rule)
        elif method == "laplace":
            return self._apply_laplace_noise(value, rule)
        elif method == "uniform":
            return self._apply_uniform_noise(value, rule)
        else:
            # Default to gaussian
            return self._apply_gaussian_noise(value, rule)
    
    def _apply_gaussian_noise(self, value: Union[int, float], rule: PrivacyRule) -> Union[int, float]:
        """
        Apply Gaussian noise to a value.
        
        Args:
            value: The value to perturb.
            rule: The privacy rule to apply.
            
        Returns:
            The perturbed value.
        """
        scale = rule.parameters.get("scale", 1.0)
        noise = random.gauss(0, scale)
        
        result = value + noise
        
        # Preserve integer type if input was integer
        if isinstance(value, int):
            result = round(result)
        
        return result
    
    def _apply_laplace_noise(self, value: Union[int, float], rule: PrivacyRule) -> Union[int, float]:
        """
        Apply Laplace noise to a value.
        
        Args:
            value: The value to perturb.
            rule: The privacy rule to apply.
            
        Returns:
            The perturbed value.
        """
        scale = rule.parameters.get("scale", 1.0)
        
        # Generate Laplace noise
        u = random.random() - 0.5
        noise = -scale * (1 if u < 0 else -1) * math.log(1 - 2 * abs(u))
        
        result = value + noise
        
        # Preserve integer type if input was integer
        if isinstance(value, int):
            result = round(result)
        
        return result
    
    def _apply_uniform_noise(self, value: Union[int, float], rule: PrivacyRule) -> Union[int, float]:
        """
        Apply uniform noise to a value.
        
        Args:
            value: The value to perturb.
            rule: The privacy rule to apply.
            
        Returns:
            The perturbed value.
        """
        min_noise = rule.parameters.get("min", -1.0)
        max_noise = rule.parameters.get("max", 1.0)
        
        noise = random.uniform(min_noise, max_noise)
        
        result = value + noise
        
        # Preserve integer type if input was integer
        if isinstance(value, int):
            result = round(result)
        
        return result 