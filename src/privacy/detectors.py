"""
Detectors for sensitive data in the AI-powered data retrieval application.
"""

import logging
import re
from typing import Dict, List, Any, Optional, Pattern
import json

from .core import DataDetector, DataCategory

logger = logging.getLogger(__name__)

class RegexPatternDetector(DataDetector):
    """
    Detector for sensitive data based on regex patterns.
    """
    
    def __init__(self):
        """
        Initialize the regex pattern detector with common patterns for sensitive data.
        """
        self.patterns = {
            DataCategory.PERSONAL_IDENTIFIER: [
                # SSN (US Social Security Number)
                re.compile(r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b'),
                # Passport numbers
                re.compile(r'\b[A-Z]{1,2}[0-9]{6,9}\b'),
                # Driver's license (simplified pattern)
                re.compile(r'\b[A-Z]{1,2}[-\s]?\d{6,8}\b')
            ],
            DataCategory.FINANCIAL: [
                # Credit card numbers
                re.compile(r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})\b'),
                # Bank account numbers (simplified)
                re.compile(r'\b\d{8,17}\b'),
                # IBAN
                re.compile(r'\b[A-Z]{2}[0-9]{2}[A-Z0-9]{4}[0-9]{7}([A-Z0-9]?){0,16}\b')
            ],
            DataCategory.HEALTH: [
                # Medical record numbers (simplified)
                re.compile(r'\bMRN[-\s]?\d{6,10}\b'),
                # Health insurance numbers (simplified)
                re.compile(r'\b[A-Z]{3,5}[-\s]?\d{6,12}\b')
            ],
            DataCategory.LOCATION: [
                # GPS coordinates
                re.compile(r'\b[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)\b'),
                # ZIP/Postal codes
                re.compile(r'\b\d{5}(?:[-\s]\d{4})?\b'),  # US
                re.compile(r'\b[A-Z]\d[A-Z][-\s]?\d[A-Z]\d\b')  # Canada
            ],
            DataCategory.CONTACT: [
                # Email addresses
                re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
                # Phone numbers
                re.compile(r'\b(?:\+\d{1,3}[-\s]?)?(?:\(?\d{3}\)?[-\s]?)?\d{3}[-\s]?\d{4}\b')
            ],
            DataCategory.BIOMETRIC: [
                # Not easily detectable via regex
            ]
        }
        
        # Field name patterns that hint at sensitive data
        self.field_name_patterns = {
            DataCategory.PERSONAL_IDENTIFIER: [
                re.compile(r'(?i)ssn|social.*security|passport|id.*number|driver.*license|national.*id|tax.*id'),
            ],
            DataCategory.FINANCIAL: [
                re.compile(r'(?i)credit.*card|card.*number|cvv|cvc|expir|payment|account.*number|routing|bank|iban|swift|balance|salary|income|tax'),
            ],
            DataCategory.HEALTH: [
                re.compile(r'(?i)health|medical|patient|diagnosis|treatment|prescription|doctor|hospital|insurance|condition|allergy|blood.*type|disability|vaccine'),
            ],
            DataCategory.LOCATION: [
                re.compile(r'(?i)address|city|state|country|zip|postal|gps|latitude|longitude|location|coordinates'),
            ],
            DataCategory.CONTACT: [
                re.compile(r'(?i)email|phone|mobile|fax|contact'),
            ],
            DataCategory.DEMOGRAPHIC: [
                re.compile(r'(?i)age|gender|sex|race|ethnicity|nationality|religion|language|marital|education|occupation'),
            ],
            DataCategory.BIOMETRIC: [
                re.compile(r'(?i)biometric|fingerprint|retina|iris|face.*recognition|voice.*print|dna'),
            ]
        }
    
    def detect(self, field_name: str, value: Any) -> Optional[DataCategory]:
        """
        Detect if a value contains sensitive data based on regex patterns.
        
        Args:
            field_name: The name of the field.
            value: The value to check.
            
        Returns:
            The data category if sensitive data is detected, None otherwise.
        """
        # Check field name first (often more reliable)
        for category, patterns in self.field_name_patterns.items():
            for pattern in patterns:
                if pattern.search(field_name):
                    return category
        
        # Then check value if it's a string
        if isinstance(value, str):
            for category, patterns in self.patterns.items():
                for pattern in patterns:
                    if pattern.search(value):
                        return category
        
        return None

class MLBasedDetector(DataDetector):
    """
    Detector for sensitive data based on machine learning.
    This is a placeholder for a more sophisticated ML-based detector.
    """
    
    def __init__(self, model_path: str = None):
        """
        Initialize the ML-based detector.
        
        Args:
            model_path: Path to the ML model.
        """
        self.model_path = model_path
        # In a real implementation, would load a trained model here
        
    def detect(self, field_name: str, value: Any) -> Optional[DataCategory]:
        """
        Detect if a value contains sensitive data using ML.
        
        Args:
            field_name: The name of the field.
            value: The value to check.
            
        Returns:
            The data category if sensitive data is detected, None otherwise.
        """
        # This is a placeholder for ML-based detection
        # In a real implementation, would use the loaded model to predict
        
        # For now, just return None
        return None

class NameEntityDetector(DataDetector):
    """
    Detector for personal names.
    """
    
    def __init__(self, name_lists: Dict[str, List[str]] = None):
        """
        Initialize the name entity detector.
        
        Args:
            name_lists: Dictionary of name lists (first names, last names).
        """
        self.name_lists = name_lists or {}
        
        # Load default name lists if not provided
        if not self.name_lists:
            self._load_default_name_lists()
        
        # Compile name patterns
        self.name_pattern = re.compile(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b')
    
    def _load_default_name_lists(self):
        """
        Load default name lists.
        """
        # In a real implementation, would load from files
        # For now, just use a small sample
        self.name_lists = {
            "first_names": [
                "John", "Jane", "Michael", "Emily", "David", "Sarah", "James",
                "Jennifer", "Robert", "Elizabeth", "William", "Linda", "Richard",
                "Barbara", "Joseph", "Susan", "Thomas", "Jessica", "Charles", "Mary"
            ],
            "last_names": [
                "Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller",
                "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White",
                "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson"
            ]
        }
    
    def detect(self, field_name: str, value: Any) -> Optional[DataCategory]:
        """
        Detect if a value contains a personal name.
        
        Args:
            field_name: The name of the field.
            value: The value to check.
            
        Returns:
            The data category if a name is detected, None otherwise.
        """
        # Check field name first
        if re.search(r'(?i)name|first.*name|last.*name|full.*name', field_name):
            return DataCategory.PERSONAL_IDENTIFIER
        
        # Check value if it's a string
        if isinstance(value, str):
            # Check if value matches name pattern
            if self.name_pattern.search(value):
                return DataCategory.PERSONAL_IDENTIFIER
            
            # Check if value is in name lists
            value_parts = value.split()
            for part in value_parts:
                if part in self.name_lists.get("first_names", []) or part in self.name_lists.get("last_names", []):
                    return DataCategory.PERSONAL_IDENTIFIER
        
        return None

class CompositeDetector(DataDetector):
    """
    Composite detector that combines multiple detectors.
    """
    
    def __init__(self, detectors: List[DataDetector]):
        """
        Initialize the composite detector.
        
        Args:
            detectors: List of detectors to use.
        """
        self.detectors = detectors
    
    def detect(self, field_name: str, value: Any) -> Optional[DataCategory]:
        """
        Detect if a value contains sensitive data using multiple detectors.
        
        Args:
            field_name: The name of the field.
            value: The value to check.
            
        Returns:
            The data category if sensitive data is detected, None otherwise.
        """
        for detector in self.detectors:
            category = detector.detect(field_name, value)
            if category:
                return category
        
        return None 