"""
Core privacy module for the AI-powered data retrieval application.
This module defines the base interfaces and common functionality for privacy filtering.
"""

import logging
import re
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Dict, List, Any, Optional, Union, Set, Tuple, Callable, Pattern
from dataclasses import dataclass, field
import json
import hashlib
import base64
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SensitivityLevel(Enum):
    """Enum representing different sensitivity levels for data."""
    PUBLIC = 0
    INTERNAL = 10
    CONFIDENTIAL = 20
    RESTRICTED = 30
    SECRET = 40

class DataCategory(Enum):
    """Enum representing different categories of sensitive data."""
    PERSONAL_IDENTIFIER = "personal_identifier"
    FINANCIAL = "financial"
    HEALTH = "health"
    LOCATION = "location"
    CONTACT = "contact"
    DEMOGRAPHIC = "demographic"
    BIOMETRIC = "biometric"
    PROFESSIONAL = "professional"
    BEHAVIORAL = "behavioral"
    USER_CONTENT = "user_content"
    DEVICE_INFO = "device_info"
    OTHER = "other"

class ComplianceRegime(Enum):
    """Enum representing different compliance regimes."""
    GDPR = "gdpr"
    HIPAA = "hipaa"
    CCPA = "ccpa"
    PCI_DSS = "pci_dss"
    GLBA = "glba"
    FERPA = "ferpa"
    COPPA = "coppa"
    CUSTOM = "custom"

class PrivacyAction(Enum):
    """Enum representing different privacy actions to take on data."""
    NONE = "none"  # No action, pass through as is
    REDACT = "redact"  # Complete removal/replacement
    MASK = "mask"  # Partial masking (e.g., last 4 digits)
    TOKENIZE = "tokenize"  # Replace with a token
    HASH = "hash"  # Replace with a hash
    AGGREGATE = "aggregate"  # Replace with an aggregate value
    GENERALIZE = "generalize"  # Replace with a more general value
    PERTURB = "perturb"  # Add noise to the value

@dataclass
class PrivacyRule:
    """Rule for privacy filtering."""
    data_category: DataCategory
    sensitivity_level: SensitivityLevel
    action: PrivacyAction
    parameters: Dict[str, Any] = field(default_factory=dict)
    compliance_regimes: List[ComplianceRegime] = field(default_factory=list)
    
    def __hash__(self):
        return hash((self.data_category, self.sensitivity_level, self.action))

@dataclass
class DataClassification:
    """Classification of a data field."""
    field_name: str
    data_category: DataCategory
    sensitivity_level: SensitivityLevel
    compliance_regimes: List[ComplianceRegime] = field(default_factory=list)
    
    def __hash__(self):
        return hash((self.field_name, self.data_category, self.sensitivity_level))

class PrivacyException(Exception):
    """Exception raised for privacy-related errors."""
    pass

class DataDetector(ABC):
    """
    Abstract base class for sensitive data detectors.
    """
    
    @abstractmethod
    def detect(self, field_name: str, value: Any) -> Optional[DataCategory]:
        """
        Detect if a value contains sensitive data.
        
        Args:
            field_name: The name of the field.
            value: The value to check.
            
        Returns:
            The data category if sensitive data is detected, None otherwise.
        """
        pass

class PrivacyHandler(ABC):
    """
    Abstract base class for privacy handlers.
    """
    
    @abstractmethod
    def apply(self, value: Any, rule: PrivacyRule) -> Any:
        """
        Apply a privacy rule to a value.
        
        Args:
            value: The value to apply the rule to.
            rule: The privacy rule to apply.
            
        Returns:
            The transformed value.
        """
        pass

class PatternBasedDetector(DataDetector):
    """
    Pattern-based detector for sensitive data.
    """
    
    def __init__(self, patterns: Dict[DataCategory, List[Pattern]]):
        """
        Initialize the pattern-based detector.
        
        Args:
            patterns: Dictionary mapping data categories to regex patterns.
        """
        self.patterns = patterns
    
    def detect(self, field_name: str, value: Any) -> Optional[DataCategory]:
        """
        Detect if a value contains sensitive data based on regex patterns.
        
        Args:
            field_name: The name of the field.
            value: The value to check.
            
        Returns:
            The data category if sensitive data is detected, None otherwise.
        """
        if not isinstance(value, str):
            return None
        
        # Check field name first (often more reliable)
        for category, field_patterns in self.field_name_hints.items():
            for pattern in field_patterns:
                if re.search(pattern, field_name, re.IGNORECASE):
                    return category
        
        # Then check value
        for category, patterns in self.patterns.items():
            for pattern in patterns:
                if pattern.search(value):
                    return category
        
        return None
    
    # Common field name patterns that hint at sensitive data
    field_name_hints = {
        DataCategory.PERSONAL_IDENTIFIER: [
            r'ssn', r'social.*security', r'passport', r'id.*number', r'driver.*license',
            r'national.*id', r'tax.*id', r'birth.*certificate'
        ],
        DataCategory.FINANCIAL: [
            r'credit.*card', r'card.*number', r'cvv', r'cvc', r'expir', r'payment',
            r'account.*number', r'routing', r'bank', r'iban', r'swift', r'balance',
            r'salary', r'income', r'tax', r'invoice'
        ],
        DataCategory.HEALTH: [
            r'health', r'medical', r'patient', r'diagnosis', r'treatment', r'prescription',
            r'doctor', r'hospital', r'insurance', r'condition', r'allergy', r'blood.*type',
            r'disability', r'vaccine'
        ],
        DataCategory.LOCATION: [
            r'address', r'city', r'state', r'country', r'zip', r'postal', r'gps',
            r'latitude', r'longitude', r'location', r'coordinates'
        ],
        DataCategory.CONTACT: [
            r'email', r'phone', r'mobile', r'fax', r'contact'
        ],
        DataCategory.DEMOGRAPHIC: [
            r'age', r'gender', r'sex', r'race', r'ethnicity', r'nationality',
            r'religion', r'language', r'marital', r'education', r'occupation'
        ],
        DataCategory.BIOMETRIC: [
            r'biometric', r'fingerprint', r'retina', r'iris', r'face.*recognition',
            r'voice.*print', r'dna'
        ]
    }

class PrivacyEngine:
    """
    Core engine for privacy filtering.
    """
    
    def __init__(self, detectors: List[DataDetector], handlers: Dict[PrivacyAction, PrivacyHandler],
                rules: List[PrivacyRule], classifications: Dict[str, DataClassification] = None,
                default_action: PrivacyAction = PrivacyAction.REDACT):
        """
        Initialize the privacy engine.
        
        Args:
            detectors: List of data detectors.
            handlers: Dictionary mapping privacy actions to handlers.
            rules: List of privacy rules.
            classifications: Dictionary mapping field names to data classifications.
            default_action: Default action to take when no rule matches.
        """
        self.detectors = detectors
        self.handlers = handlers
        self.rules = rules
        self.classifications = classifications or {}
        self.default_action = default_action
        
        # Index rules by data category and sensitivity level for faster lookup
        self.rule_index = {}
        for rule in rules:
            key = (rule.data_category, rule.sensitivity_level)
            if key not in self.rule_index:
                self.rule_index[key] = []
            self.rule_index[key].append(rule)
    
    def classify_field(self, field_name: str, value: Any) -> Optional[DataClassification]:
        """
        Classify a field based on its name and value.
        
        Args:
            field_name: The name of the field.
            value: The value of the field.
            
        Returns:
            The data classification, or None if not classified.
        """
        # Check if we already have a classification for this field
        if field_name in self.classifications:
            return self.classifications[field_name]
        
        # Try to detect the data category
        for detector in self.detectors:
            category = detector.detect(field_name, value)
            if category:
                # Create a new classification with default sensitivity level
                classification = DataClassification(
                    field_name=field_name,
                    data_category=category,
                    sensitivity_level=SensitivityLevel.CONFIDENTIAL  # Default to confidential
                )
                
                # Cache the classification
                self.classifications[field_name] = classification
                
                return classification
        
        return None
    
    def get_rule_for_classification(self, classification: DataClassification,
                                  compliance_regime: Optional[ComplianceRegime] = None) -> PrivacyRule:
        """
        Get the privacy rule for a data classification.
        
        Args:
            classification: The data classification.
            compliance_regime: The compliance regime to consider.
            
        Returns:
            The privacy rule to apply.
        """
        key = (classification.data_category, classification.sensitivity_level)
        
        if key in self.rule_index:
            rules = self.rule_index[key]
            
            # If compliance regime is specified, filter rules
            if compliance_regime:
                matching_rules = [r for r in rules if not r.compliance_regimes or 
                                compliance_regime in r.compliance_regimes]
                
                if matching_rules:
                    return matching_rules[0]
            
            # Otherwise, return the first rule
            if rules:
                return rules[0]
        
        # No matching rule, create a default rule
        return PrivacyRule(
            data_category=classification.data_category,
            sensitivity_level=classification.sensitivity_level,
            action=self.default_action
        )
    
    def apply_privacy_filtering(self, data: Dict[str, Any], 
                              compliance_regime: Optional[ComplianceRegime] = None) -> Dict[str, Any]:
        """
        Apply privacy filtering to a data record.
        
        Args:
            data: The data record to filter.
            compliance_regime: The compliance regime to consider.
            
        Returns:
            The filtered data record.
        """
        result = {}
        
        for field_name, value in data.items():
            # Handle nested dictionaries recursively
            if isinstance(value, dict):
                result[field_name] = self.apply_privacy_filtering(value, compliance_regime)
                continue
            
            # Handle lists of dictionaries recursively
            if isinstance(value, list) and value and isinstance(value[0], dict):
                result[field_name] = [self.apply_privacy_filtering(item, compliance_regime) 
                                     for item in value]
                continue
            
            # Classify the field
            classification = self.classify_field(field_name, value)
            
            if classification:
                # Get the rule to apply
                rule = self.get_rule_for_classification(classification, compliance_regime)
                
                # Apply the rule
                handler = self.handlers.get(rule.action)
                if handler:
                    result[field_name] = handler.apply(value, rule)
                else:
                    # No handler for this action, use the value as is
                    result[field_name] = value
            else:
                # No classification, use the value as is
                result[field_name] = value
        
        return result 