"""
Manager for privacy functionality with AI-centric approach.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import json

from .core import (
    PrivacyEngine, ComplianceRegime, DataCategory, SensitivityLevel,
    PrivacyAction, PrivacyRule
)
from .ai_detector import AIBasedDetector
from .detectors import RegexPatternDetector, CompositeDetector
from .handlers import (
    RedactionHandler, MaskingHandler, TokenizationHandler, HashingHandler,
    AggregationHandler, GeneralizationHandler, PerturbationHandler
)

logger = logging.getLogger(__name__)

class PrivacyManager:
    """
    Manager for privacy functionality with AI-centric approach.
    """
    
    def __init__(self, config: Dict[str, Any], ai_model):
        """
        Initialize the privacy manager.
        
        Args:
            config: The privacy configuration.
            ai_model: The AI model to use for detection and filtering.
        """
        self.config = config
        self.ai_model = ai_model
        
        # Initialize fallback detector for reliability
        self.fallback_detector = self._init_fallback_detector(config.get("fallback", {}))
        
        # Initialize AI detector
        self.ai_detector = AIBasedDetector(ai_model, self.fallback_detector)
        
        # Initialize handlers
        self.handlers = self._init_handlers(config.get("handlers", {}))
        
        # Initialize compliance regimes
        self.compliance_regimes = self._init_compliance_regimes(config.get("compliance", {}))
        
        # Default compliance regime
        default_regime_name = config.get("default_compliance_regime")
        self.default_compliance_regime = None
        if default_regime_name:
            try:
                self.default_compliance_regime = ComplianceRegime(default_regime_name)
            except ValueError:
                logger.warning(f"Unknown compliance regime: {default_regime_name}")
    
    def _init_fallback_detector(self, config: Dict[str, Any]) -> DataDetector:
        """
        Initialize fallback detector for when AI fails.
        
        Args:
            config: The fallback configuration.
            
        Returns:
            A fallback detector.
        """
        # Only use regex detector as fallback
        return RegexPatternDetector()
    
    def _init_handlers(self, config: Dict[str, Any]) -> Dict[PrivacyAction, Any]:
        """
        Initialize privacy handlers.
        
        Args:
            config: The handler configuration.
            
        Returns:
            A dictionary mapping privacy actions to handlers.
        """
        handlers = {}
        
        # Add handlers for all privacy actions
        handlers[PrivacyAction.REDACT] = RedactionHandler()
        handlers[PrivacyAction.MASK] = MaskingHandler()
        handlers[PrivacyAction.TOKENIZE] = TokenizationHandler(
            token_map=config.get("token_map"),
            salt=config.get("tokenization_salt")
        )
        handlers[PrivacyAction.HASH] = HashingHandler(
            salt=config.get("hash_salt")
        )
        handlers[PrivacyAction.AGGREGATE] = AggregationHandler()
        handlers[PrivacyAction.GENERALIZE] = GeneralizationHandler()
        handlers[PrivacyAction.PERTURB] = PerturbationHandler()
        handlers[PrivacyAction.NONE] = None
        
        return handlers
    
    def _init_compliance_regimes(self, config: Dict[str, Any]) -> Dict[ComplianceRegime, Dict[str, Any]]:
        """
        Initialize compliance regimes.
        
        Args:
            config: The compliance configuration.
            
        Returns:
            A dictionary mapping compliance regimes to their configurations.
        """
        compliance_regimes = {}
        
        for regime_name, regime_config in config.items():
            try:
                regime = ComplianceRegime(regime_name)
                compliance_regimes[regime] = regime_config
            except ValueError:
                logger.warning(f"Unknown compliance regime: {regime_name}")
        
        return compliance_regimes
    
    def apply_privacy_filtering(self, data: Dict[str, Any], user_id: str = None,
                              purpose: str = None,
                              compliance_regime: Optional[ComplianceRegime] = None) -> Dict[str, Any]:
        """
        Apply privacy filtering to data using AI.
        
        Args:
            data: The data to filter.
            user_id: The ID of the user accessing the data.
            purpose: The purpose of data access.
            compliance_regime: The compliance regime to use.
            
        Returns:
            The filtered data.
        """
        # Use specified compliance regime or default
        effective_regime = compliance_regime or self.default_compliance_regime
        
        # Create context for AI
        context = {
            "user_id": user_id,
            "purpose": purpose,
            "compliance_regime": effective_regime.value if effective_regime else None
        }
        
        # Process data
        return self._process_data_with_ai(data, context)
    
    def _process_data_with_ai(self, data: Any, context: Dict[str, Any]) -> Any:
        """
        Process data with AI for privacy filtering.
        
        Args:
            data: The data to process.
            context: The context for processing.
            
        Returns:
            The processed data.
        """
        # Handle different data types
        if isinstance(data, dict):
            return self._process_dict_with_ai(data, context)
        elif isinstance(data, list):
            return self._process_list_with_ai(data, context)
        else:
            # Primitive value, nothing to process
            return data
    
    def _process_dict_with_ai(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a dictionary with AI for privacy filtering.
        
        Args:
            data: The dictionary to process.
            context: The context for processing.
            
        Returns:
            The processed dictionary.
        """
        result = {}
        
        for field_name, value in data.items():
            # Handle nested structures recursively
            if isinstance(value, (dict, list)):
                result[field_name] = self._process_data_with_ai(value, context)
                continue
            
            # Detect if field contains sensitive data
            detection_result = self.ai_detector.detect(field_name, value)
            
            if detection_result:
                # Apply privacy action
                handler = self.handlers.get(detection_result["action"])
                if handler:
                    # Create a simple rule
                    rule = PrivacyRule(
                        data_category=detection_result["category"],
                        sensitivity_level=detection_result["level"],
                        action=detection_result["action"]
                    )
                    result[field_name] = handler.apply(value, rule)
                else:
                    # No handler, use as is
                    result[field_name] = value
            else:
                # Not sensitive, use as is
                result[field_name] = value
        
        return result
    
    def _process_list_with_ai(self, data: List[Any], context: Dict[str, Any]) -> List[Any]:
        """
        Process a list with AI for privacy filtering.
        
        Args:
            data: The list to process.
            context: The context for processing.
            
        Returns:
            The processed list.
        """
        # If list of dictionaries, process each dictionary
        if data and isinstance(data[0], dict):
            return [self._process_dict_with_ai(item, context) for item in data]
        
        # Otherwise, return as is (we don't process lists of primitives)
        return data 