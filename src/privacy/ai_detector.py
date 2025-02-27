"""
AI-based detector for sensitive data in the AI-powered data retrieval application.
"""

import logging
import json
import re
from typing import Dict, List, Any, Optional
import time

from .core import DataDetector, DataCategory, SensitivityLevel, PrivacyAction

logger = logging.getLogger(__name__)

class AIBasedDetector(DataDetector):
    """
    Detector for sensitive data using AI models.
    """
    
    def __init__(self, ai_model, fallback_detector=None):
        """
        Initialize the AI-based detector.
        
        Args:
            ai_model: The AI model to use for detection.
            fallback_detector: Optional fallback detector to use if AI fails.
        """
        self.ai_model = ai_model
        self.fallback_detector = fallback_detector
        self.cache = {}  # Simple cache to avoid repeated AI calls
    
    def detect(self, field_name: str, value: Any) -> Optional[Dict[str, Any]]:
        """
        Detect if a field contains sensitive data using AI.
        
        Args:
            field_name: The name of the field.
            value: The value to check.
            
        Returns:
            Dictionary with category, sensitivity level, and recommended action,
            or None if not sensitive.
        """
        # Check cache first
        cache_key = f"{field_name}:{str(value)[:100]}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Prepare context for AI
        context = {
            "field_name": field_name,
            "value_type": type(value).__name__,
            "timestamp": time.time()
        }
        
        # Create prompt for AI
        prompt = self._create_detection_prompt(field_name, value, context)
        
        try:
            # Get AI response
            ai_response = self.ai_model.generate(prompt)
            
            # Parse response
            result = self._parse_ai_response(ai_response)
            
            # Cache result
            self.cache[cache_key] = result
            
            return result
        except Exception as e:
            logger.error(f"AI detection failed for field '{field_name}': {e}")
            
            # Fall back to traditional detector if available
            if self.fallback_detector:
                category = self.fallback_detector.detect(field_name, value)
                if category:
                    return {
                        "category": category,
                        "level": SensitivityLevel.CONFIDENTIAL,  # Default
                        "action": PrivacyAction.REDACT  # Default
                    }
            
            return None
    
    def _create_detection_prompt(self, field_name: str, value: Any, context: Dict[str, Any]) -> str:
        """
        Create a prompt for the AI model to detect sensitive data.
        
        Args:
            field_name: The name of the field.
            value: The value to check.
            context: Additional context.
            
        Returns:
            The prompt for the AI model.
        """
        # Format value for display
        if isinstance(value, str):
            display_value = value
        else:
            try:
                display_value = str(value)
            except:
                display_value = f"<{type(value).__name__}>"
        
        # Truncate long values
        if len(display_value) > 500:
            display_value = display_value[:500] + "..."
        
        return f"""
        You are a privacy expert analyzing data for sensitive information.
        
        Field name: {field_name}
        Field value: {display_value}
        
        Is this field likely to contain sensitive personal information? If yes, please classify it according to:
        
        1. Category (PERSONAL_IDENTIFIER, FINANCIAL, HEALTH, LOCATION, CONTACT, DEMOGRAPHIC, BIOMETRIC, etc.)
        2. Sensitivity level (PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED, SECRET)
        3. Recommended privacy action (REDACT, MASK, TOKENIZE, HASH, AGGREGATE, GENERALIZE, PERTURB, NONE)
        
        Respond in JSON format like this:
        {{"is_sensitive": true/false, "category": "CATEGORY", "level": "LEVEL", "action": "ACTION"}}
        
        If not sensitive, just respond with:
        {{"is_sensitive": false}}
        """
    
    def _parse_ai_response(self, ai_response: str) -> Optional[Dict[str, Any]]:
        """
        Parse the AI response to extract sensitivity information.
        
        Args:
            ai_response: The AI response to parse.
            
        Returns:
            Dictionary with category, sensitivity level, and recommended action,
            or None if not sensitive.
        """
        # Extract JSON from the AI response
        json_match = re.search(r'(\{.*\})', ai_response, re.DOTALL)
        if not json_match:
            return None
        
        try:
            result = json.loads(json_match.group(1))
            
            # Check if sensitive
            if not result.get("is_sensitive", False):
                return None
            
            # Convert string enums to actual enum values
            if "category" in result:
                try:
                    result["category"] = DataCategory(result["category"])
                except ValueError:
                    result["category"] = DataCategory.OTHER
            
            if "level" in result:
                try:
                    result["level"] = SensitivityLevel[result["level"]]
                except (ValueError, KeyError):
                    result["level"] = SensitivityLevel.CONFIDENTIAL
            
            if "action" in result:
                try:
                    result["action"] = PrivacyAction(result["action"])
                except ValueError:
                    result["action"] = PrivacyAction.REDACT
            
            return result
        except Exception as e:
            logger.error(f"Error parsing AI response: {e}")
            return None 