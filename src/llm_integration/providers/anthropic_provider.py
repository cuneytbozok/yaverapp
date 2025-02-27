"""
Anthropic provider interface for the LLM integration.
"""

import logging
import time
from typing import Dict, List, Any, Optional
import os

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

from ..core import LLMInterface, LLMConfig, LLMResponse, LLMProvider

logger = logging.getLogger(__name__)

class AnthropicProvider(LLMInterface):
    """
    Interface for Anthropic's LLM API.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Anthropic provider.
        
        Args:
            api_key: Anthropic API key. If None, it will be read from the ANTHROPIC_API_KEY environment variable.
        """
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        self.client = None
        
        if not ANTHROPIC_AVAILABLE:
            logger.warning("Anthropic package not installed. Please install it with: pip install anthropic")
        elif self.api_key:
            try:
                self.client = anthropic.Anthropic(api_key=self.api_key)
                logger.info("Anthropic client initialized")
            except Exception as e:
                logger.error(f"Error initializing Anthropic client: {e}")
                self.client = None
    
    def is_available(self) -> bool:
        """
        Check if the Anthropic provider is available.
        
        Returns:
            True if the provider is available, False otherwise.
        """
        return ANTHROPIC_AVAILABLE and self.client is not None
    
    def generate(self, prompt: str, config: LLMConfig) -> LLMResponse:
        """
        Generate a response from Anthropic.
        
        Args:
            prompt: The prompt text.
            config: The LLM configuration.
            
        Returns:
            The LLM response.
        """
        if not self.is_available():
            raise RuntimeError("Anthropic provider is not available")
        
        try:
            # Prepare the request
            system_prompt = config.additional_params.get("system_prompt", "")
            
            # Set up parameters
            params = {
                "model": config.model,
                "max_tokens": config.max_tokens or 1024,
                "temperature": config.temperature,
                "top_p": config.top_p,
                "messages": [{"role": "user", "content": prompt}]
            }
            
            # Add system prompt if provided
            if system_prompt:
                params["system"] = system_prompt
            
            # Add stop sequences if specified
            if config.stop_sequences:
                params["stop_sequences"] = config.stop_sequences
            
            # Make the API call
            start_time = time.time()
            response = self.client.messages.create(**params)
            end_time = time.time()
            
            # Extract the response text
            response_text = response.content[0].text
            
            # Extract usage information
            usage = {}
            if hasattr(response, 'usage'):
                usage = {
                    'input_tokens': response.usage.input_tokens,
                    'output_tokens': response.usage.output_tokens
                }
                usage['total_tokens'] = usage['input_tokens'] + usage['output_tokens']
            
            # Create metadata
            metadata = {
                'response_time': end_time - start_time,
                'stop_reason': response.stop_reason
            }
            
            return LLMResponse(
                text=response_text,
                model=config.model,
                provider=LLMProvider.ANTHROPIC,
                usage=usage,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error generating response from Anthropic: {e}")
            raise 