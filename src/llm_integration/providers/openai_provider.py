"""
OpenAI provider interface for the LLM integration.
"""

import logging
import time
from typing import Dict, List, Any, Optional
import os

try:
    import openai
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from ..core import LLMInterface, LLMConfig, LLMResponse, LLMProvider

logger = logging.getLogger(__name__)

class OpenAIProvider(LLMInterface):
    """
    Interface for OpenAI's LLM API.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the OpenAI provider.
        
        Args:
            api_key: OpenAI API key. If None, it will be read from the OPENAI_API_KEY environment variable.
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.client = None
        
        if not OPENAI_AVAILABLE:
            logger.warning("OpenAI package not installed. Please install it with: pip install openai")
        elif self.api_key:
            try:
                self.client = OpenAI(api_key=self.api_key)
                logger.info("OpenAI client initialized")
            except Exception as e:
                logger.error(f"Error initializing OpenAI client: {e}")
                self.client = None
    
    def is_available(self) -> bool:
        """
        Check if the OpenAI provider is available.
        
        Returns:
            True if the provider is available, False otherwise.
        """
        return OPENAI_AVAILABLE and self.client is not None
    
    def generate(self, prompt: str, config: LLMConfig) -> LLMResponse:
        """
        Generate a response from OpenAI.
        
        Args:
            prompt: The prompt text.
            config: The LLM configuration.
            
        Returns:
            The LLM response.
        """
        if not self.is_available():
            raise RuntimeError("OpenAI provider is not available")
        
        try:
            # Prepare the request
            messages = [{"role": "user", "content": prompt}]
            
            # Set up parameters
            params = {
                "model": config.model,
                "messages": messages,
                "temperature": config.temperature,
                "top_p": config.top_p,
                "n": 1,
                "timeout": config.timeout
            }
            
            # Add max_tokens if specified
            if config.max_tokens:
                params["max_tokens"] = config.max_tokens
            
            # Add stop sequences if specified
            if config.stop_sequences:
                params["stop"] = config.stop_sequences
            
            # Add frequency and presence penalties if non-zero
            if config.frequency_penalty != 0:
                params["frequency_penalty"] = config.frequency_penalty
            if config.presence_penalty != 0:
                params["presence_penalty"] = config.presence_penalty
            
            # Add any additional parameters
            params.update(config.additional_params)
            
            # Make the API call
            start_time = time.time()
            response = self.client.chat.completions.create(**params)
            end_time = time.time()
            
            # Extract the response text
            response_text = response.choices[0].message.content
            
            # Extract usage information
            usage = {}
            if hasattr(response, 'usage'):
                usage = {
                    'prompt_tokens': response.usage.prompt_tokens,
                    'completion_tokens': response.usage.completion_tokens,
                    'total_tokens': response.usage.total_tokens
                }
            
            # Create metadata
            metadata = {
                'response_time': end_time - start_time,
                'finish_reason': response.choices[0].finish_reason
            }
            
            return LLMResponse(
                text=response_text,
                model=config.model,
                provider=LLMProvider.OPENAI,
                usage=usage,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error generating response from OpenAI: {e}")
            raise 