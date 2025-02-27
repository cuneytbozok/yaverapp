"""
LLM Manager module for coordinating between different LLM providers.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Union
import random
from .core import (
    LLMProvider, LLMConfig, LLMResponse, LLMInterface, 
    LLMCache, PromptTemplate, ContextManager
)

logger = logging.getLogger(__name__)

class LLMManager:
    """
    Manages interactions with multiple LLM providers.
    """
    
    def __init__(self, providers: Dict[LLMProvider, LLMInterface], 
                default_config: LLMConfig,
                cache: Optional[LLMCache] = None,
                context_manager: Optional[ContextManager] = None):
        """
        Initialize the LLM manager.
        
        Args:
            providers: Dictionary mapping providers to their interfaces.
            default_config: Default configuration to use.
            cache: Optional cache for LLM responses.
            context_manager: Optional context manager for handling context limitations.
        """
        self.providers = providers
        self.default_config = default_config
        self.cache = cache or LLMCache()
        self.context_manager = context_manager
        self.prompt_templates = {}
    
    def add_prompt_template(self, name: str, template: PromptTemplate):
        """
        Add a prompt template.
        
        Args:
            name: The template name.
            template: The prompt template.
        """
        self.prompt_templates[name] = template
        logger.info(f"Added prompt template: {name}")
    
    def get_prompt_template(self, name: str) -> Optional[PromptTemplate]:
        """
        Get a prompt template by name.
        
        Args:
            name: The template name.
            
        Returns:
            The prompt template, or None if not found.
        """
        return self.prompt_templates.get(name)
    
    def generate(self, prompt: str, config: Optional[LLMConfig] = None, 
                use_cache: bool = True, retry_different_provider: bool = True) -> LLMResponse:
        """
        Generate a response from an LLM.
        
        Args:
            prompt: The prompt text.
            config: Optional configuration to use instead of the default.
            use_cache: Whether to use the cache.
            retry_different_provider: Whether to try a different provider on failure.
            
        Returns:
            The LLM response.
        """
        config = config or self.default_config
        
        # Check cache if enabled
        if use_cache:
            cache_key = self.cache.generate_key(
                prompt=prompt,
                model=config.model,
                provider=config.provider,
                params={
                    'temperature': config.temperature,
                    'max_tokens': config.max_tokens,
                    'top_p': config.top_p
                }
            )
            
            cached_response = self.cache.get(cache_key)
            if cached_response:
                logger.info(f"Using cached response for {config.provider.value}/{config.model}")
                return cached_response
        
        # Get the provider interface
        provider_interface = self.providers.get(config.provider)
        if not provider_interface:
            raise ValueError(f"Provider not configured: {config.provider.value}")
        
        # Check if the provider is available
        if not provider_interface.is_available():
            logger.warning(f"Provider {config.provider.value} is not available")
            if retry_different_provider:
                return self._retry_with_different_provider(prompt, config, use_cache)
            else:
                raise RuntimeError(f"Provider {config.provider.value} is not available")
        
        # Try to generate a response with retries
        for attempt in range(config.retry_count + 1):
            try:
                response = provider_interface.generate(prompt, config)
                
                # Cache the response if caching is enabled
                if use_cache:
                    self.cache.set(cache_key, response)
                
                return response
                
            except Exception as e:
                logger.warning(f"Error generating response (attempt {attempt+1}/{config.retry_count+1}): {e}")
                
                if attempt < config.retry_count:
                    # Wait before retrying
                    time.sleep(config.retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    # All retries failed
                    if retry_different_provider:
                        return self._retry_with_different_provider(prompt, config, use_cache)
                    else:
                        raise RuntimeError(f"Failed to generate response after {config.retry_count+1} attempts") from e
    
    def _retry_with_different_provider(self, prompt: str, config: LLMConfig, 
                                     use_cache: bool) -> LLMResponse:
        """
        Retry with a different provider.
        
        Args:
            prompt: The prompt text.
            config: The original configuration.
            use_cache: Whether to use the cache.
            
        Returns:
            The LLM response from the alternative provider.
        """
        # Get available providers excluding the current one
        available_providers = [
            p for p in self.providers.keys() 
            if p != config.provider and self.providers[p].is_available()
        ]
        
        if not available_providers:
            raise RuntimeError("No alternative providers available")
        
        # Select a random alternative provider
        alternative_provider = random.choice(available_providers)
        logger.info(f"Retrying with alternative provider: {alternative_provider.value}")
        
        # Create a new configuration with the alternative provider
        alternative_config = LLMConfig(
            provider=alternative_provider,
            model=self.default_config.model if alternative_provider == self.default_config.provider else config.model,
            api_key=None,  # The provider interface should have its own API key
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            top_p=config.top_p,
            frequency_penalty=config.frequency_penalty,
            presence_penalty=config.presence_penalty,
            stop_sequences=config.stop_sequences,
            timeout=config.timeout,
            retry_count=1,  # Reduce retry count for the alternative
            retry_delay=config.retry_delay
        )
        
        # Generate with the alternative provider
        return self.generate(prompt, alternative_config, use_cache, retry_different_provider=False)
    
    def generate_from_template(self, template_name: str, variables: Dict[str, Any], 
                              config: Optional[LLMConfig] = None, 
                              use_cache: bool = True) -> LLMResponse:
        """
        Generate a response using a prompt template.
        
        Args:
            template_name: The name of the template to use.
            variables: Variables to fill in the template.
            config: Optional configuration to use.
            use_cache: Whether to use the cache.
            
        Returns:
            The LLM response.
        """
        template = self.get_prompt_template(template_name)
        if not template:
            raise ValueError(f"Prompt template not found: {template_name}")
        
        # Format the prompt
        prompt = template.format(**variables)
        
        # Generate the response
        return self.generate(prompt, config, use_cache)
    
    def generate_with_context(self, template_name: str, variables: Dict[str, Any], 
                             documents: List[Dict[str, Any]], 
                             config: Optional[LLMConfig] = None,
                             max_output_tokens: int = 500) -> LLMResponse:
        """
        Generate a response with document context, handling context window limitations.
        
        Args:
            template_name: The name of the template to use.
            variables: Variables to fill in the template.
            documents: List of documents to include in the context.
            config: Optional configuration to use.
            max_output_tokens: Maximum tokens to reserve for the output.
            
        Returns:
            The LLM response.
        """
        if not self.context_manager:
            raise ValueError("Context manager is required for generate_with_context")
        
        template = self.get_prompt_template(template_name)
        if not template:
            raise ValueError(f"Prompt template not found: {template_name}")
        
        # Format the template with a placeholder for the context
        variables_with_placeholder = variables.copy()
        variables_with_placeholder['context'] = "{context}"
        base_prompt = template.format(**variables_with_placeholder)
        
        # Use the context manager to fit documents into the context window
        config = config or self.default_config
        prompt = self.context_manager.fit_to_context_window(
            prompt=base_prompt,
            documents=documents,
            max_output_tokens=max_output_tokens
        )
        
        # Generate the response
        return self.generate(prompt, config) 