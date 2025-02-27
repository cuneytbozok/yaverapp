"""
Configuration module for LLM integration.
"""

import logging
import os
import json
from typing import Dict, Any, Optional, List
from .core import LLMConfig, LLMProvider

logger = logging.getLogger(__name__)

class LLMConfigManager:
    """
    Manages LLM configurations.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the LLM configuration manager.
        
        Args:
            config_path: Path to the configuration file. If None, it will look for
                        'llm_config.json' in the current directory or use environment variables.
        """
        self.config_path = config_path
        self.configs = {}
        self.default_provider = None
        self.fallback_providers = []
        
        # Load configuration
        self._load_config()
    
    def _load_config(self):
        """
        Load configuration from file or environment variables.
        """
        # Try to load from file first
        if self.config_path and os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    config_data = json.load(f)
                
                # Process provider configurations
                for provider_name, provider_config in config_data.get('providers', {}).items():
                    try:
                        provider = LLMProvider(provider_name.lower())
                        self.configs[provider] = self._create_config_from_dict(provider_config)
                    except ValueError:
                        logger.warning(f"Unknown provider: {provider_name}")
                
                # Set default provider
                default_provider_name = config_data.get('default_provider')
                if default_provider_name:
                    try:
                        self.default_provider = LLMProvider(default_provider_name.lower())
                    except ValueError:
                        logger.warning(f"Unknown default provider: {default_provider_name}")
                
                # Set fallback providers
                fallback_providers = config_data.get('fallback_providers', [])
                for provider_name in fallback_providers:
                    try:
                        self.fallback_providers.append(LLMProvider(provider_name.lower()))
                    except ValueError:
                        logger.warning(f"Unknown fallback provider: {provider_name}")
                
                logger.info(f"Loaded LLM configuration from {self.config_path}")
                return
            except Exception as e:
                logger.error(f"Error loading configuration from file: {e}")
        
        # Fall back to environment variables
        self._load_config_from_env()
    
    def _load_config_from_env(self):
        """
        Load configuration from environment variables.
        """
        # Check for OpenAI configuration
        if os.environ.get("OPENAI_API_KEY"):
            openai_config = {
                "model": os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo"),
                "temperature": float(os.environ.get("OPENAI_TEMPERATURE", "0.7")),
                "max_tokens": int(os.environ.get("OPENAI_MAX_TOKENS", "1024")),
                "top_p": float(os.environ.get("OPENAI_TOP_P", "1.0")),
                "timeout": int(os.environ.get("OPENAI_TIMEOUT", "30"))
            }
            self.configs[LLMProvider.OPENAI] = self._create_config_from_dict(openai_config)
            
            # Set as default if no default is set
            if not self.default_provider:
                self.default_provider = LLMProvider.OPENAI
        
        # Check for Anthropic configuration
        if os.environ.get("ANTHROPIC_API_KEY"):
            anthropic_config = {
                "model": os.environ.get("ANTHROPIC_MODEL", "claude-3-sonnet-20240229"),
                "temperature": float(os.environ.get("ANTHROPIC_TEMPERATURE", "0.7")),
                "max_tokens": int(os.environ.get("ANTHROPIC_MAX_TOKENS", "1024")),
                "top_p": float(os.environ.get("ANTHROPIC_TOP_P", "1.0")),
                "timeout": int(os.environ.get("ANTHROPIC_TIMEOUT", "30"))
            }
            self.configs[LLMProvider.ANTHROPIC] = self._create_config_from_dict(anthropic_config)
            
            # Set as default if no default is set and OpenAI is not available
            if not self.default_provider:
                self.default_provider = LLMProvider.ANTHROPIC
        
        # Check for local model configuration
        if os.environ.get("LOCAL_MODEL_PATH"):
            local_config = {
                "model": os.path.basename(os.environ.get("LOCAL_MODEL_PATH", "")),
                "temperature": float(os.environ.get("LOCAL_TEMPERATURE", "0.7")),
                "max_tokens": int(os.environ.get("LOCAL_MAX_TOKENS", "512")),
                "top_p": float(os.environ.get("LOCAL_TOP_P", "1.0")),
                "timeout": int(os.environ.get("LOCAL_TIMEOUT", "60"))
            }
            self.configs[LLMProvider.LLAMA] = self._create_config_from_dict(local_config)
            
            # Set as fallback if not already in fallbacks
            if LLMProvider.LLAMA not in self.fallback_providers:
                self.fallback_providers.append(LLMProvider.LLAMA)
        
        # Set fallback providers if not set and multiple providers are available
        if not self.fallback_providers and len(self.configs) > 1:
            for provider in self.configs:
                if provider != self.default_provider:
                    self.fallback_providers.append(provider)
        
        logger.info("Loaded LLM configuration from environment variables")
    
    def _create_config_from_dict(self, config_dict: Dict[str, Any]) -> LLMConfig:
        """
        Create an LLMConfig object from a dictionary.
        
        Args:
            config_dict: The configuration dictionary.
            
        Returns:
            An LLMConfig object.
        """
        # Extract known parameters
        model = config_dict.get("model", "")
        temperature = float(config_dict.get("temperature", 0.7))
        max_tokens = int(config_dict.get("max_tokens", 0)) or None
        top_p = float(config_dict.get("top_p", 1.0))
        frequency_penalty = float(config_dict.get("frequency_penalty", 0.0))
        presence_penalty = float(config_dict.get("presence_penalty", 0.0))
        timeout = int(config_dict.get("timeout", 30))
        
        # Extract stop sequences
        stop_sequences = config_dict.get("stop_sequences", [])
        
        # Put all other parameters in additional_params
        additional_params = {k: v for k, v in config_dict.items() if k not in [
            "model", "temperature", "max_tokens", "top_p", 
            "frequency_penalty", "presence_penalty", "timeout", "stop_sequences"
        ]}
        
        return LLMConfig(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
            timeout=timeout,
            stop_sequences=stop_sequences,
            additional_params=additional_params
        )
    
    def get_config(self, provider: LLMProvider) -> Optional[LLMConfig]:
        """
        Get the configuration for a provider.
        
        Args:
            provider: The LLM provider.
            
        Returns:
            The LLMConfig for the provider, or None if not found.
        """
        return self.configs.get(provider)
    
    def get_default_config(self) -> Optional[LLMConfig]:
        """
        Get the configuration for the default provider.
        
        Returns:
            The LLMConfig for the default provider, or None if not found.
        """
        if not self.default_provider:
            return None
        
        return self.get_config(self.default_provider)
    
    def get_fallback_configs(self) -> List[LLMConfig]:
        """
        Get the configurations for the fallback providers.
        
        Returns:
            A list of LLMConfig objects for the fallback providers.
        """
        return [self.get_config(provider) for provider in self.fallback_providers 
                if self.get_config(provider) is not None]
    
    def get_default_provider(self) -> Optional[LLMProvider]:
        """
        Get the default provider.
        
        Returns:
            The default LLMProvider, or None if not set.
        """
        return self.default_provider
    
    def get_fallback_providers(self) -> List[LLMProvider]:
        """
        Get the fallback providers.
        
        Returns:
            A list of fallback LLMProviders.
        """
        return self.fallback_providers 