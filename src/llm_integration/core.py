"""
Core LLM Integration module for AI-powered data retrieval application.
This module provides a unified interface for interacting with various LLM providers.
"""

import logging
import time
import json
import hashlib
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Callable
from dataclasses import dataclass, field
import os
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LLMProvider(Enum):
    """Enum representing different LLM providers."""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    HUGGINGFACE = "huggingface"
    LLAMA = "llama"  # For local Llama models
    CUSTOM = "custom"

class PromptTemplate:
    """
    Class for managing and formatting prompt templates.
    """
    
    def __init__(self, template: str, input_variables: List[str]):
        """
        Initialize a prompt template.
        
        Args:
            template: The template string with placeholders for variables.
            input_variables: List of variable names that can be used in the template.
        """
        self.template = template
        self.input_variables = input_variables
        
        # Validate that all input variables are present in the template
        for var in input_variables:
            if f"{{{var}}}" not in template:
                logger.warning(f"Input variable '{var}' not found in template")
    
    def format(self, **kwargs) -> str:
        """
        Format the template with the provided values.
        
        Args:
            **kwargs: Values for the input variables.
            
        Returns:
            The formatted prompt string.
        """
        # Check that all required variables are provided
        for var in self.input_variables:
            if var not in kwargs:
                raise ValueError(f"Missing required input variable: {var}")
        
        # Format the template
        try:
            return self.template.format(**kwargs)
        except KeyError as e:
            raise ValueError(f"Invalid input variable: {e}")
        except Exception as e:
            raise ValueError(f"Error formatting template: {e}")

@dataclass
class LLMResponse:
    """
    Represents a response from an LLM.
    """
    text: str
    model: str
    provider: LLMProvider
    usage: Dict[str, int] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        return self.text

@dataclass
class LLMConfig:
    """
    Configuration for an LLM.
    """
    provider: LLMProvider
    model: str
    api_key: Optional[str] = None
    api_endpoint: Optional[str] = None
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    top_p: float = 1.0
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    stop_sequences: List[str] = field(default_factory=list)
    timeout: int = 30
    retry_count: int = 3
    retry_delay: int = 2
    additional_params: Dict[str, Any] = field(default_factory=dict)

class LLMCache:
    """
    Cache for LLM responses to avoid redundant API calls.
    """
    
    def __init__(self, cache_dir: Optional[str] = None, max_size: int = 1000):
        """
        Initialize the LLM cache.
        
        Args:
            cache_dir: Directory to store cache files. If None, in-memory cache is used.
            max_size: Maximum number of items to keep in the in-memory cache.
        """
        self.cache_dir = cache_dir
        self.max_size = max_size
        self.in_memory_cache = {}
        
        # Create cache directory if it doesn't exist
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
    
    def get(self, key: str) -> Optional[LLMResponse]:
        """
        Get a cached response.
        
        Args:
            key: The cache key.
            
        Returns:
            The cached response, or None if not found.
        """
        # Check in-memory cache first
        if key in self.in_memory_cache:
            logger.debug(f"Cache hit (memory): {key}")
            return self.in_memory_cache[key]
        
        # Check file cache if enabled
        if self.cache_dir:
            cache_file = os.path.join(self.cache_dir, f"{key}.json")
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                    
                    response = LLMResponse(
                        text=data['text'],
                        model=data['model'],
                        provider=LLMProvider(data['provider']),
                        usage=data['usage'],
                        metadata=data['metadata']
                    )
                    
                    # Add to in-memory cache
                    self.in_memory_cache[key] = response
                    
                    logger.debug(f"Cache hit (file): {key}")
                    return response
                except Exception as e:
                    logger.warning(f"Error reading cache file: {e}")
        
        logger.debug(f"Cache miss: {key}")
        return None
    
    def set(self, key: str, response: LLMResponse):
        """
        Cache a response.
        
        Args:
            key: The cache key.
            response: The response to cache.
        """
        # Add to in-memory cache
        self.in_memory_cache[key] = response
        
        # Trim in-memory cache if it's too large
        if len(self.in_memory_cache) > self.max_size:
            # Remove oldest items
            keys_to_remove = list(self.in_memory_cache.keys())[:-self.max_size]
            for k in keys_to_remove:
                del self.in_memory_cache[k]
        
        # Write to file cache if enabled
        if self.cache_dir:
            cache_file = os.path.join(self.cache_dir, f"{key}.json")
            try:
                data = {
                    'text': response.text,
                    'model': response.model,
                    'provider': response.provider.value,
                    'usage': response.usage,
                    'metadata': response.metadata
                }
                
                with open(cache_file, 'w') as f:
                    json.dump(data, f)
                
                logger.debug(f"Cached response to file: {key}")
            except Exception as e:
                logger.warning(f"Error writing cache file: {e}")
    
    def generate_key(self, prompt: str, model: str, provider: LLMProvider, 
                    params: Dict[str, Any]) -> str:
        """
        Generate a cache key from a prompt and parameters.
        
        Args:
            prompt: The prompt text.
            model: The model name.
            provider: The LLM provider.
            params: Additional parameters that affect the response.
            
        Returns:
            A cache key string.
        """
        # Create a string representation of the key components
        key_str = f"{prompt}|{model}|{provider.value}|{json.dumps(params, sort_keys=True)}"
        
        # Hash the string to create a fixed-length key
        return hashlib.md5(key_str.encode()).hexdigest()

class LLMInterface(ABC):
    """
    Abstract base class for LLM provider interfaces.
    """
    
    @abstractmethod
    def generate(self, prompt: str, config: LLMConfig) -> LLMResponse:
        """
        Generate a response from the LLM.
        
        Args:
            prompt: The prompt text.
            config: The LLM configuration.
            
        Returns:
            The LLM response.
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if the LLM provider is available.
        
        Returns:
            True if the provider is available, False otherwise.
        """
        pass

class ContextManager:
    """
    Manages context window limitations for LLMs.
    """
    
    def __init__(self, max_tokens: int, token_counter: Callable[[str], int]):
        """
        Initialize the context manager.
        
        Args:
            max_tokens: Maximum number of tokens the model can handle.
            token_counter: Function that counts tokens in a string.
        """
        self.max_tokens = max_tokens
        self.token_counter = token_counter
    
    def fit_to_context_window(self, prompt: str, documents: List[Dict[str, Any]], 
                             max_output_tokens: int) -> str:
        """
        Fit documents into the context window.
        
        Args:
            prompt: The base prompt text.
            documents: List of documents to include in the context.
            max_output_tokens: Maximum number of tokens to reserve for the output.
            
        Returns:
            The prompt with as many documents as can fit in the context window.
        """
        # Count tokens in the base prompt
        prompt_tokens = self.token_counter(prompt)
        
        # Calculate available tokens for documents
        available_tokens = self.max_tokens - prompt_tokens - max_output_tokens
        
        if available_tokens <= 0:
            logger.warning("Base prompt is too large for context window")
            # Truncate the prompt if possible
            return self._truncate_text(prompt, self.max_tokens - max_output_tokens)
        
        # Sort documents by relevance (assuming they have a 'relevance' field)
        sorted_docs = sorted(documents, key=lambda x: x.get('relevance', 0), reverse=True)
        
        included_docs = []
        current_tokens = 0
        
        for doc in sorted_docs:
            # Extract the document text
            doc_text = doc.get('content', '')
            if not doc_text:
                continue
            
            # Count tokens in this document
            doc_tokens = self.token_counter(doc_text)
            
            # Check if this document fits
            if current_tokens + doc_tokens <= available_tokens:
                included_docs.append(doc)
                current_tokens += doc_tokens
            else:
                # Try to include a truncated version of the document
                remaining_tokens = available_tokens - current_tokens
                if remaining_tokens > 50:  # Only include if we can add something meaningful
                    truncated_text = self._truncate_text(doc_text, remaining_tokens)
                    doc_copy = doc.copy()
                    doc_copy['content'] = truncated_text
                    doc_copy['truncated'] = True
                    included_docs.append(doc_copy)
                break
        
        # Format the prompt with the included documents
        context_str = "\n\n".join([f"Document: {doc.get('name', 'Unnamed')}\n{doc.get('content', '')}" 
                                 for doc in included_docs])
        
        return prompt.replace("{context}", context_str)
    
    def _truncate_text(self, text: str, max_tokens: int) -> str:
        """
        Truncate text to fit within a token limit.
        
        Args:
            text: The text to truncate.
            max_tokens: Maximum number of tokens.
            
        Returns:
            The truncated text.
        """
        if self.token_counter(text) <= max_tokens:
            return text
        
        # Simple truncation strategy: keep truncating until it fits
        # A more sophisticated approach would be to use sentence boundaries
        while self.token_counter(text) > max_tokens:
            text = text[:int(len(text) * 0.9)]  # Remove 10% at a time
        
        return text + "..." 