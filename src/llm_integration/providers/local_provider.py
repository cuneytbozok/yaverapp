"""
Local model provider interface for the LLM integration.
"""

import logging
import time
from typing import Dict, List, Any, Optional
import os

# Try to import the required packages
try:
    from llama_cpp import Llama
    LLAMA_CPP_AVAILABLE = True
except ImportError:
    LLAMA_CPP_AVAILABLE = False

try:
    import ctransformers
    CTRANSFORMERS_AVAILABLE = True
except ImportError:
    CTRANSFORMERS_AVAILABLE = False

from ..core import LLMInterface, LLMConfig, LLMResponse, LLMProvider

logger = logging.getLogger(__name__)

class LocalProvider(LLMInterface):
    """
    Interface for local LLM models.
    """
    
    def __init__(self, model_path: Optional[str] = None):
        """
        Initialize the local model provider.
        
        Args:
            model_path: Path to the model file. If None, it will be read from the LOCAL_MODEL_PATH environment variable.
        """
        self.model_path = model_path or os.environ.get("LOCAL_MODEL_PATH")
        self.model = None
        self.model_type = None
        
        if not self.model_path:
            logger.warning("No model path provided for local provider")
            return
        
        if not os.path.exists(self.model_path):
            logger.warning(f"Model file not found: {self.model_path}")
            return
        
        # Try to load the model
        self._load_model()
    
    def _load_model(self):
        """
        Load the model based on the file extension.
        """
        try:
            # Determine model type based on file extension
            _, ext = os.path.splitext(self.model_path)
            ext = ext.lower()
            
            if ext in ('.gguf', '.bin'):
                # Try to load with llama.cpp
                if LLAMA_CPP_AVAILABLE:
                    logger.info(f"Loading model with llama.cpp: {self.model_path}")
                    self.model = Llama(
                        model_path=self.model_path,
                        n_ctx=2048,  # Default context size
                        n_threads=os.cpu_count() or 4
                    )
                    self.model_type = "llama_cpp"
                    logger.info("Model loaded successfully with llama.cpp")
                # Try to load with ctransformers if llama.cpp is not available
                elif CTRANSFORMERS_AVAILABLE:
                    logger.info(f"Loading model with ctransformers: {self.model_path}")
                    self.model = ctransformers.AutoModelForCausalLM.from_pretrained(
                        self.model_path,
                        model_type="llama"
                    )
                    self.model_type = "ctransformers"
                    logger.info("Model loaded successfully with ctransformers")
                else:
                    logger.error("Neither llama.cpp nor ctransformers is available")
            else:
                logger.error(f"Unsupported model file extension: {ext}")
                
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            self.model = None
            self.model_type = None
    
    def is_available(self) -> bool:
        """
        Check if the local provider is available.
        
        Returns:
            True if the provider is available, False otherwise.
        """
        return self.model is not None
    
    def generate(self, prompt: str, config: LLMConfig) -> LLMResponse:
        """
        Generate a response from the local model.
        
        Args:
            prompt: The prompt text.
            config: The LLM configuration.
            
        Returns:
            The LLM response.
        """
        if not self.is_available():
            raise RuntimeError("Local model provider is not available")
        
        try:
            # Set up parameters
            max_tokens = config.max_tokens or 512
            temperature = config.temperature
            top_p = config.top_p
            
            # Generate response
            start_time = time.time()
            
            if self.model_type == "llama_cpp":
                # Generate with llama.cpp
                response = self.model(
                    prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    top_p=top_p,
                    stop=config.stop_sequences or []
                )
                response_text = response["choices"][0]["text"]
                
                # Estimate token usage (llama.cpp doesn't provide this directly)
                prompt_tokens = len(prompt.split())
                completion_tokens = len(response_text.split())
                
            elif self.model_type == "ctransformers":
                # Generate with ctransformers
                response = self.model(
                    prompt,
                    max_new_tokens=max_tokens,
                    temperature=temperature,
                    top_p=top_p
                )
                response_text = response
                
                # Estimate token usage
                prompt_tokens = len(prompt.split())
                completion_tokens = len(response_text.split())
            
            end_time = time.time()
            
            # Create usage information
            usage = {
                'prompt_tokens': prompt_tokens,
                'completion_tokens': completion_tokens,
                'total_tokens': prompt_tokens + completion_tokens
            }
            
            # Create metadata
            metadata = {
                'response_time': end_time - start_time,
                'model_type': self.model_type
            }
            
            return LLMResponse(
                text=response_text,
                model=os.path.basename(self.model_path),
                provider=LLMProvider.LLAMA,
                usage=usage,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error generating response from local model: {e}")
            raise 