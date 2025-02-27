"""
Embedding models for the RAG system.
"""

import logging
import os
import numpy as np
from typing import List, Dict, Any, Optional
from .core import EmbeddingModel, EmbeddingModelType, EmbeddingModelConfig

logger = logging.getLogger(__name__)

class OpenAIEmbeddingModel(EmbeddingModel):
    """
    Embedding model using OpenAI's API.
    """
    
    def __init__(self, config: EmbeddingModelConfig):
        """
        Initialize the OpenAI embedding model.
        
        Args:
            config: The embedding model configuration.
        """
        self.model_name = config.model_name
        self.api_key = config.api_key or os.environ.get("OPENAI_API_KEY")
        self.dimension = config.dimension
        self.batch_size = config.batch_size
        
        if not self.api_key:
            logger.warning("No API key provided for OpenAI embedding model")
        
        try:
            import openai
            from openai import OpenAI
            self.client = OpenAI(api_key=self.api_key)
            logger.info(f"Initialized OpenAI embedding model: {self.model_name}")
        except ImportError:
            logger.error("Failed to import OpenAI. Please install it with: pip install openai")
            self.client = None
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: The texts to generate embeddings for.
            
        Returns:
            A list of embeddings.
        """
        if not self.client:
            raise RuntimeError("OpenAI client is not initialized")
        
        embeddings = []
        
        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i+self.batch_size]
            
            try:
                response = self.client.embeddings.create(
                    model=self.model_name,
                    input=batch
                )
                
                batch_embeddings = [item.embedding for item in response.data]
                embeddings.extend(batch_embeddings)
                
            except Exception as e:
                logger.error(f"Error generating embeddings with OpenAI: {e}")
                # Return empty embeddings for this batch
                empty_embeddings = [[0.0] * self.dimension for _ in range(len(batch))]
                embeddings.extend(empty_embeddings)
        
        return embeddings
    
    def get_dimension(self) -> int:
        """
        Get the dimension of the embeddings.
        
        Returns:
            The dimension of the embeddings.
        """
        return self.dimension

class HuggingFaceEmbeddingModel(EmbeddingModel):
    """
    Embedding model using Hugging Face's transformers.
    """
    
    def __init__(self, config: EmbeddingModelConfig):
        """
        Initialize the Hugging Face embedding model.
        
        Args:
            config: The embedding model configuration.
        """
        self.model_name = config.model_name
        self.dimension = config.dimension
        self.batch_size = config.batch_size
        self.device = config.additional_params.get("device", "cpu")
        self.model = None
        self.tokenizer = None
        
        try:
            from transformers import AutoModel, AutoTokenizer
            import torch
            
            # Load the model and tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModel.from_pretrained(self.model_name)
            
            # Move the model to the specified device
            self.model.to(self.device)
            
            # Set the model to evaluation mode
            self.model.eval()
            
            logger.info(f"Initialized Hugging Face embedding model: {self.model_name}")
            
        except ImportError:
            logger.error("Failed to import transformers. Please install it with: pip install transformers torch")
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: The texts to generate embeddings for.
            
        Returns:
            A list of embeddings.
        """
        if not self.model or not self.tokenizer:
            raise RuntimeError("Hugging Face model is not initialized")
        
        import torch
        
        embeddings = []
        
        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i+self.batch_size]
            
            try:
                # Tokenize the batch
                encoded_input = self.tokenizer(
                    batch,
                    padding=True,
                    truncation=True,
                    return_tensors="pt"
                ).to(self.device)
                
                # Generate embeddings
                with torch.no_grad():
                    model_output = self.model(**encoded_input)
                    
                    # Use the [CLS] token embedding as the sentence embedding
                    # or mean pooling depending on the model architecture
                    if hasattr(model_output, "pooler_output"):
                        # BERT-like models
                        batch_embeddings = model_output.pooler_output
                    else:
                        # For models without pooler_output, use mean pooling
                        attention_mask = encoded_input["attention_mask"]
                        token_embeddings = model_output.last_hidden_state
                        
                        # Mask padded tokens
                        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
                        sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
                        sum_mask = torch.sum(input_mask_expanded, 1)
                        batch_embeddings = sum_embeddings / sum_mask
                
                # Convert to list of lists
                batch_embeddings = batch_embeddings.cpu().numpy().tolist()
                embeddings.extend(batch_embeddings)
                
            except Exception as e:
                logger.error(f"Error generating embeddings with Hugging Face: {e}")
                # Return empty embeddings for this batch
                empty_embeddings = [[0.0] * self.dimension for _ in range(len(batch))]
                embeddings.extend(empty_embeddings)
        
        return embeddings
    
    def get_dimension(self) -> int:
        """
        Get the dimension of the embeddings.
        
        Returns:
            The dimension of the embeddings.
        """
        return self.dimension

class SentenceTransformersEmbeddingModel(EmbeddingModel):
    """
    Embedding model using sentence-transformers.
    """
    
    def __init__(self, config: EmbeddingModelConfig):
        """
        Initialize the sentence-transformers embedding model.
        
        Args:
            config: The embedding model configuration.
        """
        self.model_name = config.model_name
        self.dimension = config.dimension
        self.batch_size = config.batch_size
        self.model = None
        
        try:
            from sentence_transformers import SentenceTransformer
            
            # Load the model
            self.model = SentenceTransformer(self.model_name)
            
            logger.info(f"Initialized sentence-transformers model: {self.model_name}")
            
        except ImportError:
            logger.error("Failed to import sentence-transformers. Please install it with: pip install sentence-transformers")
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: The texts to generate embeddings for.
            
        Returns:
            A list of embeddings.
        """
        if not self.model:
            raise RuntimeError("sentence-transformers model is not initialized")
        
        try:
            # Generate embeddings
            embeddings = self.model.encode(
                texts,
                batch_size=self.batch_size,
                show_progress_bar=False,
                convert_to_numpy=True
            )
            
            # Convert to list of lists
            return embeddings.tolist()
            
        except Exception as e:
            logger.error(f"Error generating embeddings with sentence-transformers: {e}")
            # Return empty embeddings
            return [[0.0] * self.dimension for _ in range(len(texts))]
    
    def get_dimension(self) -> int:
        """
        Get the dimension of the embeddings.
        
        Returns:
            The dimension of the embeddings.
        """
        return self.dimension

def create_embedding_model(config: EmbeddingModelConfig) -> EmbeddingModel:
    """
    Create an embedding model based on the configuration.
    
    Args:
        config: The embedding model configuration.
        
    Returns:
        An embedding model.
    """
    if config.model_type == EmbeddingModelType.OPENAI:
        return OpenAIEmbeddingModel(config)
    elif config.model_type == EmbeddingModelType.HUGGINGFACE:
        return HuggingFaceEmbeddingModel(config)
    elif config.model_type == EmbeddingModelType.SENTENCE_TRANSFORMERS:
        return SentenceTransformersEmbeddingModel(config)
    else:
        raise ValueError(f"Unsupported embedding model type: {config.model_type}") 