"""
Core RAG (Retrieval Augmented Generation) module for AI-powered data retrieval application.
This module provides the foundation for document processing, embedding generation,
vector storage, and context assembly.
"""

import logging
import time
import hashlib
import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from dataclasses import dataclass, field
import os
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EmbeddingModelType(Enum):
    """Enum representing different embedding model types."""
    OPENAI = "openai"
    HUGGINGFACE = "huggingface"
    SENTENCE_TRANSFORMERS = "sentence_transformers"
    TENSORFLOW = "tensorflow"
    CUSTOM = "custom"

class VectorStoreType(Enum):
    """Enum representing different vector store types."""
    FAISS = "faiss"
    PINECONE = "pinecone"
    MILVUS = "milvus"
    QDRANT = "qdrant"
    CHROMA = "chroma"
    IN_MEMORY = "in_memory"

class ChunkingStrategy(Enum):
    """Enum representing different document chunking strategies."""
    FIXED_SIZE = "fixed_size"
    SENTENCE = "sentence"
    PARAGRAPH = "paragraph"
    SEMANTIC = "semantic"
    HYBRID = "hybrid"

@dataclass
class Document:
    """Represents a document to be processed by the RAG system."""
    id: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None
    
    def __post_init__(self):
        """Generate an ID if not provided."""
        if not self.id:
            self.id = hashlib.md5(self.content.encode()).hexdigest()

@dataclass
class DocumentChunk:
    """Represents a chunk of a document."""
    id: str
    document_id: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None
    
    def __post_init__(self):
        """Generate an ID if not provided."""
        if not self.id:
            chunk_hash = hashlib.md5(self.content.encode()).hexdigest()
            self.id = f"{self.document_id}_{chunk_hash}"

@dataclass
class RetrievalResult:
    """Represents a retrieval result from the vector store."""
    chunk: DocumentChunk
    score: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for LLM context."""
        return {
            "content": self.chunk.content,
            "metadata": self.chunk.metadata,
            "score": self.score,
            "document_id": self.chunk.document_id,
            "chunk_id": self.chunk.id
        }

@dataclass
class EmbeddingModelConfig:
    """Configuration for embedding models."""
    model_name: str
    model_type: EmbeddingModelType
    dimension: int
    api_key: Optional[str] = None
    batch_size: int = 32
    additional_params: Dict[str, Any] = field(default_factory=dict)

@dataclass
class VectorStoreConfig:
    """Configuration for vector stores."""
    store_type: VectorStoreType
    collection_name: str
    dimension: int
    connection_params: Dict[str, Any] = field(default_factory=dict)
    similarity_metric: str = "cosine"  # cosine, dot_product, euclidean

@dataclass
class ChunkingConfig:
    """Configuration for document chunking."""
    strategy: ChunkingStrategy
    chunk_size: int = 512
    chunk_overlap: int = 128
    separator: Optional[str] = None
    additional_params: Dict[str, Any] = field(default_factory=dict)

class DocumentProcessor(ABC):
    """Abstract base class for document processors."""
    
    @abstractmethod
    def process(self, document: Document) -> Document:
        """
        Process a document.
        
        Args:
            document: The document to process.
            
        Returns:
            The processed document.
        """
        pass

class DocumentChunker(ABC):
    """Abstract base class for document chunkers."""
    
    @abstractmethod
    def chunk(self, document: Document) -> List[DocumentChunk]:
        """
        Chunk a document.
        
        Args:
            document: The document to chunk.
            
        Returns:
            A list of document chunks.
        """
        pass

class EmbeddingModel(ABC):
    """Abstract base class for embedding models."""
    
    @abstractmethod
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: The texts to generate embeddings for.
            
        Returns:
            A list of embeddings.
        """
        pass
    
    @abstractmethod
    def get_dimension(self) -> int:
        """
        Get the dimension of the embeddings.
        
        Returns:
            The dimension of the embeddings.
        """
        pass

class VectorStore(ABC):
    """Abstract base class for vector stores."""
    
    @abstractmethod
    def add(self, chunks: List[DocumentChunk]) -> bool:
        """
        Add document chunks to the vector store.
        
        Args:
            chunks: The document chunks to add.
            
        Returns:
            True if successful, False otherwise.
        """
        pass
    
    @abstractmethod
    def search(self, query_embedding: List[float], 
              top_k: int = 5, 
              filters: Optional[Dict[str, Any]] = None) -> List[RetrievalResult]:
        """
        Search for similar document chunks.
        
        Args:
            query_embedding: The query embedding.
            top_k: The number of results to return.
            filters: Optional filters to apply.
            
        Returns:
            A list of retrieval results.
        """
        pass
    
    @abstractmethod
    def delete(self, chunk_ids: List[str]) -> bool:
        """
        Delete document chunks from the vector store.
        
        Args:
            chunk_ids: The IDs of the chunks to delete.
            
        Returns:
            True if successful, False otherwise.
        """
        pass 