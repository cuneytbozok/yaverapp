"""
Vector stores for the RAG system.
"""

import logging
import os
import json
import numpy as np
from typing import List, Dict, Any, Optional, Union, Tuple
from .core import (
    DocumentChunk, RetrievalResult, VectorStore, 
    VectorStoreType, VectorStoreConfig
)

logger = logging.getLogger(__name__)

class InMemoryVectorStore(VectorStore):
    """
    A simple in-memory vector store implementation.
    """
    
    def __init__(self, config: VectorStoreConfig):
        """
        Initialize the in-memory vector store.
        
        Args:
            config: The vector store configuration.
        """
        self.collection_name = config.collection_name
        self.dimension = config.dimension
        self.similarity_metric = config.similarity_metric
        self.chunks = {}  # Dictionary mapping chunk IDs to DocumentChunk objects
        self.embeddings = {}  # Dictionary mapping chunk IDs to embeddings
        
        logger.info(f"Initialized in-memory vector store: {self.collection_name}")
    
    def add(self, chunks: List[DocumentChunk]) -> bool:
        """
        Add document chunks to the vector store.
        
        Args:
            chunks: The document chunks to add.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            for chunk in chunks:
                if chunk.embedding is None:
                    logger.warning(f"Chunk {chunk.id} has no embedding, skipping")
                    continue
                
                self.chunks[chunk.id] = chunk
                self.embeddings[chunk.id] = chunk.embedding
            
            return True
        except Exception as e:
            logger.error(f"Error adding chunks to in-memory vector store: {e}")
            return False
    
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
        if not self.chunks:
            logger.warning("Vector store is empty")
            return []
        
        try:
            # Convert query embedding to numpy array
            query_embedding_np = np.array(query_embedding)
            
            # Calculate similarity scores
            scores = {}
            for chunk_id, embedding in self.embeddings.items():
                chunk = self.chunks[chunk_id]
                
                # Apply filters if provided
                if filters and not self._apply_filters(chunk, filters):
                    continue
                
                # Calculate similarity based on the metric
                embedding_np = np.array(embedding)
                
                if self.similarity_metric == "cosine":
                    # Cosine similarity
                    norm_query = np.linalg.norm(query_embedding_np)
                    norm_embedding = np.linalg.norm(embedding_np)
                    
                    if norm_query == 0 or norm_embedding == 0:
                        score = 0
                    else:
                        score = np.dot(query_embedding_np, embedding_np) / (norm_query * norm_embedding)
                
                elif self.similarity_metric == "dot_product":
                    # Dot product
                    score = np.dot(query_embedding_np, embedding_np)
                
                elif self.similarity_metric == "euclidean":
                    # Euclidean distance (converted to similarity)
                    distance = np.linalg.norm(query_embedding_np - embedding_np)
                    score = 1 / (1 + distance)  # Convert distance to similarity
                
                else:
                    # Default to dot product
                    score = np.dot(query_embedding_np, embedding_np)
                
                scores[chunk_id] = score
            
            # Sort by score and get top_k
            sorted_chunk_ids = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)[:top_k]
            
            # Create retrieval results
            results = []
            for chunk_id in sorted_chunk_ids:
                results.append(RetrievalResult(
                    chunk=self.chunks[chunk_id],
                    score=float(scores[chunk_id])
                ))
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching in-memory vector store: {e}")
            return []
    
    def delete(self, chunk_ids: List[str]) -> bool:
        """
        Delete document chunks from the vector store.
        
        Args:
            chunk_ids: The IDs of the chunks to delete.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            for chunk_id in chunk_ids:
                if chunk_id in self.chunks:
                    del self.chunks[chunk_id]
                
                if chunk_id in self.embeddings:
                    del self.embeddings[chunk_id]
            
            return True
        except Exception as e:
            logger.error(f"Error deleting chunks from in-memory vector store: {e}")
            return False
    
    def _apply_filters(self, chunk: DocumentChunk, filters: Dict[str, Any]) -> bool:
        """
        Apply filters to a chunk.
        
        Args:
            chunk: The document chunk.
            filters: The filters to apply.
            
        Returns:
            True if the chunk passes the filters, False otherwise.
        """
        for key, value in filters.items():
            # Check if the filter key exists in metadata
            if key not in chunk.metadata:
                return False
            
            # Handle different filter types
            if isinstance(value, list):
                # List filter (any match)
                if chunk.metadata[key] not in value:
                    return False
            elif isinstance(value, dict):
                # Range filter
                if "min" in value and chunk.metadata[key] < value["min"]:
                    return False
                if "max" in value and chunk.metadata[key] > value["max"]:
                    return False
            else:
                # Exact match filter
                if chunk.metadata[key] != value:
                    return False
        
        return True

class FAISSVectorStore(VectorStore):
    """
    Vector store using FAISS for efficient similarity search.
    """
    
    def __init__(self, config: VectorStoreConfig):
        """
        Initialize the FAISS vector store.
        
        Args:
            config: The vector store configuration.
        """
        self.collection_name = config.collection_name
        self.dimension = config.dimension
        self.similarity_metric = config.similarity_metric
        self.index_path = config.connection_params.get("index_path")
        self.chunks = {}  # Dictionary mapping chunk IDs to DocumentChunk objects
        self.id_to_index = {}  # Dictionary mapping chunk IDs to FAISS indices
        self.index_to_id = {}  # Dictionary mapping FAISS indices to chunk IDs
        self.index = None
        
        try:
            import faiss
            
            # Create the FAISS index
            if self.similarity_metric == "cosine":
                # L2 normalize vectors and use inner product
                self.index = faiss.IndexFlatIP(self.dimension)
            elif self.similarity_metric == "dot_product":
                # Use inner product
                self.index = faiss.IndexFlatIP(self.dimension)
            else:
                # Default to L2 distance
                self.index = faiss.IndexFlatL2(self.dimension)
            
            # Load existing index if available
            if self.index_path and os.path.exists(self.index_path):
                self._load_index()
            
            logger.info(f"Initialized FAISS vector store: {self.collection_name}")
            
        except ImportError:
            logger.error("Failed to import FAISS. Please install it with: pip install faiss-cpu or faiss-gpu")
    
    def add(self, chunks: List[DocumentChunk]) -> bool:
        """
        Add document chunks to the vector store.
        
        Args:
            chunks: The document chunks to add.
            
        Returns:
            True if successful, False otherwise.
        """
        if self.index is None:
            logger.error("FAISS index is not initialized")
            return False
        
        try:
            import faiss
            import numpy as np
            
            # Collect embeddings and chunk IDs
            embeddings = []
            valid_chunks = []
            
            for chunk in chunks:
                if chunk.embedding is None:
                    logger.warning(f"Chunk {chunk.id} has no embedding, skipping")
                    continue
                
                embeddings.append(chunk.embedding)
                valid_chunks.append(chunk)
            
            if not embeddings:
                logger.warning("No valid embeddings to add")
                return True
            
            # Convert to numpy array
            embeddings_np = np.array(embeddings, dtype=np.float32)
            
            # Normalize if using cosine similarity
            if self.similarity_metric == "cosine":
                faiss.normalize_L2(embeddings_np)
            
            # Add to index
            start_idx = len(self.chunks)
            self.index.add(embeddings_np)
            
            # Update mappings
            for i, chunk in enumerate(valid_chunks):
                idx = start_idx + i
                self.chunks[chunk.id] = chunk
                self.id_to_index[chunk.id] = idx
                self.index_to_id[idx] = chunk.id
            
            # Save index if path is provided
            if self.index_path:
                self._save_index()
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding chunks to FAISS vector store: {e}")
            return False
    
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
        if self.index is None:
            logger.error("FAISS index is not initialized")
            return []
        
        if not self.chunks:
            logger.warning("Vector store is empty")
            return []
        
        try:
            import numpy as np
            
            # Convert query embedding to numpy array
            query_embedding_np = np.array([query_embedding], dtype=np.float32)
            
            # Normalize if using cosine similarity
            if self.similarity_metric == "cosine":
                import faiss
                faiss.normalize_L2(query_embedding_np)
            
            # Determine how many results to fetch (more if filtering)
            fetch_k = top_k
            if filters:
                # Fetch more results if filtering will be applied
                fetch_k = min(top_k * 10, len(self.chunks))
            
            # Search the index
            distances, indices = self.index.search(query_embedding_np, fetch_k)
            
            # Process results
            results = []
            
            for i, idx in enumerate(indices[0]):
                if idx == -1:  # FAISS returns -1 for padded results
                    continue
                
                chunk_id = self.index_to_id.get(int(idx))
                if not chunk_id:
                    continue
                
                chunk = self.chunks.get(chunk_id)
                if not chunk:
                    continue
                
                # Apply filters if provided
                if filters and not self._apply_filters(chunk, filters):
                    continue
                
                # Convert distance to similarity score
                if self.similarity_metric == "euclidean":
                    # Convert Euclidean distance to similarity
                    score = 1 / (1 + distances[0][i])
                else:
                    # For cosine and dot product, higher is better
                    score = float(distances[0][i])
                
                results.append(RetrievalResult(
                    chunk=chunk,
                    score=score
                ))
                
                # Stop if we have enough results
                if len(results) >= top_k:
                    break
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching FAISS vector store: {e}")
            return []
    
    def delete(self, chunk_ids: List[str]) -> bool:
        """
        Delete document chunks from the vector store.
        
        Args:
            chunk_ids: The IDs of the chunks to delete.
            
        Returns:
            True if successful, False otherwise.
        """
        if self.index is None:
            logger.error("FAISS index is not initialized")
            return False
        
        try:
            import faiss
            import numpy as np
            
            # FAISS doesn't support direct deletion, so we need to rebuild the index
            # First, collect all embeddings except the ones to delete
            embeddings = []
            valid_chunks = []
            
            for chunk_id, chunk in self.chunks.items():
                if chunk_id in chunk_ids:
                    continue
                
                if chunk.embedding is None:
                    continue
                
                embeddings.append(chunk.embedding)
                valid_chunks.append(chunk)
            
            if not embeddings:
                # No embeddings left, create empty index
                if self.similarity_metric == "cosine":
                    self.index = faiss.IndexFlatIP(self.dimension)
                elif self.similarity_metric == "dot_product":
                    self.index = faiss.IndexFlatIP(self.dimension)
                else:
                    self.index = faiss.IndexFlatL2(self.dimension)
                
                self.chunks = {}
                self.id_to_index = {}
                self.index_to_id = {}
                
                # Save empty index if path is provided
                if self.index_path:
                    self._save_index()
                
                return True
            
            # Convert to numpy array
            embeddings_np = np.array(embeddings, dtype=np.float32)
            
            # Normalize if using cosine similarity
            if self.similarity_metric == "cosine":
                faiss.normalize_L2(embeddings_np)
            
            # Create new index
            if self.similarity_metric == "cosine":
                new_index = faiss.IndexFlatIP(self.dimension)
            elif self.similarity_metric == "dot_product":
                new_index = faiss.IndexFlatIP(self.dimension)
            else:
                new_index = faiss.IndexFlatL2(self.dimension)
            
            # Add embeddings to new index
            new_index.add(embeddings_np)
            
            # Update index and mappings
            self.index = new_index
            self.chunks = {chunk.id: chunk for chunk in valid_chunks}
            self.id_to_index = {chunk.id: i for i, chunk in enumerate(valid_chunks)}
            self.index_to_id = {i: chunk.id for i, chunk in enumerate(valid_chunks)}
            
            # Save index if path is provided
            if self.index_path:
                self._save_index()
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting chunks from FAISS vector store: {e}")
            return False
    
    def _apply_filters(self, chunk: DocumentChunk, filters: Dict[str, Any]) -> bool:
        """
        Apply filters to a chunk.
        
        Args:
            chunk: The document chunk.
            filters: The filters to apply.
            
        Returns:
            True if the chunk passes the filters, False otherwise.
        """
        for key, value in filters.items():
            # Check if the filter key exists in metadata
            if key not in chunk.metadata:
                return False
            
            # Handle different filter types
            if isinstance(value, list):
                # List filter (any match)
                if chunk.metadata[key] not in value:
                    return False
            elif isinstance(value, dict):
                # Range filter
                if "min" in value and chunk.metadata[key] < value["min"]:
                    return False
                if "max" in value and chunk.metadata[key] > value["max"]:
                    return False
            else:
                # Exact match filter
                if chunk.metadata[key] != value:
                    return False
        
        return True
    
    def _save_index(self):
        """
        Save the FAISS index and metadata to disk.
        """
        try:
            import faiss
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
            
            # Save FAISS index
            faiss.write_index(self.index, self.index_path)
            
            # Save metadata
            metadata_path = f"{self.index_path}.metadata.json"
            metadata = {
                "chunks": {chunk_id: chunk.metadata for chunk_id, chunk in self.chunks.items()},
                "id_to_index": self.id_to_index,
                "index_to_id": {str(k): v for k, v in self.index_to_id.items()}  # Convert int keys to strings for JSON
            }
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)
            
            logger.info(f"Saved FAISS index to {self.index_path}")
            
        except Exception as e:
            logger.error(f"Error saving FAISS index: {e}")
    
    def _load_index(self):
        """
        Load the FAISS index and metadata from disk.
        """
        try:
            import faiss
            
            # Load FAISS index
            self.index = faiss.read_index(self.index_path)
            
            # Load metadata
            metadata_path = f"{self.index_path}.metadata.json"
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                # Reconstruct chunks
                for chunk_id, chunk_metadata in metadata.get("chunks", {}).items():
                    self.chunks[chunk_id] = DocumentChunk(
                        id=chunk_id,
                        document_id=chunk_metadata.get("document_id", ""),
                        content="",  # Content is not stored in metadata
                        metadata=chunk_metadata
                    )
                
                # Reconstruct mappings
                self.id_to_index = metadata.get("id_to_index", {})
                self.index_to_id = {int(k): v for k, v in metadata.get("index_to_id", {}).items()}  # Convert string keys back to ints
            
            logger.info(f"Loaded FAISS index from {self.index_path}")
            
        except Exception as e:
            logger.error(f"Error loading FAISS index: {e}")

def create_vector_store(config: VectorStoreConfig) -> VectorStore:
    """
    Create a vector store based on the configuration.
    
    Args:
        config: The vector store configuration.
        
    Returns:
        A vector store.
    """
    if config.store_type == VectorStoreType.IN_MEMORY:
        return InMemoryVectorStore(config)
    elif config.store_type == VectorStoreType.FAISS:
        return FAISSVectorStore(config)
    else:
        raise ValueError(f"Unsupported vector store type: {config.store_type}") 