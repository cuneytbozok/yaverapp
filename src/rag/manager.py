"""
RAG Manager module for coordinating the RAG system.
"""

import logging
import time
from typing import List, Dict, Any, Optional, Union, Tuple
from .core import (
    Document, DocumentChunk, RetrievalResult,
    EmbeddingModelConfig, VectorStoreConfig, ChunkingConfig,
    EmbeddingModelType, VectorStoreType, ChunkingStrategy
)
from .document_processing import (
    DocumentProcessingPipeline, TextNormalizer, 
    create_chunker
)
from .embedding_models import create_embedding_model
from .vector_stores import create_vector_store

logger = logging.getLogger(__name__)

class RAGManager:
    """
    Manager for the RAG system.
    """
    
    def __init__(self, 
                embedding_config: EmbeddingModelConfig,
                vector_store_config: VectorStoreConfig,
                chunking_config: ChunkingConfig):
        """
        Initialize the RAG manager.
        
        Args:
            embedding_config: Configuration for the embedding model.
            vector_store_config: Configuration for the vector store.
            chunking_config: Configuration for document chunking.
        """
        # Create components
        self.embedding_model = create_embedding_model(embedding_config)
        self.vector_store = create_vector_store(vector_store_config)
        self.chunker = create_chunker(chunking_config)
        
        # Create document processing pipeline
        self.pipeline = DocumentProcessingPipeline(
            processors=[
                TextNormalizer()
            ],
            chunker=self.chunker
        )
        
        logger.info("Initialized RAG manager")
    
    def add_document(self, document: Document) -> bool:
        """
        Process and add a document to the RAG system.
        
        Args:
            document: The document to add.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            # Process and chunk the document
            chunks = self.pipeline.process_and_chunk(document)
            
            if not chunks:
                logger.warning(f"No chunks generated for document {document.id}")
                return False
            
            # Generate embeddings for chunks
            chunk_texts = [chunk.content for chunk in chunks]
            embeddings = self.embedding_model.generate_embeddings(chunk_texts)
            
            # Assign embeddings to chunks
            for i, chunk in enumerate(chunks):
                chunk.embedding = embeddings[i]
            
            # Add chunks to vector store
            success = self.vector_store.add(chunks)
            
            if success:
                logger.info(f"Added document {document.id} with {len(chunks)} chunks")
            else:
                logger.error(f"Failed to add document {document.id} to vector store")
            
            return success
            
        except Exception as e:
            logger.error(f"Error adding document {document.id}: {e}")
            return False
    
    def add_documents(self, documents: List[Document]) -> Tuple[int, int]:
        """
        Process and add multiple documents to the RAG system.
        
        Args:
            documents: The documents to add.
            
        Returns:
            A tuple of (success_count, total_count).
        """
        success_count = 0
        total_count = len(documents)
        
        for document in documents:
            if self.add_document(document):
                success_count += 1
        
        logger.info(f"Added {success_count}/{total_count} documents")
        
        return success_count, total_count
    
    def search(self, query: str, 
              top_k: int = 5, 
              filters: Optional[Dict[str, Any]] = None) -> List[RetrievalResult]:
        """
        Search for relevant document chunks.
        
        Args:
            query: The search query.
            top_k: The number of results to return.
            filters: Optional filters to apply.
            
        Returns:
            A list of retrieval results.
        """
        try:
            # Generate embedding for the query
            query_embedding = self.embedding_model.generate_embeddings([query])[0]
            
            # Search the vector store
            results = self.vector_store.search(
                query_embedding=query_embedding,
                top_k=top_k,
                filters=filters
            )
            
            logger.info(f"Found {len(results)} results for query: {query}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching for query '{query}': {e}")
            return []
    
    def delete_document(self, document_id: str) -> bool:
        """
        Delete a document from the RAG system.
        
        Args:
            document_id: The ID of the document to delete.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            # In a real implementation, we would need to track which chunks belong to which document
            # For now, we'll assume the chunk IDs start with the document ID
            chunk_ids = [chunk_id for chunk_id in self.vector_store.chunks.keys() 
                        if chunk_id.startswith(f"{document_id}_")]
            
            if not chunk_ids:
                logger.warning(f"No chunks found for document {document_id}")
                return False
            
            success = self.vector_store.delete(chunk_ids)
            
            if success:
                logger.info(f"Deleted document {document_id} with {len(chunk_ids)} chunks")
            else:
                logger.error(f"Failed to delete document {document_id} from vector store")
            
            return success
            
        except Exception as e:
            logger.error(f"Error deleting document {document_id}: {e}")
            return False
    
    def get_context_for_query(self, query: str, 
                             top_k: int = 5, 
                             filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get context for a query to be used with an LLM.
        
        Args:
            query: The query.
            top_k: The number of results to include in the context.
            filters: Optional filters to apply.
            
        Returns:
            A dictionary containing the context.
        """
        # Search for relevant chunks
        results = self.search(query, top_k, filters)
        
        # Format the context
        context = {
            "query": query,
            "results": [result.to_dict() for result in results],
            "timestamp": time.time()
        }
        
        return context
    
    def get_formatted_context(self, query: str, 
                             top_k: int = 5, 
                             filters: Optional[Dict[str, Any]] = None) -> str:
        """
        Get formatted context string for a query to be used with an LLM.
        
        Args:
            query: The query.
            top_k: The number of results to include in the context.
            filters: Optional filters to apply.
            
        Returns:
            A formatted context string.
        """
        # Search for relevant chunks
        results = self.search(query, top_k, filters)
        
        # Format the context
        context_parts = []
        
        for i, result in enumerate(results):
            context_parts.append(f"[{i+1}] Source: {result.chunk.metadata.get('source', 'Unknown')}")
            if "title" in result.chunk.metadata:
                context_parts.append(f"Title: {result.chunk.metadata['title']}")
            context_parts.append(f"Content: {result.chunk.content}")
            context_parts.append("")  # Empty line between chunks
        
        if not context_parts:
            return "No relevant information found."
        
        return "\n".join(context_parts) 