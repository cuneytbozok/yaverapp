"""
Document processing module for the RAG system.
"""

import logging
import re
import nltk
from typing import List, Dict, Any, Optional, Callable
from .core import (
    Document, DocumentChunk, DocumentProcessor, DocumentChunker,
    ChunkingStrategy, ChunkingConfig
)

logger = logging.getLogger(__name__)

# Try to download NLTK resources if needed
try:
    nltk.download('punkt', quiet=True)
except Exception as e:
    logger.warning(f"Failed to download NLTK resources: {e}")

class TextNormalizer(DocumentProcessor):
    """
    Normalizes text by removing extra whitespace, fixing encoding issues, etc.
    """
    
    def __init__(self, lowercase: bool = False, remove_urls: bool = True,
                remove_html: bool = True, remove_special_chars: bool = False):
        """
        Initialize the text normalizer.
        
        Args:
            lowercase: Whether to convert text to lowercase.
            remove_urls: Whether to remove URLs.
            remove_html: Whether to remove HTML tags.
            remove_special_chars: Whether to remove special characters.
        """
        self.lowercase = lowercase
        self.remove_urls = remove_urls
        self.remove_html = remove_html
        self.remove_special_chars = remove_special_chars
    
    def process(self, document: Document) -> Document:
        """
        Normalize the document text.
        
        Args:
            document: The document to normalize.
            
        Returns:
            The normalized document.
        """
        content = document.content
        
        # Remove URLs
        if self.remove_urls:
            content = re.sub(r'https?://\S+|www\.\S+', ' ', content)
        
        # Remove HTML tags
        if self.remove_html:
            content = re.sub(r'<.*?>', ' ', content)
        
        # Remove special characters
        if self.remove_special_chars:
            content = re.sub(r'[^\w\s]', ' ', content)
        
        # Normalize whitespace
        content = re.sub(r'\s+', ' ', content).strip()
        
        # Convert to lowercase
        if self.lowercase:
            content = content.lower()
        
        # Create a new document with normalized content
        return Document(
            id=document.id,
            content=content,
            metadata=document.metadata,
            source=document.source
        )

class MetadataExtractor(DocumentProcessor):
    """
    Extracts metadata from documents.
    """
    
    def __init__(self, extractors: Dict[str, Callable[[str], Any]]):
        """
        Initialize the metadata extractor.
        
        Args:
            extractors: A dictionary mapping metadata keys to extractor functions.
        """
        self.extractors = extractors
    
    def process(self, document: Document) -> Document:
        """
        Extract metadata from the document.
        
        Args:
            document: The document to extract metadata from.
            
        Returns:
            The document with extracted metadata.
        """
        metadata = document.metadata.copy()
        
        for key, extractor in self.extractors.items():
            try:
                metadata[key] = extractor(document.content)
            except Exception as e:
                logger.warning(f"Failed to extract metadata '{key}': {e}")
        
        return Document(
            id=document.id,
            content=document.content,
            metadata=metadata,
            source=document.source
        )

class FixedSizeChunker(DocumentChunker):
    """
    Chunks documents into fixed-size chunks.
    """
    
    def __init__(self, config: ChunkingConfig):
        """
        Initialize the fixed-size chunker.
        
        Args:
            config: The chunking configuration.
        """
        self.chunk_size = config.chunk_size
        self.chunk_overlap = config.chunk_overlap
    
    def chunk(self, document: Document) -> List[DocumentChunk]:
        """
        Chunk a document into fixed-size chunks.
        
        Args:
            document: The document to chunk.
            
        Returns:
            A list of document chunks.
        """
        content = document.content
        chunks = []
        
        # If the content is shorter than the chunk size, return it as a single chunk
        if len(content) <= self.chunk_size:
            return [DocumentChunk(
                id="",
                document_id=document.id,
                content=content,
                metadata=document.metadata.copy()
            )]
        
        # Split the content into chunks
        start = 0
        chunk_index = 0
        
        while start < len(content):
            end = min(start + self.chunk_size, len(content))
            
            # If we're not at the end of the content, try to find a good break point
            if end < len(content):
                # Try to find a period, question mark, or exclamation point followed by a space
                match = re.search(r'[.!?]\s', content[end-20:end])
                if match:
                    end = end - 20 + match.end()
            
            chunk_content = content[start:end]
            
            # Create a chunk
            chunk = DocumentChunk(
                id="",
                document_id=document.id,
                content=chunk_content,
                metadata={
                    **document.metadata,
                    "chunk_index": chunk_index,
                    "source": document.source
                }
            )
            
            chunks.append(chunk)
            
            # Move to the next chunk, accounting for overlap
            start = end - self.chunk_overlap
            chunk_index += 1
        
        return chunks

class SentenceChunker(DocumentChunker):
    """
    Chunks documents by sentences, grouping them to approach the target chunk size.
    """
    
    def __init__(self, config: ChunkingConfig):
        """
        Initialize the sentence chunker.
        
        Args:
            config: The chunking configuration.
        """
        self.target_size = config.chunk_size
        self.max_sentences = config.additional_params.get("max_sentences", 15)
    
    def chunk(self, document: Document) -> List[DocumentChunk]:
        """
        Chunk a document by sentences.
        
        Args:
            document: The document to chunk.
            
        Returns:
            A list of document chunks.
        """
        content = document.content
        
        try:
            # Split the content into sentences
            sentences = nltk.sent_tokenize(content)
        except Exception as e:
            logger.warning(f"Failed to tokenize sentences: {e}. Falling back to simple splitting.")
            sentences = re.split(r'(?<=[.!?])\s+', content)
        
        chunks = []
        current_chunk = []
        current_size = 0
        chunk_index = 0
        
        for sentence in sentences:
            sentence_size = len(sentence)
            
            # If adding this sentence would exceed the target size and we already have sentences,
            # or if we've reached the maximum number of sentences per chunk,
            # create a new chunk
            if ((current_size + sentence_size > self.target_size and current_chunk) or
                len(current_chunk) >= self.max_sentences):
                
                chunk_content = " ".join(current_chunk)
                chunk = DocumentChunk(
                    id="",
                    document_id=document.id,
                    content=chunk_content,
                    metadata={
                        **document.metadata,
                        "chunk_index": chunk_index,
                        "source": document.source
                    }
                )
                
                chunks.append(chunk)
                
                current_chunk = []
                current_size = 0
                chunk_index += 1
            
            current_chunk.append(sentence)
            current_size += sentence_size
        
        # Add the last chunk if there's anything left
        if current_chunk:
            chunk_content = " ".join(current_chunk)
            chunk = DocumentChunk(
                id="",
                document_id=document.id,
                content=chunk_content,
                metadata={
                    **document.metadata,
                    "chunk_index": chunk_index,
                    "source": document.source
                }
            )
            
            chunks.append(chunk)
        
        return chunks

class ParagraphChunker(DocumentChunker):
    """
    Chunks documents by paragraphs, with options to split large paragraphs.
    """
    
    def __init__(self, config: ChunkingConfig):
        """
        Initialize the paragraph chunker.
        
        Args:
            config: The chunking configuration.
        """
        self.max_paragraph_size = config.chunk_size
        self.split_large_paragraphs = config.additional_params.get("split_large_paragraphs", True)
        self.paragraph_separator = config.separator or "\n\n"
    
    def chunk(self, document: Document) -> List[DocumentChunk]:
        """
        Chunk a document by paragraphs.
        
        Args:
            document: The document to chunk.
            
        Returns:
            A list of document chunks.
        """
        content = document.content
        
        # Split the content into paragraphs
        paragraphs = content.split(self.paragraph_separator)
        paragraphs = [p.strip() for p in paragraphs if p.strip()]
        
        chunks = []
        chunk_index = 0
        
        for paragraph in paragraphs:
            # If the paragraph is too large and splitting is enabled, split it
            if len(paragraph) > self.max_paragraph_size and self.split_large_paragraphs:
                # Use the fixed-size chunker to split the paragraph
                fixed_chunker = FixedSizeChunker(ChunkingConfig(
                    strategy=ChunkingStrategy.FIXED_SIZE,
                    chunk_size=self.max_paragraph_size,
                    chunk_overlap=min(100, self.max_paragraph_size // 4)
                ))
                
                paragraph_doc = Document(
                    id=f"{document.id}_p{chunk_index}",
                    content=paragraph,
                    metadata=document.metadata,
                    source=document.source
                )
                
                paragraph_chunks = fixed_chunker.chunk(paragraph_doc)
                
                for i, p_chunk in enumerate(paragraph_chunks):
                    p_chunk.metadata["chunk_index"] = chunk_index + i
                
                chunks.extend(paragraph_chunks)
                chunk_index += len(paragraph_chunks)
            else:
                # Add the paragraph as a single chunk
                chunk = DocumentChunk(
                    id="",
                    document_id=document.id,
                    content=paragraph,
                    metadata={
                        **document.metadata,
                        "chunk_index": chunk_index,
                        "source": document.source
                    }
                )
                
                chunks.append(chunk)
                chunk_index += 1
        
        return chunks

class HybridChunker(DocumentChunker):
    """
    A hybrid chunking strategy that combines multiple approaches.
    """
    
    def __init__(self, config: ChunkingConfig):
        """
        Initialize the hybrid chunker.
        
        Args:
            config: The chunking configuration.
        """
        self.paragraph_chunker = ParagraphChunker(config)
        self.sentence_chunker = SentenceChunker(config)
        self.max_chunk_size = config.chunk_size
    
    def chunk(self, document: Document) -> List[DocumentChunk]:
        """
        Chunk a document using a hybrid approach.
        
        Args:
            document: The document to chunk.
            
        Returns:
            A list of document chunks.
        """
        # First, split by paragraphs
        paragraph_chunks = self.paragraph_chunker.chunk(document)
        
        # Then, for any paragraphs that are too large, split by sentences
        final_chunks = []
        
        for chunk in paragraph_chunks:
            if len(chunk.content) > self.max_chunk_size:
                # Create a temporary document for the paragraph
                paragraph_doc = Document(
                    id=chunk.document_id,
                    content=chunk.content,
                    metadata=chunk.metadata,
                    source=document.source
                )
                
                # Split the paragraph by sentences
                sentence_chunks = self.sentence_chunker.chunk(paragraph_doc)
                final_chunks.extend(sentence_chunks)
            else:
                final_chunks.append(chunk)
        
        # Update chunk indices
        for i, chunk in enumerate(final_chunks):
            chunk.metadata["chunk_index"] = i
        
        return final_chunks

def create_chunker(config: ChunkingConfig) -> DocumentChunker:
    """
    Create a document chunker based on the configuration.
    
    Args:
        config: The chunking configuration.
        
    Returns:
        A document chunker.
    """
    if config.strategy == ChunkingStrategy.FIXED_SIZE:
        return FixedSizeChunker(config)
    elif config.strategy == ChunkingStrategy.SENTENCE:
        return SentenceChunker(config)
    elif config.strategy == ChunkingStrategy.PARAGRAPH:
        return ParagraphChunker(config)
    elif config.strategy == ChunkingStrategy.HYBRID:
        return HybridChunker(config)
    else:
        raise ValueError(f"Unsupported chunking strategy: {config.strategy}")

class DocumentProcessingPipeline:
    """
    A pipeline for processing documents.
    """
    
    def __init__(self, processors: List[DocumentProcessor], chunker: DocumentChunker):
        """
        Initialize the document processing pipeline.
        
        Args:
            processors: A list of document processors.
            chunker: A document chunker.
        """
        self.processors = processors
        self.chunker = chunker
    
    def process(self, document: Document) -> Document:
        """
        Process a document through all processors.
        
        Args:
            document: The document to process.
            
        Returns:
            The processed document.
        """
        processed_doc = document
        
        for processor in self.processors:
            processed_doc = processor.process(processed_doc)
        
        return processed_doc
    
    def process_and_chunk(self, document: Document) -> List[DocumentChunk]:
        """
        Process a document and chunk it.
        
        Args:
            document: The document to process and chunk.
            
        Returns:
            A list of document chunks.
        """
        processed_doc = self.process(document)
        chunks = self.chunker.chunk(processed_doc)
        
        return chunks 