"""
Integration module for connecting the RAG system with the query processor.
"""

import logging
from typing import Dict, List, Any, Optional
from ..query_processor.core import ProcessedResult
from ..query_processor.result_processor import ResultProcessor
from ..llm_integration.core import LLMResponse
from ..llm_integration.manager import LLMManager
from .manager import RAGManager

logger = logging.getLogger(__name__)

class RAGResultProcessor(ResultProcessor):
    """
    Result processor that uses RAG to enhance query results.
    """
    
    def __init__(self, rag_manager: RAGManager, llm_manager: LLMManager, formatter=None):
        """
        Initialize the RAG result processor.
        
        Args:
            rag_manager: The RAG manager to use for retrieving context.
            llm_manager: The LLM manager to use for generating answers.
            formatter: An optional formatter for customizing result presentation.
        """
        super().__init__(formatter)
        self.rag_manager = rag_manager
        self.llm_manager = llm_manager
        
        # Register the RAG answer template if not already registered
        self._register_rag_template()
    
    def _register_rag_template(self):
        """Register the RAG answer template with the LLM manager."""
        from ..llm_integration.core import PromptTemplate
        
        rag_template = """
        You are an AI assistant helping to answer questions based on retrieved information.
        
        User question: {query}
        
        Retrieved context from documents:
        {context}
        
        Retrieved structured data:
        {structured_data}
        
        Please provide a clear, concise answer to the user's question based on the retrieved information.
        If the information doesn't contain enough details to answer the question, please state that clearly.
        """
        
        try:
            self.llm_manager.add_prompt_template(
                "rag_answer",
                PromptTemplate(rag_template, ["query", "context", "structured_data"])
            )
        except Exception as e:
            logger.warning(f"Failed to register RAG template: {e}")
    
    def _generate_answer(self, combined_data: Dict[str, Any], 
                        original_query: str) -> str:
        """
        Generate a natural language answer based on the combined data.
        
        Args:
            combined_data: The combined data from all successful queries.
            original_query: The original natural language query.
            
        Returns:
            A string containing the answer.
        """
        # Get context from RAG system
        context = self.rag_manager.get_formatted_context(original_query, top_k=5)
        
        # Format the structured data
        structured_data = self._format_structured_data(combined_data)
        
        try:
            # Generate answer using LLM
            response = self.llm_manager.generate_with_template(
                template_name="rag_answer",
                variables={
                    "query": original_query,
                    "context": context,
                    "structured_data": structured_data
                }
            )
            
            return response.text
        except Exception as e:
            logger.error(f"Error generating answer with LLM: {e}")
            
            # Fallback to basic answer generation
            return super()._generate_answer(combined_data, original_query)
    
    def _format_structured_data(self, data: Dict[str, Any]) -> str:
        """
        Format structured data for inclusion in a prompt.
        
        Args:
            data: The structured data.
            
        Returns:
            A formatted string representation of the data.
        """
        if not data:
            return "No structured data available."
        
        formatted_parts = []
        
        for source_id, source_data in data.items():
            formatted_parts.append(f"Source: {source_id}")
            
            if isinstance(source_data, dict):
                formatted_parts.append(self._format_dict(source_data))
            elif isinstance(source_data, list):
                formatted_parts.append(self._format_list(source_data))
            else:
                formatted_parts.append(str(source_data))
            
            formatted_parts.append("")  # Empty line between sources
        
        return "\n".join(formatted_parts)
    
    def _format_dict(self, data: Dict[str, Any], indent: int = 1) -> str:
        """
        Format a dictionary for inclusion in a prompt.
        
        Args:
            data: The dictionary to format.
            indent: The indentation level.
            
        Returns:
            A formatted string representation of the dictionary.
        """
        if not data:
            return "{}"
        
        formatted_parts = []
        indent_str = "  " * indent
        
        for key, value in data.items():
            if isinstance(value, dict):
                formatted_parts.append(f"{indent_str}{key}:")
                formatted_parts.append(self._format_dict(value, indent + 1))
            elif isinstance(value, list):
                formatted_parts.append(f"{indent_str}{key}:")
                formatted_parts.append(self._format_list(value, indent + 1))
            else:
                formatted_parts.append(f"{indent_str}{key}: {value}")
        
        return "\n".join(formatted_parts)
    
    def _format_list(self, data: List[Any], indent: int = 1) -> str:
        """
        Format a list for inclusion in a prompt.
        
        Args:
            data: The list to format.
            indent: The indentation level.
            
        Returns:
            A formatted string representation of the list.
        """
        if not data:
            return "[]"
        
        formatted_parts = []
        indent_str = "  " * indent
        
        for i, item in enumerate(data):
            if isinstance(item, dict):
                formatted_parts.append(f"{indent_str}{i+1}.")
                formatted_parts.append(self._format_dict(item, indent + 1))
            elif isinstance(item, list):
                formatted_parts.append(f"{indent_str}{i+1}.")
                formatted_parts.append(self._format_list(item, indent + 1))
            else:
                formatted_parts.append(f"{indent_str}{i+1}. {item}")
        
        return "\n".join(formatted_parts) 