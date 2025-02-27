"""
Integration module for connecting the LLM system with the query processor.
"""

import logging
import time
from typing import Dict, List, Any, Optional
from ..query_processor.core import ProcessedResult
from ..query_processor.result_processor import ResultProcessor
from .core import LLMResponse, PromptTemplate
from .manager import LLMManager

logger = logging.getLogger(__name__)

class LLMResultProcessor(ResultProcessor):
    """
    Result processor that uses LLMs to generate answers from query results.
    """
    
    def __init__(self, llm_manager: LLMManager, formatter=None):
        """
        Initialize the LLM result processor.
        
        Args:
            llm_manager: The LLM manager to use for generating answers.
            formatter: An optional formatter for customizing result presentation.
        """
        super().__init__(formatter)
        self.llm_manager = llm_manager
        
        # Register default prompt templates
        self._register_default_templates()
    
    def _register_default_templates(self):
        """
        Register default prompt templates for different query scenarios.
        """
        # Basic answer template
        basic_template = """
        You are an AI assistant helping to answer questions based on retrieved data.
        
        User question: {query}
        
        Retrieved data:
        {data}
        
        Please provide a clear, concise answer to the user's question based on the retrieved data.
        If the data doesn't contain enough information to answer the question, please state that clearly.
        """
        
        self.llm_manager.add_prompt_template(
            "basic_answer",
            PromptTemplate(basic_template, ["query", "data"])
        )
        
        # Comparison template
        comparison_template = """
        You are an AI assistant helping to compare different pieces of information.
        
        User question: {query}
        
        Data to compare:
        {data}
        
        Please provide a detailed comparison based on the user's question.
        Highlight key similarities and differences in a structured way.
        """
        
        self.llm_manager.add_prompt_template(
            "comparison",
            PromptTemplate(comparison_template, ["query", "data"])
        )
        
        # Summary template
        summary_template = """
        You are an AI assistant helping to summarize information.
        
        User question: {query}
        
        Data to summarize:
        {data}
        
        Please provide a concise summary of the data that addresses the user's question.
        Focus on the most important points and keep the summary clear and structured.
        """
        
        self.llm_manager.add_prompt_template(
            "summary",
            PromptTemplate(summary_template, ["query", "data"])
        )
    
    def _generate_answer(self, combined_data: Dict[str, Any], 
                        original_query: str) -> str:
        """
        Generate a natural language answer based on the combined data using an LLM.
        
        Args:
            combined_data: The combined data from all successful queries.
            original_query: The original natural language query.
            
        Returns:
            A string containing the answer.
        """
        if not combined_data:
            return "I couldn't find any information to answer your question."
        
        try:
            # Format the data for the prompt
            formatted_data = self._format_data_for_prompt(combined_data)
            
            # Determine which template to use based on the query and data
            template_name = self._determine_template(original_query, combined_data)
            
            # Generate the answer using the LLM
            response = self.llm_manager.generate_from_template(
                template_name=template_name,
                variables={
                    "query": original_query,
                    "data": formatted_data
                }
            )
            
            return response.text
            
        except Exception as e:
            logger.error(f"Error generating answer with LLM: {e}")
            
            # Fall back to basic answer generation
            return super()._generate_answer(combined_data, original_query)
    
    def _format_data_for_prompt(self, combined_data: Dict[str, Any]) -> str:
        """
        Format the combined data for inclusion in a prompt.
        
        Args:
            combined_data: The combined data from all successful queries.
            
        Returns:
            A formatted string representation of the data.
        """
        formatted_parts = []
        
        for source_id, data in combined_data.items():
            formatted_parts.append(f"Source: {source_id}")
            
            if isinstance(data, dict):
                formatted_parts.append(self._format_dict(data))
            elif isinstance(data, list):
                formatted_parts.append(self._format_list(data))
            else:
                formatted_parts.append(str(data))
            
            formatted_parts.append("")  # Add a blank line between sources
        
        return "\n".join(formatted_parts)
    
    def _format_dict(self, data: Dict[str, Any], indent: int = 0) -> str:
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
    
    def _format_list(self, data: List[Any], indent: int = 0) -> str:
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
    
    def _determine_template(self, query: str, data: Dict[str, Any]) -> str:
        """
        Determine which template to use based on the query and data.
        
        Args:
            query: The original query.
            data: The combined data.
            
        Returns:
            The name of the template to use.
        """
        # Simple heuristic for template selection
        query_lower = query.lower()
        
        if "compare" in query_lower or "difference" in query_lower or "versus" in query_lower or "vs" in query_lower:
            return "comparison"
        elif "summarize" in query_lower or "summary" in query_lower or "overview" in query_lower:
            return "summary"
        else:
            return "basic_answer" 