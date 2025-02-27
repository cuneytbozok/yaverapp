"""
Result Processor module for assembling and formatting query results.
"""

import logging
import time
from typing import Dict, List, Any
from .core import QueryResult, ProcessedResult

logger = logging.getLogger(__name__)

class ResultProcessor:
    """
    Processes and formats query results for presentation to the user.
    """
    
    def __init__(self, formatter=None):
        """
        Initialize the result processor.
        
        Args:
            formatter: An optional formatter for customizing result presentation.
        """
        self.formatter = formatter
    
    def process_results(self, results: List[QueryResult], 
                       original_query: str) -> ProcessedResult:
        """
        Process query results into a final response.
        
        Args:
            results: The list of query results to process.
            original_query: The original natural language query.
            
        Returns:
            A ProcessedResult object.
        """
        logger.info(f"Processing {len(results)} query results")
        
        start_time = time.time()
        
        # Check for errors
        errors = [result for result in results if result.status == "error"]
        if errors:
            error_messages = [f"{result.data_source_id}: {result.error}" for result in errors]
            logger.warning(f"Errors in query results: {', '.join(error_messages)}")
        
        # Combine successful results
        successful_results = [result for result in results if result.status == "success"]
        
        # Extract data from successful results
        combined_data = self._combine_results(successful_results)
        
        # Generate answer based on combined data
        answer = self._generate_answer(combined_data, original_query)
        
        # Calculate confidence score
        confidence_score = self._calculate_confidence(successful_results, errors)
        
        execution_time = time.time() - start_time
        
        # Format the result if a formatter is provided
        if self.formatter:
            answer = self.formatter.format(answer, combined_data)
        
        return ProcessedResult(
            answer=answer,
            supporting_data=combined_data,
            confidence_score=confidence_score,
            execution_time=execution_time
        )
    
    def _combine_results(self, results: List[QueryResult]) -> Dict[str, Any]:
        """
        Combine data from multiple query results.
        
        Args:
            results: The list of successful query results.
            
        Returns:
            A dictionary containing the combined data.
        """
        combined_data = {}
        
        for result in results:
            combined_data[result.data_source_id] = result.data
        
        return combined_data
    
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
        # This is a placeholder implementation
        # In a real system, you would use an LLM or template-based approach
        
        if not combined_data:
            return "I couldn't find any information to answer your question."
        
        # Simple answer generation based on data
        answer_parts = []
        
        for source_id, data in combined_data.items():
            if isinstance(data, dict):
                answer_parts.append(f"From {source_id}, I found: {data}")
            elif isinstance(data, list):
                answer_parts.append(f"From {source_id}, I found {len(data)} results.")
            else:
                answer_parts.append(f"From {source_id}, I found: {data}")
        
        return "\n".join(answer_parts)
    
    def _calculate_confidence(self, successful_results: List[QueryResult], 
                            errors: List[QueryResult]) -> float:
        """
        Calculate a confidence score for the results.
        
        Args:
            successful_results: The list of successful query results.
            errors: The list of error query results.
            
        Returns:
            A float between 0 and 1 representing the confidence.
        """
        # This is a simplified implementation
        # In a real system, you would use more sophisticated confidence calculation
        
        total_queries = len(successful_results) + len(errors)
        
        if total_queries == 0:
            return 0.0
        
        # Base confidence on the ratio of successful queries
        base_confidence = len(successful_results) / total_queries
        
        # Adjust confidence based on the amount of data returned
        data_confidence = 0.0
        for result in successful_results:
            if isinstance(result.data, dict) and result.data:
                data_confidence += 0.1
            elif isinstance(result.data, list) and result.data:
                data_confidence += min(0.1, len(result.data) / 100)
            elif result.data:
                data_confidence += 0.05
        
        # Cap data confidence at 0.5
        data_confidence = min(0.5, data_confidence)
        
        # Combine base confidence and data confidence
        confidence = base_confidence * 0.5 + data_confidence
        
        # Cap at 1.0
        return min(1.0, confidence) 