"""
Main Query Processor module that orchestrates the entire query processing pipeline.
"""

import logging
import time
from typing import Dict, List, Any, Optional
from .core import ParsedQuery, SubQuery, QueryResult, ProcessedResult, DataSource
from .parser import QueryParser
from .planner import QueryPlanner
from .executor import QueryExecutor
from .result_processor import ResultProcessor

logger = logging.getLogger(__name__)

class QueryProcessor:
    """
    Main query processor that orchestrates the entire query processing pipeline.
    """
    
    def __init__(self, data_sources: Dict[str, DataSource], 
                data_source_connectors: Dict[str, Any],
                nlp_model=None, formatter=None):
        """
        Initialize the query processor.
        
        Args:
            data_sources: A dictionary mapping data source IDs to DataSource objects.
            data_source_connectors: A dictionary mapping data source IDs to connector objects.
            nlp_model: An optional NLP model for advanced parsing.
            formatter: An optional formatter for customizing result presentation.
        """
        self.parser = QueryParser(nlp_model)
        self.planner = QueryPlanner(data_sources)
        self.executor = QueryExecutor(data_source_connectors)
        self.result_processor = ResultProcessor(formatter)
    
    def process_query(self, query: str, user_permissions: Dict[str, Any], 
                     timeout: int = 30) -> ProcessedResult:
        """
        Process a natural language query.
        
        Args:
            query: The natural language query string.
            user_permissions: The user's permissions.
            timeout: The maximum time to wait for query execution (in seconds).
            
        Returns:
            A ProcessedResult object containing the answer and supporting data.
        """
        logger.info(f"Processing query: {query}")
        
        start_time = time.time()
        
        try:
            # Parse the query
            parsed_query = self.parser.parse(query)
            
            # Create an execution plan
            sub_queries = self.planner.create_execution_plan(parsed_query, user_permissions)
            
            # Execute the sub-queries
            results = self.executor.execute(sub_queries, timeout)
            
            # Process the results
            processed_result = self.result_processor.process_results(results, query)
            
            # Add total execution time
            total_time = time.time() - start_time
            logger.info(f"Query processed in {total_time:.2f}s")
            
            return processed_result
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            
            # Return an error result
            return ProcessedResult(
                answer=f"I'm sorry, but I encountered an error while processing your query: {str(e)}",
                supporting_data={},
                confidence_score=0.0,
                execution_time=time.time() - start_time
            ) 