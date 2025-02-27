"""
Query Parser module for processing natural language queries.
"""

import logging
from typing import Dict, List, Any, Set
import re
from .core import ParsedQuery, QueryIntent

logger = logging.getLogger(__name__)

class QueryParser:
    """
    Parses natural language queries to extract intent, entities, and filters.
    """
    
    def __init__(self, nlp_model=None):
        """
        Initialize the query parser.
        
        Args:
            nlp_model: An optional NLP model for advanced parsing.
                       If None, simple rule-based parsing is used.
        """
        self.nlp_model = nlp_model
        # Define keywords for intent recognition
        self.intent_keywords = {
            QueryIntent.RETRIEVAL: ["get", "find", "show", "retrieve", "what is", "who is"],
            QueryIntent.CALCULATION: ["calculate", "compute", "sum", "average", "count"],
            QueryIntent.COMPARISON: ["compare", "difference", "versus", "vs", "against"],
            QueryIntent.SUMMARY: ["summarize", "overview", "brief", "summary"]
        }
        
    def parse(self, query: str) -> ParsedQuery:
        """
        Parse a natural language query.
        
        Args:
            query: The natural language query string.
            
        Returns:
            A ParsedQuery object containing the extracted information.
        """
        logger.info(f"Parsing query: {query}")
        
        # Normalize query
        normalized_query = query.lower().strip()
        
        # Determine intent
        intent = self._determine_intent(normalized_query)
        
        # Extract entities
        entities = self._extract_entities(normalized_query)
        
        # Extract filters
        filters = self._extract_filters(normalized_query)
        
        # Determine required data sources
        required_data_sources = self._determine_data_sources(intent, entities, filters)
        
        parsed_query = ParsedQuery(
            original_query=query,
            intent=intent,
            entities=entities,
            filters=filters,
            required_data_sources=required_data_sources
        )
        
        logger.debug(f"Parsed query: {parsed_query}")
        return parsed_query
    
    def _determine_intent(self, query: str) -> QueryIntent:
        """
        Determine the intent of the query.
        
        Args:
            query: The normalized query string.
            
        Returns:
            The determined QueryIntent.
        """
        # If using an NLP model, use it for intent classification
        if self.nlp_model:
            return self._determine_intent_with_model(query)
        
        # Simple rule-based intent detection
        for intent, keywords in self.intent_keywords.items():
            for keyword in keywords:
                if keyword in query:
                    return intent
        
        return QueryIntent.UNKNOWN
    
    def _determine_intent_with_model(self, query: str) -> QueryIntent:
        """
        Use the NLP model to determine query intent.
        
        Args:
            query: The normalized query string.
            
        Returns:
            The determined QueryIntent.
        """
        # This is a placeholder for using an actual NLP model
        # In a real implementation, you would use the model to classify the intent
        # For now, we'll just return UNKNOWN
        logger.warning("NLP model-based intent detection not implemented")
        return QueryIntent.UNKNOWN
    
    def _extract_entities(self, query: str) -> List[str]:
        """
        Extract entities from the query.
        
        Args:
            query: The normalized query string.
            
        Returns:
            A list of extracted entities.
        """
        # This is a simplified implementation
        # In a real system, you would use NER (Named Entity Recognition)
        
        # Look for quoted strings as entities
        quoted_entities = re.findall(r'"([^"]*)"', query)
        
        # Look for capitalized words as potential entities
        # This is very simplistic and would need to be improved
        capitalized_entities = []
        for word in re.findall(r'\b[A-Z][a-z]*\b', query):
            if word not in capitalized_entities:
                capitalized_entities.append(word)
        
        return quoted_entities + capitalized_entities
    
    def _extract_filters(self, query: str) -> Dict[str, Any]:
        """
        Extract filters from the query.
        
        Args:
            query: The normalized query string.
            
        Returns:
            A dictionary of filters.
        """
        filters = {}
        
        # Look for common filter patterns
        # Example: "where X is Y" or "with X greater than Y"
        
        # Date filters
        date_matches = re.findall(r'(from|before|after|between) (\d{4}-\d{2}-\d{2})', query)
        if date_matches:
            filters['date'] = {}
            for match in date_matches:
                operator, date = match
                if operator == 'from' or operator == 'after':
                    filters['date']['min'] = date
                elif operator == 'before':
                    filters['date']['max'] = date
                elif operator == 'between':
                    # This is simplified; would need to extract both dates
                    filters['date']['min'] = date
        
        # Numeric filters
        numeric_matches = re.findall(r'(greater than|less than|equal to) (\d+)', query)
        if numeric_matches:
            filters['numeric'] = {}
            for match in numeric_matches:
                operator, value = match
                if operator == 'greater than':
                    filters['numeric']['min'] = int(value)
                elif operator == 'less than':
                    filters['numeric']['max'] = int(value)
                elif operator == 'equal to':
                    filters['numeric']['exact'] = int(value)
        
        return filters
    
    def _determine_data_sources(self, intent: QueryIntent, entities: List[str], 
                               filters: Dict[str, Any]) -> List[str]:
        """
        Determine which data sources are required based on the query.
        
        Args:
            intent: The query intent.
            entities: The extracted entities.
            filters: The extracted filters.
            
        Returns:
            A list of data source IDs.
        """
        # This is a placeholder implementation
        # In a real system, you would have a mapping of entities to data sources
        # or use a more sophisticated approach
        
        # For now, just return a default data source
        return ["default_data_source"] 