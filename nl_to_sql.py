import re
from database import execute_query
from schema_extractor import format_schema_for_prompt
from groq_client import GroqClient

class NLtoSQLConverter:
    def __init__(self, groq_client=None):
        """Initialize the NL to SQL converter"""
        self.groq_client = groq_client or GroqClient()
        self.schema_info = format_schema_for_prompt()
    
    def extract_sql_from_response(self, response):
        """Extract the SQL query from the LLM response"""
        # Try to extract SQL code block
        sql_match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL)
        if sql_match:
            return sql_match.group(1).strip()
        
        # Try to extract any code block
        code_match = re.search(r'```\s*(.*?)\s*```', response, re.DOTALL)
        if code_match:
            return code_match.group(1).strip()
        
        # If no code blocks, try to extract lines that look like SQL
        sql_keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 
                        'HAVING', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN',
                        'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']
        
        lines = response.split('\n')
        sql_lines = []
        is_sql = False
        
        for line in lines:
            line_upper = line.strip().upper()
            
            # Check if the line starts with an SQL keyword
            if any(line_upper.startswith(keyword) for keyword in sql_keywords):
                is_sql = True
                sql_lines.append(line)
            # Continue if we're in an SQL block
            elif is_sql and line.strip():
                sql_lines.append(line)
        
        if sql_lines:
            return '\n'.join(sql_lines)
        
        # If all else fails, return the original response
        return response
    
    def nl_to_sql(self, nl_query, execute=False):
        """
        Convert natural language query to SQL
        
        Args:
            nl_query (str): The natural language query
            execute (bool): Whether to execute the generated SQL query
            
        Returns:
            dict: A dictionary containing the natural language query, 
                  generated SQL, and optionally the execution results
        """
        # Get SQL from LLM
        raw_response = self.groq_client.get_nl_to_sql_completion(nl_query, self.schema_info)
        
        # Extract SQL query
        sql_query = self.extract_sql_from_response(raw_response)
        
        result = {
            "natural_language_query": nl_query,
            "generated_sql": sql_query
        }
        
        # Execute the query if requested
        if execute:
            try:
                execution_result = execute_query(sql_query)
                result["execution_success"] = True
                result["execution_result"] = execution_result
            except Exception as e:
                result["execution_success"] = False
                result["execution_error"] = str(e)
        
        return result
    
    def process_nl_to_sql_dataset(self, dataset, execute=False):
        """
        Process a dataset of natural language queries
        
        Args:
            dataset (list): List of dictionaries containing natural language queries
            execute (bool): Whether to execute the generated SQL queries
            
        Returns:
            list: A list of results, each containing the natural language query, 
                  generated SQL, and optionally the execution results
        """
        results = []
        
        for i, item in enumerate(dataset):
            nl_query = item.get("nl_query", "")
            print(f"Processing query {i+1}/{len(dataset)}: {nl_query[:50]}...")
            
            result = self.nl_to_sql(nl_query, execute)
            results.append(result)
        
        return results

if __name__ == "__main__":
    # Test the NL to SQL converter
    converter = NLtoSQLConverter()
    result = converter.nl_to_sql("Show me all customers with their orders", execute=True)
    print("NL to SQL converter test:")
    print(f"Natural Language: {result['natural_language_query']}")
    print(f"Generated SQL: {result['generated_sql']}")
    if "execution_result" in result:
        print(f"Execution Result: {result['execution_result']}")