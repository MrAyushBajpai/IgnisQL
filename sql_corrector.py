import re
from database import execute_query
from schema_extractor import format_schema_for_prompt
from groq_client import GroqClient

class SQLCorrector:
    def __init__(self, groq_client=None):
        """Initialize the SQL corrector"""
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
    
    def get_error_message(self, sql_query):
        """Execute the SQL query and get the error message if it fails"""
        try:
            execute_query(sql_query)
            return None  # No error
        except Exception as e:
            return str(e)
    
    def correct_sql(self, incorrect_sql, execute=False):
        """
        Correct a SQL query
        
        Args:
            incorrect_sql (str): The incorrect SQL query
            execute (bool): Whether to execute the corrected SQL query
            
        Returns:
            dict: A dictionary containing the incorrect SQL, 
                  error message, corrected SQL, and optionally the execution results
        """
        # Get error message
        error_message = self.get_error_message(incorrect_sql)
        
        # Get corrected SQL from LLM
        raw_response = self.groq_client.get_sql_correction_completion(
            incorrect_sql, self.schema_info, error_message
        )
        
        # Extract SQL query
        corrected_sql = self.extract_sql_from_response(raw_response)
        
        result = {
            "incorrect_sql": incorrect_sql,
            "error_message": error_message,
            "corrected_sql": corrected_sql
        }
        
        # Execute the corrected query if requested
        if execute:
            try:
                execution_result = execute_query(corrected_sql)
                result["execution_success"] = True
                result["execution_result"] = execution_result
            except Exception as e:
                result["execution_success"] = False
                result["execution_error"] = str(e)
        
        return result
    
    def process_sql_correction_dataset(self, dataset, execute=False):
        """
        Process a dataset of incorrect SQL queries
        
        Args:
            dataset (list): List of dictionaries containing incorrect SQL queries
            execute (bool): Whether to execute the corrected SQL queries
            
        Returns:
            list: A list of results, each containing the incorrect SQL, 
                  error message, corrected SQL, and optionally the execution results
        """
        results = []
        
        for i, item in enumerate(dataset):
            incorrect_sql = item.get("incorrect_sql", "")
            print(f"Processing query {i+1}/{len(dataset)}: {incorrect_sql[:50]}...")
            
            result = self.correct_sql(incorrect_sql, execute)
            results.append(result)
        
        return results

if __name__ == "__main__":
    # Test the SQL corrector
    corrector = SQLCorrector()
    result = corrector.correct_sql("SELECT * FORM customers", execute=True)
    print("SQL corrector test:")
    print(f"Incorrect SQL: {result['incorrect_sql']}")
    print(f"Error Message: {result['error_message']}")
    print(f"Corrected SQL: {result['corrected_sql']}")
    if "execution_result" in result:
        print(f"Execution Result: {result['execution_result']}")