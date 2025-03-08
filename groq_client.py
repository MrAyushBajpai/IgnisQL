import os
from groq import Groq

class GroqClient:
    def __init__(self, api_key=None):
        """Initialize the Groq API client with the provided API key or from environment variable"""
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY must be provided or set as an environment variable")
        
        self.client = Groq(api_key=self.api_key)
        
        # Available Groq models (up to 7B as per hackathon requirements)
        self.models = {
            "llama3-8b": "llama3-8b-8192",
            "mixtral-8x7b": "mixtral-8x7b-32768",
            "gemma-7b": "gemma-7b-it"
        }
        
        # Default model to use
        self.default_model = self.models["llama3-8b"]
    
    def get_completion(self, prompt, system_prompt=None, model=None, temperature=0.1, max_tokens=1024):
        """
        Get a completion from the Groq API
        
        Args:
            prompt (str): The user prompt
            system_prompt (str, optional): The system prompt to guide the model's behavior
            model (str, optional): The model ID to use. Defaults to llama3-8b-8192.
            temperature (float, optional): Sampling temperature. Defaults to 0.1.
            max_tokens (int, optional): Max tokens to generate. Defaults to 1024.
            
        Returns:
            str: The generated completion
        """
        try:
            messages = []
            
            # Add system prompt if provided
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            
            # Add user prompt
            messages.append({"role": "user", "content": prompt})
            
            # Use specified model or default
            model_id = model or self.default_model
            
            # Make API call
            response = self.client.chat.completions.create(
                messages=messages,
                model=model_id,
                temperature=temperature,
                max_tokens=max_tokens
            )
            
            return response.choices[0].message.content
        
        except Exception as e:
            print(f"Error getting completion from Groq: {e}")
            return f"Error: {str(e)}"
    
    def get_nl_to_sql_completion(self, nl_query, schema_info, temperature=0.1):
        """Specialized completion method for natural language to SQL conversion"""
        system_prompt = """You are an expert SQL query generator. 
Your task is to convert natural language queries into precise, efficient SQL queries.
Always follow these rules:
1. Generate only SQL code without any explanations or comments
2. Ensure the queries are compatible with PostgreSQL
3. Use schema information provided to reference the correct tables and columns
4. Return the most efficient query possible
5. Format the SQL query with proper indentation"""
        
        prompt = f"""
{schema_info}

Natural Language Query: {nl_query}

Generate the SQL query:"""
        
        return self.get_completion(prompt, system_prompt, temperature=temperature)
    
    def get_sql_correction_completion(self, incorrect_sql, schema_info, error_message=None, temperature=0.1):
        """Specialized completion method for SQL query correction"""
        system_prompt = """You are an expert SQL query debugger.
Your task is to correct errors in SQL queries based on the provided error messages and schema information.
Always follow these rules:
1. Generate only the corrected SQL code without any explanations or comments
2. Ensure the queries are compatible with PostgreSQL
3. Fix all syntax errors, logical errors, and reference errors
4. Use schema information provided to reference the correct tables and columns
5. Return the most efficient query possible
6. Format the SQL query with proper indentation"""
        
        prompt = f"""
{schema_info}

Incorrect SQL Query:
```sql
{incorrect_sql}
```
"""
        
        if error_message:
            prompt += f"\nError Message: {error_message}\n"
        
        prompt += "\nCorrected SQL Query:"
        
        return self.get_completion(prompt, system_prompt, temperature=temperature)

if __name__ == "__main__":
    # Test the Groq client
    try:
        client = GroqClient()
        response = client.get_completion("Hello, how are you?")
        print("Groq client initialized successfully!")
        print(f"Test response: {response[:100]}...")
    except Exception as e:
        print(f"Error initializing Groq client: {e}")