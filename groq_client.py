import os
from groq import Groq

class GroqClient:
    def __init__(self, api_key=None):
        self.api_key = "?"
        if not self.api_key:
            raise ValueError("API key must be provided or set as an environment variable (GROQ_API_KEY).")

        self.client = Groq(api_key=self.api_key)

        self.models = {
            "llama3-8b": "llama3-8b-8192",
            "llama3-8b-instant" : "llama-3.1-8b-instant",
            "mixtral-8x7b": "mixtral-8x7b-32768",
            "gemma-9b": "gemma2-9b-it"
        }
        self.default_model = self.models.get("llama3-8b")

    def get_completion(self, prompt, system_prompt=None, model=None, temperature=0.1, max_tokens=1024):
        messages = [{"role": "system", "content": system_prompt}] if system_prompt else []
        messages.append({"role": "user", "content": prompt})

        try:
            response = self.client.chat.completions.create(
                messages=messages,
                model=model or self.default_model,
                temperature=temperature,
                max_tokens=max_tokens
            )
            return response.choices[0].message.content

        except Exception as e:
            raise RuntimeError(f"Groq API error: {str(e)}") from e

    def get_nl_to_sql_completion(self, nl_query, schema_info, temperature=0.1):
        system_prompt = self._common_system_prompt("SQL")
        prompt = f"{schema_info}\n\nNatural Language Query: {nl_query}\n\nGenerate the SQL query:"
        return self.get_completion(prompt, system_prompt, temperature=temperature)

    def get_sql_correction_completion(self, incorrect_sql, schema_info, error_message=None, temperature=0.1):
        system_prompt = self._common_system_prompt("debug")
        prompt = f"{schema_info}\n\nIncorrect SQL Query:\n```sql\n{incorrect_sql}\n```"
        if error_message:
            prompt += f"\n\nError Message: {error_message}"
        prompt += "\n\nCorrected SQL Query:"
        return self.get_completion(prompt, system_prompt, temperature=temperature)

    def _common_system_prompt(self, mode):
        prompts = {
            "SQL": "You are an expert SQL query generator. Generate only SQL code with proper formatting, optimized for PostgreSQL.",
            "debug": "You are an expert SQL debugger. Fix errors in SQL queries efficiently for PostgreSQL, without explanations."
        }
        return prompts.get(mode, "")

if __name__ == "__main__":
    try:
        client = GroqClient()
        response = client.get_completion("Hello, how are you?")
        print("Groq client initialized successfully!")
        print(f"Test response: {response[:100]}...")
    except Exception as e:
        print(f"Error initializing Groq client: {e}")
