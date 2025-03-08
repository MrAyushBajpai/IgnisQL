import sys
from groq_client import GroqClient
from nl_to_sql import NLtoSQLConverter
from sql_corrector import SQLCorrector

def main():
    nl_converter = NLtoSQLConverter()
    sql_corrector = SQLCorrector()
    
    while True:
        print("\nChoose an option:")
        print("1. Convert natural language to SQL")
        print("2. Correct SQL query")
        print("3. Exit")
        
        choice = input("Enter your choice (1-3): ")
        
        if choice == '1':
            nl_query = input("Enter your natural language query: ")
            result = nl_converter.nl_to_sql(nl_query, execute=True)
            print("\nGenerated SQL:")
            print(result['generated_sql'])
            if 'execution_result' in result:
                print("\nExecution Result:")
                print(result['execution_result'])
        
        elif choice == '2':
            sql_query = input("Enter the SQL query to correct: ")
            result = sql_corrector.correct_sql(sql_query, execute=True)
            print("\nCorrected SQL:")
            print(result['corrected_sql'])
            if 'execution_result' in result:
                print("\nExecution Result:")
                print(result['execution_result'])
        
        elif choice == '3':
            print("Exiting...")
            sys.exit(0)
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()