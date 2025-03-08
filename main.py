import os
import json
import pandas as pd
import argparse
from database import test_connection
from groq_client import GroqClient
from nl_to_sql import NLtoSQLConverter
from sql_corrector import SQLCorrector

def load_json_data(file_path):
    """Load JSON data from a file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")
        return []

def save_results_to_csv(results, output_file):
    """Save results to a CSV file"""
    try:
        df = pd.DataFrame(results)
        df.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")
    except Exception as e:
        print(f"Error saving results to {output_file}: {e}")

def process_nl_to_sql_task(data_file, output_file, execute=False):
    """Process the NL to SQL task"""
    print(f"Processing NL to SQL task using {data_file}")
    
    # Load data
    data = load_json_data(data_file)
    if not data:
        print("No data found. Exiting.")
        return
    
    # Initialize converter
    converter = NLtoSQLConverter()
    
    # Process data
    results = converter.process_nl_to_sql_dataset(data, execute)
    
    # Save results
    save_results_to_csv(results, output_file)

def process_sql_correction_task(data_file, output_file, execute=False):
    """Process the SQL correction task"""
    print(f"Processing SQL correction task using {data_file}")
    
    # Load data
    data = load_json_data(data_file)
    if not data:
        print("No data found. Exiting.")
        return
    
    # Initialize corrector
    corrector = SQLCorrector()
    
    # Process data
    results = corrector.process_sql_correction_dataset(data, execute)
    
    # Save results
    save_results_to_csv(results, output_file)

def main():
    parser = argparse.ArgumentParser(description='AI-Powered SQL Query Generator and Error Corrector')
    parser.add_argument('--task', type=str, choices=['generate', 'correct', 'both'], default='both',
                      help='Task to perform: generate (NL to SQL), correct (SQL correction), or both')
    parser.add_argument('--execute', action='store_true',
                      help='Execute the generated/corrected SQL queries')
    parser.add_argument('--nl-data', type=str, default='train_generate_task.json',
                      help='Path to NL to SQL dataset JSON file')
    parser.add_argument('--sql-data', type=str, default='train_query_correction_task.json',
                      help='Path to SQL correction dataset JSON file')
    parser.add_argument('--nl-output', type=str, default='nl_to_sql_results.csv',
                      help='Path to output file for NL to SQL results')
    parser.add_argument('--sql-output', type=str, default='sql_correction_results.csv',
                      help='Path to output file for SQL correction results')
    
    args = parser.parse_args()
    
    # Test database connection
    if not test_connection():
        print("Database connection failed. Please check your configuration.")
        return
    
    # Test Groq API key
    try:
        client = GroqClient(api_key="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")
        _ = client.get_completion("test", max_tokens=10)
    except Exception as e:
        print(f"Groq API connection failed: {e}")
        print("Please set your GROQ_API_KEY environment variable or provide it in the code.")
        return
    
    # Process tasks
    if args.task in ['generate', 'both']:
        process_nl_to_sql_task(args.nl_data, args.nl_output, args.execute)
    
    if args.task in ['correct', 'both']:
        process_sql_correction_task(args.sql_data, args.sql_output, args.execute)

if __name__ == "__main__":
    main()