import os
import json
import pandas as pd
import argparse
import time
import concurrent.futures
from tqdm import tqdm
from database import test_connection
from groq_client import GroqClient
from nl_to_sql import NLtoSQLConverter
from sql_corrector import SQLCorrector

# Simple cache implementation
class QueryCache:
    def __init__(self):
        self.nl_to_sql_cache = {}
        self.sql_correction_cache = {}
    
    def get_nl_to_sql(self, nl_query):
        return self.nl_to_sql_cache.get(nl_query)
    
    def set_nl_to_sql(self, nl_query, result):
        self.nl_to_sql_cache[nl_query] = result
    
    def get_sql_correction(self, incorrect_sql):
        return self.sql_correction_cache.get(incorrect_sql)
    
    def set_sql_correction(self, incorrect_sql, result):
        self.sql_correction_cache[incorrect_sql] = result

# Global cache instance
cache = QueryCache()

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

def process_nl_query(args):
    """Process a single natural language query"""
    converter, nl_query, execute = args
    
    # Check cache
    cached_result = cache.get_nl_to_sql(nl_query)
    if cached_result:
        return cached_result
    
    # Process the query
    result = converter.nl_to_sql(nl_query, execute)
    
    # Cache the result
    cache.set_nl_to_sql(nl_query, result)
    
    return result

def process_incorrect_sql(args):
    """Process a single incorrect SQL query"""
    corrector, incorrect_sql, execute = args
    
    # Check cache
    cached_result = cache.get_sql_correction(incorrect_sql)
    if cached_result:
        return cached_result
    
    # Process the query
    result = corrector.correct_sql(incorrect_sql, execute)
    
    # Cache the result
    cache.set_sql_correction(incorrect_sql, result)
    
    return result

def process_nl_to_sql_task(data_file, output_file, execute=False, max_workers=4, batch_size=10):
    """Process the NL to SQL task with parallel execution"""
    print(f"Processing NL to SQL task using {data_file}")
    
    # Load data
    data = load_json_data(data_file)
    if not data:
        print("No data found. Exiting.")
        return
    
    # Initialize converter
    converter = NLtoSQLConverter()
    
    # Process data in parallel with progress bar
    all_results = []
    
    # Process in batches to avoid overwhelming the API
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        
        # Prepare arguments for parallel processing
        task_args = [(converter, item.get("nl_query", ""), execute) for item in batch]
        
        # Process batch in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_nl_query, args): args for args in task_args}
            
            for future in tqdm(concurrent.futures.as_completed(futures), 
                              total=len(futures),
                              desc=f"Batch {i//batch_size + 1}/{(len(data)-1)//batch_size + 1}"):
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as exc:
                    print(f"Query processing generated an exception: {exc}")
        
        # Intermediate save
        if i % (batch_size * 5) == 0 and i > 0:
            save_results_to_csv(all_results, output_file)
        
        # Rate limiting pause between batches
        time.sleep(2)
    
    # Save final results
    save_results_to_csv(all_results, output_file)

def process_sql_correction_task(data_file, output_file, execute=False, max_workers=4, batch_size=10):
    """Process the SQL correction task with parallel execution"""
    print(f"Processing SQL correction task using {data_file}")
    
    # Load data
    data = load_json_data(data_file)
    if not data:
        print("No data found. Exiting.")
        return
    
    # Initialize corrector
    corrector = SQLCorrector()
    
    # Process data in parallel with progress bar
    all_results = []
    
    # Process in batches to avoid overwhelming the API
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        
        # Prepare arguments for parallel processing
        task_args = [(corrector, item.get("incorrect_sql", ""), execute) for item in batch]
        
        # Process batch in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_incorrect_sql, args): args for args in task_args}
            
            for future in tqdm(concurrent.futures.as_completed(futures), 
                              total=len(futures),
                              desc=f"Batch {i//batch_size + 1}/{(len(data)-1)//batch_size + 1}"):
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as exc:
                    print(f"Query correction generated an exception: {exc}")
        
        # Intermediate save
        if i % (batch_size * 5) == 0 and i > 0:
            save_results_to_csv(all_results, output_file)
        
        # Rate limiting pause between batches
        time.sleep(2)
    
    # Save final results
    save_results_to_csv(all_results, output_file)

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
    parser.add_argument('--max-workers', type=int, default=4,
                      help='Maximum number of worker threads for parallel processing')
    parser.add_argument('--batch-size', type=int, default=10,
                      help='Number of queries to process in each batch')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    # Test database connection
    if not test_connection():
        print("Database connection failed. Please check your configuration.")
        return
    
    # Test Groq API key
    try:
        client = GroqClient()
        _ = client.get_completion("test", max_tokens=10)
    except Exception as e:
        print(f"Groq API connection failed: {e}")
        print("Please set your GROQ_API_KEY environment variable or provide it in the code.")
        return
    
    # Process tasks
    if args.task in ['generate', 'both']:
        process_nl_to_sql_task(args.nl_data, args.nl_output, args.execute, 
                              args.max_workers, args.batch_size)
    
    if args.task in ['correct', 'both']:
        process_sql_correction_task(args.sql_data, args.sql_output, args.execute,
                                   args.max_workers, args.batch_size)
    
    elapsed_time = time.time() - start_time
    print(f"Total execution time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()