import os
import json
import argparse
from tqdm import tqdm
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

def save_json_data(data, file_path):
    """Save data to a JSON file"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        print(f"Results saved to {file_path}")
    except Exception as e:
        print(f"Error saving results to {file_path}: {e}")

def process_nl_to_sql_data(input_file, output_file):
    """Process the NL to SQL data"""
    print(f"Processing NL to SQL data from {input_file}")
    
    # Load data
    train_data = load_json_data(input_file)
    if not train_data:
        print("No training data found. Exiting.")
        return
    
    # Initialize converter
    converter = NLtoSQLConverter()
    
    # Process each NL query
    results = []
    
    for item in tqdm(train_data, desc="Processing NL queries"):
        nl_query = item.get("NL", "")
        if not nl_query:
            continue
        
        # Generate SQL from NL
        result = converter.nl_to_sql(nl_query, execute=False)
        
        # Create output item
        output_item = {
            "nl_query": nl_query,
            "generated_sql": result["generated_sql"],
            "reference_sql": item.get("Query", "")  # Include reference SQL if available
        }
        
        results.append(output_item)
    
    # Save results
    save_json_data(results, output_file)
    print(f"Processed {len(results)} NL queries")

def process_sql_correction_data(input_file, output_file):
    """Process the SQL correction data"""
    print(f"Processing SQL correction data from {input_file}")
    
    # Load data
    train_data = load_json_data(input_file)
    if not train_data:
        print("No training data found. Exiting.")
        return
    
    # Initialize corrector
    corrector = SQLCorrector()
    
    # Process each incorrect SQL query
    results = []
    
    for item in tqdm(train_data, desc="Processing incorrect SQL queries"):
        incorrect_sql = item.get("IncorrectQuery", "")
        if not incorrect_sql:
            continue
        
        # Correct SQL
        result = corrector.correct_sql(incorrect_sql, execute=False)
        
        # Create output item
        output_item = {
            "incorrect_sql": incorrect_sql,
            "corrected_sql": result["corrected_sql"],
            "reference_sql": item.get("CorrectQuery", "")  # Include reference SQL if available
        }
        
        results.append(output_item)
    
    # Save results
    save_json_data(results, output_file)
    print(f"Processed {len(results)} SQL queries")

def main():
    parser = argparse.ArgumentParser(description='Process NL-to-SQL and SQL correction training data')
    parser.add_argument('--nl-input', type=str, default='train_generate_task.json',
                      help='Path to NL to SQL training data JSON file')
    parser.add_argument('--sql-input', type=str, default='train_query_correction_task.json',
                      help='Path to SQL correction training data JSON file')
    parser.add_argument('--nl-output', type=str, default='generate_task.json',
                      help='Path to output file for NL to SQL results')
    parser.add_argument('--sql-output', type=str, default='query_correction_task.json',
                      help='Path to output file for SQL correction results')
    parser.add_argument('--task', type=str, choices=['generate', 'correct', 'both'], default='both',
                      help='Task to perform: generate (NL to SQL), correct (SQL correction), or both')
    
    args = parser.parse_args()
    
    # Process tasks based on argument
    if args.task in ['generate', 'both']:
        process_nl_to_sql_data(args.nl_input, args.nl_output)
    
    if args.task in ['correct', 'both']:
        process_sql_correction_data(args.sql_input, args.sql_output)

if __name__ == "__main__":
    main()