import pandas as pd
from database import get_connection

def get_schema_info():
    """
    Extract the database schema information including tables, columns, data types, 
    primary keys, foreign keys, and relationships.
    """
    conn = get_connection()
    if not conn:
        return "Failed to connect to database"
    
    try:
        schema_info = {
            "tables": [],
            "relationships": []
        }
        
        # Get all tables
        tables_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """
        
        cursor = conn.cursor()
        cursor.execute(tables_query)
        tables = [table[0] for table in cursor.fetchall()]
        
        # For each table, get columns with their data types
        for table in tables:
            table_info = {"name": table, "columns": []}
            
            # Get columns
            columns_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{table}'
            ORDER BY ordinal_position
            """
            
            cursor.execute(columns_query)
            columns = cursor.fetchall()
            
            for column in columns:
                column_name, data_type, is_nullable = column
                table_info["columns"].append({
                    "name": column_name,
                    "type": data_type,
                    "nullable": is_nullable
                })
            
            # Get primary key
            pk_query = f"""
            SELECT c.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
            JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
              AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = '{table}'
            """
            
            cursor.execute(pk_query)
            pks = [pk[0] for pk in cursor.fetchall()]
            table_info["primary_keys"] = pks
            
            schema_info["tables"].append(table_info)
        
        # Get foreign keys (relationships)
        fk_query = """
        SELECT
            tc.table_name AS table_name,
            kcu.column_name AS column_name,
            ccu.table_name AS referenced_table_name,
            ccu.column_name AS referenced_column_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        """
        
        cursor.execute(fk_query)
        fks = cursor.fetchall()
        
        for fk in fks:
            table_name, column_name, referenced_table_name, referenced_column_name = fk
            schema_info["relationships"].append({
                "table": table_name,
                "column": column_name,
                "referenced_table": referenced_table_name,
                "referenced_column": referenced_column_name
            })
        
        return schema_info
    
    except Exception as e:
        print(f"Error extracting schema: {e}")
        return None
    
    finally:
        cursor.close()
        conn.close()

def format_schema_for_prompt():
    """Format the schema information into a string for inclusion in LLM prompts"""
    schema_info = get_schema_info()
    if not schema_info or isinstance(schema_info, str):
        return "Could not retrieve schema information"
    
    prompt_text = "Database Schema:\n\n"
    
    # Add tables and columns
    for table in schema_info["tables"]:
        prompt_text += f"Table: {table['name']}\n"
        prompt_text += "Columns:\n"
        
        for column in table["columns"]:
            nullable = "NULL" if column["nullable"] == "YES" else "NOT NULL"
            prompt_text += f"  - {column['name']} ({column['type']}, {nullable})"
            
            # Mark primary keys
            if "primary_keys" in table and column["name"] in table["primary_keys"]:
                prompt_text += " PRIMARY KEY"
            
            prompt_text += "\n"
        
        prompt_text += "\n"
    
    # Add relationships
    if schema_info["relationships"]:
        prompt_text += "Relationships:\n"
        for rel in schema_info["relationships"]:
            prompt_text += f"  - {rel['table']}.{rel['column']} references {rel['referenced_table']}.{rel['referenced_column']}\n"
    
    return prompt_text

if __name__ == "__main__":
    # Test the schema extraction
    schema = get_schema_info()
    print("Schema extracted successfully!")
    
    # Test the prompt formatting
    prompt = format_schema_for_prompt()
    print("\nFormatted Schema for Prompt:")
    print(prompt)