import psycopg2
import pandas as pd
from psycopg2 import sql

DB_CONFIG = {
    'host': 'localhost',
    'database': 'dataH',
    'user': 'postgres',
    'password': '1711',
    'port': '5432'
}

def get_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def execute_query(query, fetch=True):
    conn = get_connection()
    if not conn:
        return "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        if fetch:
            try:
                results = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                return pd.DataFrame(results, columns=colnames)
            except psycopg2.ProgrammingError:
                conn.commit()
                return f"Query executed successfully. Rows affected: {cursor.rowcount}"
        else:
            conn.commit()
            return f"Query executed successfully. Rows affected: {cursor.rowcount}"
    
    except Exception as e:
        conn.rollback()
        return f"Error executing query: {str(e)}"
    
    finally:
        cursor.close()
        conn.close()

def test_connection():
    conn = get_connection()
    if not conn:
        return "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        
        print("Database connected successfully!")
        print("Tables in the database:")
        for table in tables:
            print(f"- {table[0]}")
        
        return True
    
    except Exception as e:
        print(f"Error testing connection: {e}")
        return False
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    test_connection()
