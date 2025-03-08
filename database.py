import psycopg2
import pandas as pd
from psycopg2 import sql

# Database configuration - update with your PostgreSQL credentials
DB_CONFIG = {
    'host': 'localhost',
    'database': 'dataH',
    'user': 'postgres',  # Update this with your username
    'password': '1711',  # Update this with your password
    'port': '5432'
}

def get_connection():
    """Establish a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def execute_query(query, fetch=True):
    """Execute a SQL query and return the results"""
    conn = get_connection()
    if not conn:
        return "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        if fetch:
            # Attempt to fetch results if applicable
            try:
                results = cursor.fetchall()
                # Get column names
                colnames = [desc[0] for desc in cursor.description]
                # Convert to DataFrame
                df = pd.DataFrame(results, columns=colnames)
                return df
            except psycopg2.ProgrammingError:
                # No results to fetch (e.g., for INSERT/UPDATE/DELETE)
                conn.commit()
                return f"Query executed successfully. Rows affected: {cursor.rowcount}"
        else:
            # For non-SELECT queries, just commit the transaction
            conn.commit()
            return f"Query executed successfully. Rows affected: {cursor.rowcount}"
    
    except Exception as e:
        conn.rollback()
        return f"Error executing query: {str(e)}"
    
    finally:
        cursor.close()
        conn.close()

def test_connection():
    """Test the database connection and print schema information"""
    conn = get_connection()
    if not conn:
        return "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        
        # Get list of tables
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
    # Test the database connection
    test_connection()