import duckdb
import pandas as pd
import os

# Define the path to your DuckDB database file
# Make sure this path is correct based on where main.py created the file
# (It should be in the same directory as your main.py if you used the path from the last main.py code)
duckdb_database_path = 'traffic_data.duckdb' # Or the full path if it's elsewhere
table_name = 'traffic_data'

print(f"Attempting to connect to DuckDB database: {duckdb_database_path}")

try:
    # Connect to the DuckDB database
    con = duckdb.connect(database=duckdb_database_path, read_only=True) # Open in read-only mode

    print(f"Successfully connected. Querying table: {table_name}")

    # Execute a SQL query to select all data from the table
    query = f"SELECT * FROM {table_name};"
    df = con.execute(query).fetchdf()

    # Print the data as a pandas DataFrame
    print("\nData from the DuckDB table:")
    print(df)

except duckdb.CatalogException as e:
     print(f"❌ Error: Table '{table_name}' not found in the database.")
     print("Please ensure the table name is correct and the ETL pipeline ran successfully.")
     print(e)
except Exception as e:
    print(f"❌ An error occurred while accessing DuckDB: {e}")
finally:
    # Close the connection
    if 'con' in locals() and con:
        con.close()
        print("\nDuckDB connection closed.")