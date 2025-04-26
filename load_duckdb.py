# load_duckdb.py

import duckdb
import pandas as pd
import os

def load_transformed_data_to_duckdb(df: pd.DataFrame, db_path: str = 'traffic_data.duckdb', table_name: str = 'traffic_data'):
    """
    Loads a pandas DataFrame into a DuckDB database table.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to load.
        db_path (str): The path to the DuckDB database file. Defaults to 'traffic_data.duckdb'.
        table_name (str): The name of the table to load the data into. Defaults to 'traffic_data'.
    """
    print(f"\n--- Starting Load Phase (DuckDB) ---")

    if df.empty:
        print("No data to load into DuckDB. Skipping load phase.")
        return

    try:
        # Connect to DuckDB - creates the database file if it doesn't exist
        con = duckdb.connect(database=db_path, read_only=False)
        print(f"Connected to DuckDB database: {db_path}")

        # Create or Replace a persistent table
        print(f"Loading data into table: {table_name}")
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df;")
        print(f"Successfully loaded {len(df)} rows into table '{table_name}' in {db_path}")

        # Example: Verify data by querying
        # print(f"Querying sample data from {table_name}:")
        # sample_df = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        # print(sample_df)

    except Exception as e:
        print(f"❌ Error loading data to DuckDB: {e}")
    finally:
        # Close the connection
        if 'con' in locals() and con:
            con.close()
            print("DuckDB connection closed.")

    print(f"--- Load Phase (DuckDB) Complete ---")

# Example of how to use this function (for testing the load script directly)
if __name__ == "__main__":
    print("Running load_duckdb.py directly for testing...")

    # Create a dummy DataFrame for testing
    test_data = {
        'currentSpeed': [50.5, 60.1, 45.0], # km/h
        'freeFlowSpeed': [70.0, 75.5, 65.2], # km/h
        'currentTravelTime': [120, 90, 150], # seconds per segment
        'freeFlowTravelTime': [80, 60, 100], # seconds per segment
        'confidence': [7, 8, 6], # unitless
        'frc': ['FRC1', 'FRC0', 'FRC2'], # string
        'roadClosure': [False, False, True], # boolean
        'coordinate_count': [2, 3, 2], # unitless
        'coordinates': [[(10.1, 100.1), (10.2, 100.2)], [(10.3, 100.3), (10.4, 100.4), (10.5, 100.5)], [(10.6, 100.6), (10.7, 100.7)]] # list of tuples (degrees)
    }
    dummy_df = pd.DataFrame(test_data)

    print("\nDummy DataFrame created:")
    print(dummy_df)

    # Define a test database path
    test_db_path = 'test_traffic_data.duckdb'

    # Load the dummy data
    load_transformed_data_to_duckdb(dummy_df, db_path=test_db_path, table_name='test_traffic_table')

    # Optional: Verify the data was loaded by querying the test database
    try:
        con_test = duckdb.connect(database=test_db_path, read_only=True)
        print(f"\nVerifying data in {test_db_path}:")
        verified_df = con_test.execute("SELECT * FROM test_traffic_table").fetchdf()
        print("\nData successfully loaded and retrieved:")
        print(verified_df)
        con_test.close()
        # Clean up the test database file
        # os.remove(test_db_path)
        # print(f"\nCleaned up test database file: {test_db_path}")
    except Exception as e:
        print(f"\nError verifying data in test database: {e}")