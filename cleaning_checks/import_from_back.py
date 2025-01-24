############################################
##
## import_from_back.py
##
## Provides a function to import a specified table
## from the backend (staging or production) using given credentials.
##
## Author: Ignacio Lepe
## Created: 2024/11/15
##
############################################

# Imports
import os
import pandas as pd
import psycopg2
import csv

# Output directory for saved CSV files
output_dir = '/Users/ignaciolepe/Documents/duplicates_check/data'
os.makedirs(output_dir, exist_ok=True)

# Function to read credentials from a CSV file
def load_credentials(cred_path):
    """
    Load database credentials from a CSV file.

    Args:
        cred_path (str): Path to the credentials file.

    Returns:
        dict: A dictionary containing the database credentials.
    """
    try:
        with open(cred_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            credentials = next(reader)
            return {
                'dsn_database_users': credentials['dsn_database_users'],
                'dsn_database_tables': credentials['dsn_database_tables'],
                'dsn_hostname_te_pr': credentials['dsn_hostname_te_pr'],  # Production host
                'dsn_hostname_te_st': credentials['dsn_hostname_te_st'],  # Staging host
                'dsn_pwd_te_pr': credentials['dsn_pwd_te_pr'],            # Production password
                'dsn_pwd_te_st': credentials['dsn_pwd_te_st'],            # Staging password
                'dsn_port': credentials['dsn_port'],
                'dsn_uid_te': credentials['dsn_uid_te']
            }
    except Exception as e:
        print(f"Error reading credentials file: {e}")
        return None

# Generalized function to connect to the backend
def connect_to_backend(credentials, environment="staging"):
    """
    Connect to the specified backend environment (staging or production).

    Args:
        credentials (dict): A dictionary containing database credentials.
        environment (str): The target environment ('staging' or 'production').

    Returns:
        psycopg2.extensions.connection: A connection object to the database.
    """
    try:
        if environment == "production":
            conn = psycopg2.connect(
                dbname=credentials['dsn_database_tables'],
                host=credentials['dsn_hostname_te_pr'],
                port=credentials['dsn_port'],
                user=credentials['dsn_uid_te'],
                password=credentials['dsn_pwd_te_pr']
            )
        elif environment == "staging":
            conn = psycopg2.connect(
                dbname=credentials['dsn_database_tables'],
                host=credentials['dsn_hostname_te_st'],
                port=credentials['dsn_port'],
                user=credentials['dsn_uid_te'],
                password=credentials['dsn_pwd_te_st']
            )
        else:
            raise ValueError("Invalid environment specified. Use 'staging' or 'production'.")
        
        print(f"Connected to the {environment} database successfully.")
        return conn
    except Exception as e:
        print(f"Unable to connect to the {environment} database: {e}")
        return None

# Function to import a specific table from the backend
def import_back_tables(table_name, credentials, environment="staging"):
    """
    Import a specific table from the specified backend environment.

    Args:
        table_name (str): Name of the table to import.
        credentials (dict): A dictionary containing database credentials.
        environment (str): The target environment ('staging' or 'production').

    Returns:
        pd.DataFrame: A DataFrame containing the imported table data.
    """
    # Connect to the specified environment
    conn = connect_to_backend(credentials, environment)
    if not conn:
        print(f"Failed to connect to the {environment} database.")
        return None

    # Query and import the table
    query = f"SELECT * FROM {table_name}"
    try:
        df = pd.read_sql_query(query, conn)
        pd.set_option('display.max_rows', None)  # Display all rows
        print(f"\nData from {table_name}:")
        #print(df)

        # Save the DataFrame to CSV
        output_path = os.path.join(output_dir, f"{table_name.split('.')[-1]}_{environment}.csv")
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Saved {table_name} to {output_path}")
        return df
    except Exception as e:
        print(f"Error importing data from {table_name}: {e}")
        return None
    finally:
        # Close the database connection
        conn.close()
        print(f"{environment.capitalize()} database connection closed.")

# Example usage:
# cred_path = '/path/to/credentials.csv'
# credentials = load_credentials(cred_path)
# table_name = "your_table_name"
# environment = "staging"  # or "production"
# df = import_back_tables(table_name, credentials, environment)