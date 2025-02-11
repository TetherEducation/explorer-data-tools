############################################
##
## export_duplicates.py
##
## Exports only the duplicated rows for any table, country, 
## and environment (staging or production) that contains duplicates.
##
## Author: Ignacio Lepe
## Created: 2025/01/30
##
############################################

###### Paths
# Data path
data_path = '/Users/ignaciolepe/Documents/duplicates_check/data'
# Duplicates list to check
dup_path = '/Users/ignaciolepe/Documents/duplicates_check/data/resumen_duplicados.xlsx'
# Output path for duplicate rows
out_path = '/Users/ignaciolepe/Documents/duplicates_check/output'
######

###### Packages
import sys
import os
import pandas as pd
# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_connection import conect_bd
###### 

###### Load duplicates checklist
duplicate_checklist = pd.read_excel(dup_path)

###### Connect to databases
environment_staging = 'staging'
environment_production = 'production'

conn_staging = conect_bd('core', environment_staging)
conn_production = conect_bd('core', environment_production)

###### Function to fetch only the duplicated rows from the database
def get_duplicate_rows(conn, table_name, unique_columns):
    """
    Queries the database to retrieve only duplicated records based on unique columns.

    Args:
        conn (psycopg2 connection): Active database connection.
        table_name (str): Table to check for duplicates.
        unique_columns (list): Columns that should be unique.

    Returns:
        pd.DataFrame: A DataFrame containing only the duplicated rows, sorted by unique columns.
    """
    try:
        cursor = conn.cursor()
        unique_cols_str = ', '.join(unique_columns)

        query = f"""
            SELECT * FROM {table_name}
            WHERE ({unique_cols_str}) IN (
                SELECT {unique_cols_str}
                FROM {table_name}
                GROUP BY {unique_cols_str}
                HAVING COUNT(*) > 1
            )
            ORDER BY {unique_cols_str};  -- Sorting by unique columns
        """

        df = pd.read_sql_query(query, conn)
        cursor.close()
        
        return df if not df.empty else None
    except Exception as e:
        print(f"Error fetching duplicates from {table_name}: {e}")
        return None

###### Loop through the checklist to export only duplicated rows
for idx, row in duplicate_checklist.iterrows():
    table_name = row['Tabla']
    country = row['País']
    unique_columns = row['identificador_unico'].split(', ')

    print(f"\nProcessing table: {table_name} ({country})")

    # Check for duplicates in staging
    if row['existe_duplicado_st']:
        print(f"Exporting duplicates for {table_name} in STAGING...")
        df_st = get_duplicate_rows(conn_staging, f"{country}.{table_name}", unique_columns)
        if df_st is not None:
            output_file_st = os.path.join(out_path, f"{country}_{table_name}_duplicates_staging.csv")
            df_st.to_csv(output_file_st, index=False, encoding='utf-8')
            print(f"Duplicated rows saved to {output_file_st}")

    # Check for duplicates in production
    if row['existe_duplicado_pr']:
        print(f"Exporting duplicates for {table_name} in PRODUCTION...")
        df_pr = get_duplicate_rows(conn_production, f"{country}.{table_name}", unique_columns)
        if df_pr is not None:
            output_file_pr = os.path.join(out_path, f"{country}_{table_name}_duplicates_production.csv")
            df_pr.to_csv(output_file_pr, index=False, encoding='utf-8')
            print(f"Duplicated rows saved to {output_file_pr}")

###### Close connections
conn_staging.close()
conn_production.close()

print("\nDuplicate export process completed.")