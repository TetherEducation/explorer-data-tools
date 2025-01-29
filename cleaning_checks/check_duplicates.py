############################################
##
## check_duplicates.py
##
## Checks duplicates in backend tables based on the provided checklist.
##
## Author: Ignacio Lepe
## Created: 2025/01/24
##
############################################

###### Paths
# Data path
data_path = '/Users/ignaciolepe/Documents/duplicates_check/data'
# Duplicates list to check
dup_path = '/Users/ignaciolepe/Documents/duplicates_check/data/resumen_duplicados.xlsx'
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

###### Function to check for duplicates in the database
def check_duplicates(conn, table_name, unique_columns):
    """
    Queries the database to check if a table has duplicate records based on unique columns.

    Args:
        conn (psycopg2 connection): Active database connection.
        table_name (str): Table to check for duplicates.
        unique_columns (list): Columns that should be unique.

    Returns:
        bool: True if duplicates exist, False otherwise.
    """
    try:
        cursor = conn.cursor()
        unique_cols_str = ', '.join(unique_columns)
        query = f"""
            SELECT COUNT(*)
            FROM (
                SELECT {unique_cols_str}, COUNT(*) as cnt
                FROM {table_name}
                GROUP BY {unique_cols_str}
                HAVING COUNT(*) > 1
            ) subquery;
        """
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        return result[0] > 0 if result else False
    except Exception as e:
        print(f"Error checking duplicates in {table_name}: {e}")
        return False

###### Loop through the checklist to check duplicates
for idx, row in duplicate_checklist.iterrows():
    table_name = row['Tabla']
    country = row['País']
    unique_columns = row['identificador_unico'].split(', ')

    print(f"\nProcessing table: {table_name} ({country})")

    # Initialize duplicate flags
    existe_duplicado_st = False
    existe_duplicado_pr = False

    # Check duplicates in staging
    existe_duplicado_st = check_duplicates(conn_staging, f"{country}.{table_name}", unique_columns)

    # Check duplicates in production
    existe_duplicado_pr = check_duplicates(conn_production, f"{country}.{table_name}", unique_columns)

    # Update the checklist
    duplicate_checklist.loc[idx, 'existe_duplicado_st'] = existe_duplicado_st
    duplicate_checklist.loc[idx, 'existe_duplicado_pr'] = existe_duplicado_pr

###### Save updated checklist
output_path = os.path.join(data_path, 'resumen_duplicados_actualizado.xlsx')
duplicate_checklist.to_excel(output_path, index=False)
print(f"\nUpdated checklist saved to {output_path}")

###### Close connections
conn_staging.close()
conn_production.close()