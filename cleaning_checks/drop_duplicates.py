############################################
##
## delete_duplicates.py
##
## Deletes duplicated rows from chile.institutions_images table
## in either staging or production environments, keeping only the most recent one.
##
## Author: Ignacio Lepe
## Created: 2025/01/30
##
############################################

import os

###### Paths
# Data path
data_path = '/Users/ignaciolepe/Documents/duplicates_check/output'
# Duplicates file paths
dup_files = {
    "staging": os.path.join(data_path, 'chile_institutions_images_duplicates_staging.csv'),
    "production": os.path.join(data_path, 'chile_institutions_images_duplicates_production.csv')
}
######

###### Packages
import sys
import pandas as pd
# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_connection import conect_bd
######

import pandas as pd
from psycopg2.extras import execute_batch
from utils.db_connection import conect_bd

def delete_from_postgres(df, query, environment, batch_size=500):
    """
    Ejecuta un DELETE en PostgreSQL en batches, permitiendo múltiples variables.
    
    :param df: DataFrame con los registros a eliminar.
    :param query: Consulta SQL con `DELETE FROM ... WHERE (campo1, campo2, ...) IN %s;`
    :param environment: Entorno de ejecución ('staging' o 'production').
    :param batch_size: Número de registros por batch.
    """

    # Crear conexión a la base de datos
    conn = conect_bd('core', environment)
    cursor = conn.cursor()

    try:
        with conn:
            with cursor:
                # Determinar si se eliminan por una o varias columnas
                if len(df.columns) == 1:
                    column_name = df.columns[0]
                    for i in range(0, len(df), batch_size):
                        batch = df.iloc[i:i+batch_size][column_name].tolist()
                        if batch:
                            cursor.execute(query, (batch,))
                            conn.commit()
                            print(f"Eliminados {cursor.rowcount} registros en este batch.")
                else:
                    for i in range(0, len(df), batch_size):
                        batch = list(df.iloc[i:i+batch_size].itertuples(index=False, name=None))
                        if batch:
                            execute_batch(cursor, query, batch)
                            conn.commit()
                            print(f"Eliminados {cursor.rowcount} registros en este batch.")
    except Exception as e:
        conn.rollback()
        print(f"Error durante la eliminación: {e}")
    finally:
        cursor.close()
        conn.close()



###### Step 1: Ask User for Environment Selection
valid_envs = ["staging", "production"]
selected_env = input("Select environment (staging/production): ").strip().lower()

while selected_env not in valid_envs:
    print("Invalid choice. Please enter 'staging' or 'production'.")
    selected_env = input("Select environment (staging/production): ").strip().lower()

print(f"\nSelected environment: {selected_env.upper()}")

###### Step 2: Connect to the Database
conn = conect_bd('core', selected_env)

###### Step 3: Load Duplicates CSV
dup_file_path = dup_files[selected_env]

if not os.path.exists(dup_file_path):
    print(f"Error: The file {dup_file_path} does not exist.")
    sys.exit()

df_duplicates = pd.read_csv(dup_file_path)

###### Step 4: Select the Most Recent Row for Each Duplicate Group
# Define unique identifier columns
unique_columns = ["campus_code", "institution_code", "image_name", "image_link"]

# Keep the most recent row (based on 'created') for each duplicate group
df_keep = df_duplicates.sort_values(by="order", ascending=False).drop_duplicates(subset=unique_columns, keep="first")

# Find rows to delete (all rows except the ones we're keeping)
df_delete = df_duplicates.merge(df_keep, on=["campus_code", "institution_code", "image_name", "image_link", "order"], how="left", indicator=True)
df_delete = df_delete[df_delete["_merge"] == "left_only"].drop(columns=["_merge"])

# Handle the ID column name issue
if "id_dup" in df_delete.columns:
    df_delete.rename(columns={"id_dup": "id"}, inplace=True)
elif "id_x" in df_delete.columns:
    df_delete.rename(columns={"id_x": "id"}, inplace=True)

if df_delete.empty:
    print("No duplicates found to delete.")
else:
    print(f"Deleting {len(df_delete)} duplicate rows from {selected_env.upper()}...")

    ###### Step 5: Execute Batch Deletion
    delete_query = """
        DELETE FROM chile.institutions_images 
        WHERE id = ANY(%s);
    """

    delete_from_postgres(df_delete[['id']], delete_query, selected_env, batch_size=500)

###### Step 6: Download the Updated Table
print("Fetching updated data from the database...")

query = """
    SELECT * FROM chile.institutions_images;
"""

df_updated = pd.read_sql_query(query, conn)

###### Step 7: Verify Uniqueness at (campus_code, institution_code, image_name, image_link)
df_check = df_updated.duplicated(subset=unique_columns, keep=False)

if df_check.any():
    print("Warning: Some duplicates still exist in the table!")
else:
    print("All duplicates successfully removed. The table is now unique at the required level.")

###### Step 8: Save Updated Table for Reference
output_updated_file = os.path.join(data_path, f"chile_institutions_images_cleaned_{selected_env}.csv")
df_updated.to_csv(output_updated_file, index=False)
print(f"Updated table saved at: {output_updated_file}")

###### Step 9: Close Connection
conn.close()
print("Database connection closed.")