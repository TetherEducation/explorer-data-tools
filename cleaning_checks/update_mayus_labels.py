############################################
##
## update_mayus_labels.py
##
## Provides a function to update labels
## from all uppercase to first letter uppercase and the rest lowercase.
##
## Author: Ignacio Lepe
## Created: 2024/01/20
##
############################################

###### Paths
dir = '/Users/ignaciolepe/ConsiliumBots Dropbox/Ignacio Lepe/Explorador_CB/Explorador_Chile/E_Escolar/inputs/2025_01_20'
######

###### Packages
import os
from utils.db_connection import conect_bd
######

###### Function to Update Column to Proper Case
def update_column_to_proper_case(conn, table_name, column_name, schema_name="public", country="unknown"):
    """
    Generalized function to update a column in a table to ensure proper capitalization.

    Args:
        conn (psycopg2 connection): Active database connection.
        table_name (str): Name of the table to update.
        column_name (str): Name of the column to transform.
        schema_name (str): Schema containing the table (default: "public").
        country (str): Country context for logging purposes (default: "unknown").
    """
    try:
        cursor = conn.cursor()

        # Log the action
        print(f"Updating {schema_name}.{table_name}.{column_name} for {country}...")

        # Transformation query
        update_query = f"""
        UPDATE {schema_name}.{table_name}
        SET {column_name} = CONCAT(
            UPPER(LEFT(LOWER({column_name}), 1)),
            SUBSTRING(LOWER({column_name}), 2)
        );
        """

        # Execute the update query
        cursor.execute(update_query)
        conn.commit()  # Save changes
        print(f"Column {column_name} in table {table_name} updated successfully.")

        # Optional: Verify some rows
        cursor.execute(f"SELECT id, {column_name} FROM {schema_name}.{table_name} LIMIT 5;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    except Exception as e:
        print(f"Error updating the table {schema_name}.{table_name}: {e}")
    finally:
        if cursor:
            cursor.close()

###### Connect to the Database
environment = 'production'  # Change to 'staging' if needed
conn = conect_bd('core', environment)

###### Example Calls
update_column_to_proper_case(
    conn=conn,
    table_name="institutions_infrastructure",
    column_name="descrip",
    schema_name="chile",
    country="Chile"
)

update_column_to_proper_case(
    conn=conn,
    table_name="school_data",
    column_name="school_name",
    schema_name="colombia",
    country="Colombia"
)

###### Close Connection
conn.close()
print("Database connection closed.")