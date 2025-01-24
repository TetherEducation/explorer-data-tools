############################################
##
## update_mayus_labels.py
##
## Provides a function to update labels
## from all mayus to first mayus and the rest minus
##
## Author: Ignacio Lepe
## Created: 2024/01/20
##
############################################

###### Paths
dir = '/Users/ignaciolepe/ConsiliumBots Dropbox/Ignacio Lepe/Explorador_CB/Explorador_Chile/E_Escolar/inputs/2025_01_20'
# Path to credentials file
cred_path = '/Users/ignaciolepe/ConsiliumBots Dropbox/Ignacio Lepe/credentials.csv'
######

###### Packages
import os
import psycopg2
from import_from_back import load_credentials  # Import the credentials function
######

###### Load credentials
credentials = load_credentials(cred_path)

if credentials:
    print("Credentials loaded successfully.")
else:
    print("Failed to load credentials.")
    exit()

###### Generalized Update Function
def update_column_to_proper_case(table_name, column_name, schema_name="public", country="unknown"):
    """
    Generalized function to update a column in a table to ensure proper capitalization.

    Args:
        table_name (str): Name of the table to update.
        column_name (str): Name of the column to transform.
        schema_name (str): Schema containing the table (default: "public").
        country (str): Country context for logging purposes (default: "unknown").
    """
    try:
        # Establish a connection using the loaded credentials
        connection = psycopg2.connect(
            dbname=credentials['dsn_database_tables'],
            user=credentials['dsn_uid_te'],
            password=credentials['dsn_pwd_te_pr'],  # Using production password
            host=credentials['dsn_hostname_te_pr'],  # Using production host
            port=credentials['dsn_port']
        )
        cursor = connection.cursor()

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
        connection.commit()  # Save changes
        print(f"Column {column_name} in table {table_name} updated successfully.")

        # Optional: Verify some rows
        cursor.execute(f"SELECT id, {column_name} FROM {schema_name}.{table_name} LIMIT 5;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    except Exception as e:
        print(f"Error updating the table {schema_name}.{table_name}: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print(f"Database connection for {country} closed.")

# Example calls
update_column_to_proper_case(
    table_name="institutions_infrastructure",
    column_name="descrip",
    schema_name="chile",
    country="Chile"
)

update_column_to_proper_case(
    table_name="school_data",
    column_name="school_name",
    schema_name="colombia",
    country="Colombia"
)