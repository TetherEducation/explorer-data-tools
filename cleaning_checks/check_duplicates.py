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
# Path to credentials file
cred_path = '/Users/ignaciolepe/ConsiliumBots Dropbox/Ignacio Lepe/credentials.csv'
# Data path
data_path = '/Users/ignaciolepe/Documents/duplicates_check/data'
# Duplicates list to check
dup_path = '/Users/ignaciolepe/Documents/duplicates_check/data/resumen_duplicados.xlsx'
######

###### Packages
import os
import pandas as pd
from import_from_back import load_credentials, import_back_tables
###### 

###### Load duplicates checklist
duplicate_checklist = pd.read_excel(dup_path)

###### Load credentials
credentials = load_credentials(cred_path)

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
    try:
        staging_data = import_back_tables(f"{country}.{table_name}", credentials, 'staging')
        if staging_data is not None:
            existe_duplicado_st = staging_data.duplicated(subset=unique_columns, keep=False).any()
    except Exception as e:
        print(f"Error checking duplicates in staging for {table_name}: {e}")

    # Check duplicates in production
    try:
        production_data = import_back_tables(f"{country}.{table_name}", credentials, 'production')
        if production_data is not None:
            existe_duplicado_pr = production_data.duplicated(subset=unique_columns, keep=False).any()
    except Exception as e:
        print(f"Error checking duplicates in production for {table_name}: {e}")

    # Update the checklist
    duplicate_checklist.loc[idx, 'existe_duplicado_st'] = existe_duplicado_st
    duplicate_checklist.loc[idx, 'existe_duplicado_pr'] = existe_duplicado_pr

###### Save updated checklist
output_path = os.path.join(data_path, 'resumen_duplicados_actualizado.xlsx')
duplicate_checklist.to_excel(output_path, index=False)
print(f"\nUpdated checklist saved to {output_path}")