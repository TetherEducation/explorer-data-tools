############################################
##
## payments_consistency.py
##
## Checks that institutions_payment is consistent with institutions_payments_v2 for every unique observation.
##
## Author: Ignacio Lepe
## Created: 2025/01/27
##
############################################

###### Paths
# Path to credentials file
cred_path = '/Users/ignaciolepe/ConsiliumBots Dropbox/Ignacio Lepe/credentials.csv'
# Data path
data_path = '/Users/ignaciolepe/Documents/duplicates_check/data'
######

###### Packages
import os
import pandas as pd
from import_from_back import load_credentials, import_back_tables
######

###### Load credentials
credentials = load_credentials(cred_path)

###### Function to check variable consistency across institutions_payment and institutions_payments_v2

###### Mapping
category_to_band_mapping = {
    1: 1,  # Free
    2: 2,  # "$1.000 a $10.000" -> "$0 a $50.000"
    3: 2,  # "$10.001 a $25.000" -> "$0 a $50.000"
    4: 2,  # "$25.001 a $50.000" -> "$0 a $50.000"
    5: 3,  # "$50.001 a $100.000" -> "$50.000 a $100.000"
    6: 4   # "Más de $100.000" -> "Más de $100.000"
}

def check_payments_consistency(country, table1, table2, credentials, environment="staging"):
    """
    Checks consistency between institutions_payment and institutions_payments_v2.

    Specifically, handles cases where payment_band_id refers to payments (payment_type_id = 2)
    or tuitions (payment_type_id = 1).

    Args:
        country (str): The country to select the tables from.
        table1 (str): The name of the first table (institutions_payment).
        table2 (str): The name of the second table (institutions_payments_v2).
        credentials (dict): Database credentials.
        environment (str): The backend environment ('staging' or 'production').

    Returns:
        None: Prints results and saves inconsistent observations to a CSV file.
    """
    print(f"\nProcessing consistency check for {table1} and {table2} ({country})")

    try:
        # Import the tables
        df1 = import_back_tables(f"{country}.{table1}", credentials, environment)
        df2 = import_back_tables(f"{country}.{table2}", credentials, environment)
        
        if df1 is None or df2 is None:
            print(f"Failed to import one or both tables: {table1}, {table2}")
            return

        # Debug: Print column names
        print(f"Columns in {table1}: {df1.columns}")
        print(f"Columns in {table2}: {df2.columns}")

        # Check if required columns exist
        required_columns_df1 = ['payment_category_label_id', 'tuition_category_label_id']
        required_columns_df2 = ['payment_band_id', 'payment_type_id']

        for col in required_columns_df1:
            if col not in df1.columns:
                print(f"'{col}' not found in {table1}. Ensure the column exists.")
                return

        for col in required_columns_df2:
            if col not in df2.columns:
                print(f"'{col}' not found in {table2}. Ensure the column exists.")
                return

        # Map payment_category_label_id and tuition_category_label_id to equivalent payment_band_id
        df1['mapped_payment_band_id'] = df1['payment_category_label_id'].map(category_to_band_mapping)
        df1['mapped_tuition_band_id'] = df1['tuition_category_label_id'].map(category_to_band_mapping)

        # Merge the two tables on the unique identifier but include payment_type_id from df2
        merged_df = pd.merge(
            df1,
            df2[['campus_code', 'institution_code', 'year', 'payment_band_id', 'payment_type_id']],
            on=["campus_code", "institution_code", "year"],
            how="inner",
            suffixes=('_t1', '_t2')
        )

        # Separate checks based on payment_type_id
        payment_type_1_df = merged_df[merged_df['payment_type_id'] == 1]
        payment_type_2_df = merged_df[merged_df['payment_type_id'] == 2]

        # Rule 1: Check for tuition consistency (payment_type_id = 1)
        payment_type_1_df['rule_consistent'] = (
            payment_type_1_df['mapped_tuition_band_id'] == payment_type_1_df['payment_band_id']
        )
        inconsistent_type_1 = payment_type_1_df[~payment_type_1_df['rule_consistent']]

        # Rule 2: Check for payment consistency (payment_type_id = 2)
        payment_type_2_df['rule_consistent'] = (
            payment_type_2_df['mapped_payment_band_id'] == payment_type_2_df['payment_band_id']
        )
        inconsistent_type_2 = payment_type_2_df[~payment_type_2_df['rule_consistent']]

        # Print and save results
        if inconsistent_type_1.empty and inconsistent_type_2.empty:
            print("All observations are consistent.")
        else:
            if not inconsistent_type_1.empty:
                print("\nInconsistent observations for tuition (payment_type_id = 1):")
                print(inconsistent_type_1[[
                    "campus_code", "institution_code", "year",
                    "tuition_category_label_id", "mapped_tuition_band_id", "payment_band_id"
                ]])
                output_path_type_1 = os.path.join(data_path, f"{table1}_{table2}_tuition_inconsistencies_{environment}.csv")
                inconsistent_type_1.to_csv(output_path_type_1, index=False, encoding='utf-8')
                print(f"Inconsistencies for tuition saved to {output_path_type_1}")

            if not inconsistent_type_2.empty:
                print("\nInconsistent observations for payment (payment_type_id = 2):")
                print(inconsistent_type_2[[
                    "campus_code", "institution_code", "year",
                    "payment_category_label_id", "mapped_payment_band_id", "payment_band_id"
                ]])
                output_path_type_2 = os.path.join(data_path, f"{table1}_{table2}_payment_inconsistencies_{environment}.csv")
                inconsistent_type_2.to_csv(output_path_type_2, index=False, encoding='utf-8')
                print(f"Inconsistencies for payment saved to {output_path_type_2}")

    except Exception as e:
        print(f"Error processing tables {table1} and {table2}: {e}")

###### Example Usage
# Parameters
country = "chile"

# Table 1: institutions_payments
table1 = "institutions_payment"

# Table 2: institutions_payments_v2
table2 = "institutions_payments_v2"

# Check consistency for staging environment
check_payments_consistency(
    country=country,
    table1=table1,
    table2=table2,
    credentials=credentials,
    environment="staging"
)

# Check consistency for production environment
check_payments_consistency(
    country=country,
    table1=table1,
    table2=table2,
    credentials=credentials,
    environment="production"
)