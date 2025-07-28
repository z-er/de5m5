import pandas as pd
import numpy as np

def ingest_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None
    return df

def drop_na_in_essential_columns(df, columns=None):
    if columns is None:
        columns = [col for col in df.columns]
        print(f"Defaulting to checking all columns: {columns}")
    try:
        df.dropna(subset=columns, inplace=True)
    except Exception as e:
        print(f"Error dropping NA values: {e}")
        return None
    return df

def clean_quotes_from_field(df):
    df = df.str.replace('"', '')
    return df

def convert_field_to_datetime(df):
    df = pd.to_datetime(df, errors='coerce', dayfirst=True, format='%d/%m/%Y')
    return df

def convert_field_to_int(df):
    df = df.astype(int)
    return df

def fill_na_with_custom(df, custom_text):
    df = df.fillna(custom_text)
    return df

def systembook_processing(df_systembook):
    df_systembook = drop_na_in_essential_columns(df_systembook)
    df_systembook['Book checkout'] = clean_quotes_from_field(df_systembook['Book checkout'])
    df_systembook['Book checkout'] = convert_field_to_datetime(df_systembook['Book checkout'])
    df_systembook['Book Returned'] = convert_field_to_datetime(df_systembook['Book Returned'])
    df_systembook['Id'] = convert_field_to_int(df_systembook['Id'])
    df_systembook['Customer ID'] = convert_field_to_int(df_systembook['Customer ID'])
    df_systembook['Days allowed to borrow'] = 14 # Quick and dirty, as all are 2 weeks.

    # Add some derived fields: Loan duration, if overdue.
    df_systembook['Loan duration (days)'] = (df_systembook['Book Returned'] - df_systembook['Book checkout']).dt.days
    df_systembook['Overdue'] = np.where((df_systembook['Loan duration (days)'] > df_systembook['Days allowed to borrow']), 'Yes', 'No')

    # Flag date errors
    df_systembook['Date error'] = df_systembook['Loan duration (days)'] < 0

    return df_systembook

def system_customers_processing(df_system_customers):
    df_system_customers = drop_na_in_essential_columns(df_system_customers)
    df_system_customers['Customer ID'] = convert_field_to_int(df_system_customers['Customer ID'])
    return df_system_customers


def push_to_SQL(df, name):
    import sqlalchemy
    from sqlalchemy import create_engine

    # Connection String
    conn_str = (
        "mssql+pyodbc://localhost/LibrarySystem"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
    )
    engine = create_engine(conn_str)
    df.to_sql(name=f'{name}', con=engine, if_exists='replace', index=False)
    print(f'{name} pushed to SQL.')

def main():
    # Extraction
    df_systembook = systembook_processing(ingest_csv_file('python_app\\data\\03_Library Systembook.csv'))
    df_system_customers = system_customers_processing(ingest_csv_file('python_app\\data\\03_Library SystemCustomers.csv'))

    # Transformation
    df_aggregated = pd.merge(df_systembook, df_system_customers, on='Customer ID', how='left')
    df_aggregated['Customer Name'] = fill_na_with_custom(df_aggregated['Customer Name'])

    # Loading
    push_to_SQL(df_aggregated, 'library_records')

main()