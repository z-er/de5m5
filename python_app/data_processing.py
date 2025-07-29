import pandas as pd
import numpy as np
import argparse as ap
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine

parser = ap.ArgumentParser(
    prog='DataProcessorJS',
    description='This program processes the library system CSV files and pushes an aggregated table to SQL.'
)

parser.add_argument('sysbpath', help='path to systembook csv')
parser.add_argument('syscpath', help='path to systemcustomers csv')
parser.add_argument('-sql', action='store_true', help='flag for pushing to SQL')
parser.add_argument('-u', '--username', help='entering a username for Azure SQL')
parser.add_argument('-s', '--secret', help='entering a secret for Azure SQL')
parser.add_argument('-cl', '--cloud', action='store_true', help='flag for pushing to Azure SQL')
parser.add_argument('-tn', '--table-name', help='name of the resultant SQL table')
parser.add_argument('-p', action='store_true', help='flag for printing output of aggregated table')
args = parser.parse_args()

if args.cloud:
    if (args.username is None) or (args.secret is None):
        print("Please provide a username/secret for SQL Authentication with -u/--username and -s/--secret.")
        exit

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

def enrich_duration(df):
    df['Loan duration (days)'] = (df['Book Returned'] - df['Book checkout']).dt.days
    return df['Loan duration (days)']

def enrich_overdue(df):
    df['Overdue'] = np.where((df['Loan duration (days)'] > df['Days allowed to borrow']), 'Yes', 'No')
    return df['Overdue']

def enrich_date_error_flag(df):
    df['Date error'] = df['Loan duration (days)'] < 0
    return df['Date error']

def systembook_processing(df_systembook):
    df_systembook = drop_na_in_essential_columns(df_systembook)
    df_systembook['Book checkout'] = clean_quotes_from_field(df_systembook['Book checkout'])
    df_systembook['Book checkout'] = convert_field_to_datetime(df_systembook['Book checkout'])
    df_systembook['Book Returned'] = convert_field_to_datetime(df_systembook['Book Returned'])
    df_systembook['Id'] = convert_field_to_int(df_systembook['Id'])
    df_systembook['Customer ID'] = convert_field_to_int(df_systembook['Customer ID'])
    df_systembook['Days allowed to borrow'] = 14 # Quick and dirty, as all are 2 weeks.
    df_systembook['Loan duration (days)'] = enrich_duration(df_systembook)
    df_systembook['Overdue'] = enrich_overdue(df_systembook)
    df_systembook['Date error'] = enrich_date_error_flag(df_systembook)
    return df_systembook

def system_customers_processing(df_system_customers):
    df_system_customers = drop_na_in_essential_columns(df_system_customers)
    df_system_customers['Customer ID'] = convert_field_to_int(df_system_customers['Customer ID'])
    return df_system_customers

def aggregate_processing(df1, df2):
    df = pd.merge(df1, df2, on='Customer ID', how='left')
    df['Customer Name'] = fill_na_with_custom(df['Customer Name'], 'Unknown Customer')
    return df

def push_to_SQL(df, name):
    # Connection String
    conn_str = (
        "mssql+pyodbc://localhost/LibrarySystem"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
    )
    engine = create_engine(conn_str)
    df.to_sql(name=f'{name}', con=engine, if_exists='replace', index=False)
    print(f'{name} pushed to SQL.')

def push_to_azure_SQL(df, name, username, password):
    server = 'jjs-qa-testdb.database.windows.net'
    driver = '{ODBC Driver 18 for SQL Server}'
    database = 'db-qa-test'

    conn_str = (
        f"Driver={driver};"
        f"Server=tcp:{server},1433;Database={database};"
        f"UID={username};PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=30"
    )
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}')
    df.to_sql(name=f'{name}', con=engine, if_exists='replace', index=False)

if __name__ == '__main__':
    # 'python_app\\data\\03_Library Systembook.csv'
    # 'python_app\\data\\03_Library SystemCustomers.csv'
    # Extraction
    df_systembook = systembook_processing(ingest_csv_file(args.sysbpath))
    df_system_customers = system_customers_processing(ingest_csv_file(args.syscpath))

    # Transformation
    df_aggregated = pd.merge(df_systembook, df_system_customers, on='Customer ID', how='left')
    df_aggregated['Customer Name'] = fill_na_with_custom(df_aggregated['Customer Name'], 'Unknown Customer')

    if args.p:
        print(df_aggregated.head(10))

    # Loading
    if args.sql:
        if args.table_name is None:
            push_to_SQL(df_aggregated, 'library_records')
        else:
            push_to_SQL(df_aggregated, f'{args.table_name}')
    
    if args.cloud:
        if args.table_name is None:
            push_to_azure_SQL(df_aggregated, 'library_records', args.username, args.secret)
        else:
            push_to_azure_SQL(df_aggregated, f'{args.table_name}', args.username, args.secret)