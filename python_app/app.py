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
parser.add_argument('-cl', '--cloud', action='store_true', help='flag for pushing to Azure SQL')
parser.add_argument('-tn', '--table-name', help='name of the resultant SQL table')
parser.add_argument('-p', action='store_true', help='flag for printing output of aggregated table')
args = parser.parse_args()

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

def push_to_azure_SQL(df, name):
    access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkpZaEFjVFBNWl9MWDZEQmxPV1E3SG4wTmVYRSIsImtpZCI6IkpZaEFjVFBNWl9MWDZEQmxPV1E3SG4wTmVYRSJ9.eyJhdWQiOiJodHRwczovL2RhdGFiYXNlLndpbmRvd3MubmV0LyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzA2YjliNzk4LTIxMDYtNDhlOS05MTFlLWY5YzA1OWEwNDk1MS8iLCJpYXQiOjE3NTM3ODcwMDEsIm5iZiI6MTc1Mzc4NzAwMSwiZXhwIjoxNzUzNzkyNTM1LCJhY3IiOiIxIiwiYWlvIjoiQVhRQWkvOFpBQUFBZTI3T1BHaXJrQUZzWEgxeG50elBwMzhCcXBtaHV6TVVVamdPZll3RkxnOW5OSzJZYkR2ZDVJS2lWZGliT3gyUmtKek1QcStiKzRwV0JxRDUrMUtzODRQOGVCeWtVLzVHNDBVR3lKTWNEVnlWUytBbGlPaTdlVElXS1dRSXhRYXBjaTFkVkFiN1EybGZ2dDN6UUtoc0dnPT0iLCJhbHRzZWNpZCI6IjE6bGl2ZS5jb206MDAwNjQwMDBCQjgwOTVFMiIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiIwNGIwNzc5NS04ZGRiLTQ2MWEtYmJlZS0wMmY5ZTFiZjdiNDYiLCJhcHBpZGFjciI6IjAiLCJlbWFpbCI6ImplZWtmb2c3QG91dGxvb2suY29tIiwiZmFtaWx5X25hbWUiOiJTcGllcnMiLCJnaXZlbl9uYW1lIjoiSm9zaHVhIiwiZ3JvdXBzIjpbImU0NjA3OTlkLTQ2YTUtNDAyMS05ZTJiLTEzZTVhY2RjMzBkYSJdLCJpZHAiOiJsaXZlLmNvbSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjkyLjIzOC4xNTQuMTU2IiwibmFtZSI6Ikpvc2h1YSBTcGllcnMiLCJvaWQiOiIzZWFmZGEwOS1mNTc5LTRlNDktODcwMC1hOGNkMTU1ODY1YzciLCJwdWlkIjoiMTAwMzIwMDRGMEUxM0E5MiIsInJoIjoiMS5BUk1CbUxlNUJnWWg2VWlSSHZuQVdhQkpVZE1IS1FJYkRfZEl1dHdicHF1cmJXWTZBWjhUQVEuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic2lkIjoiMDA2ZjQ0NzktZjc0NS1lYzUwLTQyMzAtNTVlNWZmYzYyMmViIiwic3ViIjoiVFNmd3RSRzZwOVpNcEdJYUp0N0FHZE5wakMxWGpDOGRsLWxiN25IZlZVRSIsInRpZCI6IjA2YjliNzk4LTIxMDYtNDhlOS05MTFlLWY5YzA1OWEwNDk1MSIsInVuaXF1ZV9uYW1lIjoibGl2ZS5jb20jamVla2ZvZzdAb3V0bG9vay5jb20iLCJ1dGkiOiJDTVVMN2ZhV3RVV1V1ZGZoTmhBaEFBIiwidmVyIjoiMS4wIiwieG1zX2Z0ZCI6ImxRdV9RVEpmRFRobnBSTXVwUnc3QW5mVTJSQ1dsZzRpNmlWQ0lqaEw1RFlCWlhWeWIzQmxibTl5ZEdndFpITnRjdyIsInhtc19pZHJlbCI6IjEgMTIifQ.NAHg9vTsS5tCwFmzZQv1l4YpNQR8GjT1KBADKkqfDPA1MhQidonYH-cVGNaYOJ_BnmPUV8feZeEfYiWFFrn-UTXAfirGBHM3shXiMqIS5p75bt7mVCTmWRME8HehWhCkuUaM3hBlJAaUrrPPf3RKUJPs17IYIJuFQXOFjfcUh_opaON6n2Rs_TWMeVlLWSfEa6Z4bnyNZNkNBNeHU9uePBBojZciKB_FWIFj0v5treowcIUrCeMSvWWAlY4WwkJ12095eBjKgjZXkcWzUCibw352DmvEoOCT2vNDQOXHty1tMGG--mhzYiGCoyrBpHlPVTBPwLd2sLetzTP_wsi0Vg"
    server = 'jjs-qa-testdb.database.windows.net'
    driver = '{ODBC Driver 18 for SQL Server}'
    database = 'db-qa-test'

    token_bytes = bytes(access_token, "utf-16-le")

    conn_str = (
        f"Driver={driver};"
        f"Server=tcp:{server},1433;Database={database};"
        f"Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=30"
    )
    engine = pyodbc.connect(conn_str, attrs_before={1256: token_bytes})
    df.to_sql(name=f'{name}', con=engine, if_exists='replace', index=False)

if __name__ == '__main__':
    # 'python_app\\data\\03_Library Systembook.csv'
    # 'python_app\\data\\03_Library SystemCustomers.csv'
    # Run with: 'python_app\\data\\03_Library Systembook.csv' 'python_app\\data\\03_Library SystemCustomers.csv' -p   

    print(pyodbc.drivers())

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
            push_to_azure_SQL(df_aggregated, 'library_records')
        else:
            push_to_azure_SQL(df_aggregated, f'{args.table_name}')