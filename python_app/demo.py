import data_processing as dp
# 'python_app\\data\\03_Library Systembook.csv'
# 'python_app\\data\\03_Library SystemCustomers.csv'

# Push to Azure SQL function.
def push_to_azure_SQL(df, name):
    # DB-related imports
    from sqlalchemy import create_engine

    # Azure-related imports
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient

    # Key-vault details
    kv_name = "kv-qa-test"
    kv_url = f"https://{kv_name}.vault.azure.net/"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=kv_url, credential=credential)

    # Define all the parts to include in the connection string.
    server = 'jjs-qa-testdb.database.windows.net'
    driver = '{ODBC Driver 18 for SQL Server}' # <-- this will differ between systems.
    database = 'db-qa-test'

    conn_str = (
        f"Driver={driver};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"UID={client.get_secret('db-username').value};"
        f"PWD={client.get_secret('db-password').value};"
        f"Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=30"
    )
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}')

    df.to_sql(name=f'{name}', con=engine, if_exists='replace', index=False)

if __name__ == '__main__':
    # Ingest the files, and perform the processing.
    df_systembook = dp.systembook_processing(dp.ingest_csv_file('python_app\\data\\03_Library Systembook.csv'))
    df_systemcustomers = dp.system_customers_processing(dp.ingest_csv_file('python_app\\data\\03_Library SystemCustomers.csv'))
    df_final = dp.aggregate_processing(df_systembook, df_systemcustomers)

    push_to_azure_SQL(df_final, 'library-qa-demo')