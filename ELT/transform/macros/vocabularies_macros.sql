
{% macro load_vocabularies(path, schema_name="vocabularies") %}
import pandas as pd
import os
from sqlalchemy import create_engine

# Fetch database connection details from dbt profile
database = target['dbname']
user = target['user']
password = target['pass']
host = target['host']
port = target['port']

# Create a connection engine using SQLAlchemy
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# Function to load CSV file in chunks
def load_csv_in_chunks(file_path, table_name, schema_name, chunk_size=10000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False)

# Get list of CSV files in the specified path
csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]

# Iterate over each CSV file and load it into the respective table
for csv_file in csv_files:
    print(csv_file)
    table_name = os.path.splitext(csv_file)[0].lower()  # Extract table name from file name
    file_path = os.path.join(path, csv_file)
    load_csv_in_chunks(file_path, table_name, schema_name)

{% endmacro %}
