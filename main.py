import pandas as pd
import psycopg2
from psycopg2 import sql
import os

# Database connection parameters
DB_PARAMS = {
    "dbname": "postgres",
    "user": "kurciqs",
    "password": "password",
    "host": "localhost",  # Change if necessary
    "port": 5432,         # Change if necessary
}

import re

def format_table_name(filename):
    # Remove "fp_" prefix if present
    filename = filename.lstrip("fp_")

    # Split into parts
    parts = filename.split("_")

    # Extract date (YYYY_MM_DD) and time (HH_MM_SS)
    date_part = "_".join(parts[:3])  # First 3 parts are the date
    time_part = "_".join(parts[3:6])  # Next 3 parts are the time
    extra_part = "_".join(parts[6:])  # Remaining parts (e.g., t25)

    # Construct the new table name
    formatted_name = f"fp__{date_part}__{time_part}_{extra_part}"

    # Ensure the table name is PostgreSQL-safe
    formatted_name = re.sub(r'\W+', '_', formatted_name.lower())

    return formatted_name

def store_fp_file_in_db(csv_file):
    df = pd.read_csv(csv_file, skiprows=[0], header=None, sep=";", low_memory=False) 
    # Set column names
    # Convert data types
    #df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])  # Convert first column (timestamps)
    df.iloc[2:, 2:] = df.iloc[2:, 2:].astype(float)   # Convert remaining columns to float

    df = df.drop(df.columns[1], axis=1)  # Drop the second column from the original DataFrame

    column_names = df.iloc[0].tolist()
    print(column_names)
    timestamps = df.iloc[:, 0].tolist()

    print(df)

    # Define table name
    table_name = "fp_" + csv_file[-26:-7].replace("-", "_")
    table_name = format_table_name(table_name)

    if "23510000" in csv_file:
        table_name += "_t25"
    elif "24400210" in csv_file:
        table_name += "_t10"
    print(table_name)

    # Create database connection
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    # Create table dynamically
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "{column_names[0]}" TIMESTAMP PRIMARY KEY,
        {", ".join(f'"{col}" DOUBLE PRECISION' for col in column_names[1:])}
    );
    """

    cur.execute(create_table_query)
    conn.commit()


    # Insert data
    insert_query = sql.SQL(f"""
        INSERT INTO {table_name} ({", ".join(f'"{col}"' for col in column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})
        ON CONFLICT ("{column_names[0]}") DO NOTHING;
    """)


    for i, row in enumerate(df.itertuples(index=False, name=None)):
        if i == 0:
            continue
        cur.execute(insert_query, row)
    
    conn.commit()
    cur.close()
    conn.close()

    print(f"Data from file {csv_file} inserted successfully!")


def store_par_file_in_db(csv_file):
    df = pd.read_csv(csv_file, skiprows=[0], header=None, sep=";", low_memory=False)
    df = df.iloc[:, ::2]
    df.iloc[2:, 2:] = df.iloc[2:, 2:].astype(float)   # Convert remaining columns to float
    column_names = df.iloc[0].tolist()
    timestamps = df.iloc[:, 0].tolist()
    
    print(column_names)
    print(df)
    
    table_name = "par_"

    # Extract date and time
    date_match = re.search(r'\d{4}-\d{2}-\d{2}', csv_file)
    time_match = re.search(r'\d{2}-\d{2}-\d{2}', csv_file)

    if date_match and time_match:
        date_str = date_match.group().replace('-', '_')
        time_str = time_match.group().replace('-', '_')
        table_name += f"{date_str}_{time_str}"


    print(table_name)
    # Create database connection
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    # Create table dynamically
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "{column_names[0]}" TIMESTAMP PRIMARY KEY,
        {", ".join(f'"{col}" DOUBLE PRECISION' for col in column_names[1:])}
    );
    """

    cur.execute(create_table_query)
    conn.commit()


    # Insert data
    insert_query = sql.SQL(f"""
        INSERT INTO {table_name} ({", ".join(f'"{col}"' for col in column_names)})
        VALUES ({", ".join(["%s"] * len(column_names))})
        ON CONFLICT ("{column_names[0]}") DO NOTHING;
    """)


    for i, row in enumerate(df.itertuples(index=False, name=None)):
        if i == 0:
            continue
        cur.execute(insert_query, row)
    
    conn.commit()
    cur.close()
    conn.close()

    print(f"Data from file {csv_file} inserted successfully!")

def get_all_file_paths(directory):
    # Get absolute paths of all files in the specified directory
    file_paths = [os.path.abspath(os.path.join(directory, file)) 
                  for file in os.listdir(directory) 
                  if os.path.isfile(os.path.join(directory, file))]
    return file_paths

directory_path = './'
all_files = get_all_file_paths(directory_path)
fp_files = []
for file in all_files:
    if file.endswith("_fp.csv"):
        fp_files.append(file)

#print(fp_files)
fp_files = []
for file in fp_files:
    store_fp_file_in_db(file)



store_par_file_in_db("PannoniaShrimp_2024-09-04_09-34-00_par.csv")
