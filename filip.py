import csv
import psycopg2
from psycopg2 import sql

def csv_to_postgresql(csv_filepath, table_name, db_params):
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # First, read the CSV to determine column structure (after skipping header)
    with open(csv_filepath, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        
        # Skip header row
        next(csv_reader)
        
        # Read first data row to determine number of columns
        first_row = next(csv_reader)
        
        # Create table schema (excluding second column)
        # Column 1: timestamp, Column 3: text, Others: float
        columns = []
        columns.append(sql.SQL("timestamp_col TIMESTAMP"))  # First column - timestamp
        
        # Add remaining columns (skip second column)
        for i in range(2, len(first_row)):
            if i == 2:  # Third column (index 2) should be text
                columns.append(sql.SQL("col_{} TEXT").format(sql.Identifier(str(i))))
            else:
                columns.append(sql.SQL("col_{} FLOAT").format(sql.Identifier(str(i))))
        
        # Create table
        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(columns)
        )
        
        cursor.execute(create_table_query)
        
        # Reset file for data import
        csvfile.seek(0)
        next(csv_reader)  # Skip header again
        
        # Process each row and insert into PostgreSQL
        for row in csv_reader:
            # Process row according to requirements
            processed_row = []
            processed_row.append(row[0])  # First column (timestamp)
            
            # Skip second column (index 1)
            # Process remaining columns
            for i in range(2, len(row)):
                if i == 2:  # Third column (now second in our database) as string
                    processed_row.append(row[i])
                else:
                    try:
                        processed_row.append(float(row[i]))
                    except (ValueError, TypeError):
                        processed_row.append(None)  # Handle invalid float values
            
            # Create column names for insert statement (matching our table schema)
            col_names = ["timestamp_col"] + [f"col_{i}" for i in range(2, len(first_row))]
            
            # Insert data
            placeholders = ", ".join(["%s"] * len(processed_row))
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, col_names)),
                sql.SQL(placeholders)
            )
            
            cursor.execute(insert_query, processed_row)
    
    # Commit changes and clean up
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Successfully imported data from {csv_filepath} to table {table_name}")

# Example usage
db_params = {
    "dbname": "your_database",
    "user": "your_username",
    "password": "your_password",
    "host": "localhost",
    "port": "5432"
}

csv_to_postgresql("your_data.csv", "your_table_name", db_params
                  )
