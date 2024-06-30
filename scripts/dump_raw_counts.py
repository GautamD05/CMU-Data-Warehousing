import os
import duckdb

# Print the current working directory
print("Current working directory:", os.getcwd())

# Connect to the DuckDB database with the full path
db_path = 'C:\\Users\\gauta\\Data Warehousing\\Repo\\CMU-Data-Warehousing\\main.db'
con = duckdb.connect(db_path)
print("Connected to database:", db_path)

# Get the list of tables
tables = con.execute("SHOW TABLES").fetchall()
print("Tables:", tables)

# Full path to the output file
output_path = os.path.join('C:\\Users\\gauta\\Data Warehousing\\Repo\\CMU-Data-Warehousing\\answers', 'raw_counts.txt')
print("Output file path:", output_path)

# Open the output file
with open(output_path, 'w') as f:
    # For each table, get the row count
    for table in tables:
        table_name = table[0]
        print(f"Processing table: {table_name}")
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Table: {table_name}, Row count: {row_count}")
        # Write the table name and row count to the file
        f.write(f"{table_name}: {row_count}\n")

print("Row counts written to", output_path)
