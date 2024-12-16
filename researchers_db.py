import pandas as pd
import sqlite3

# Load the CSV file
file_path = "syracuse_university_orcid_data.csv"
data = pd.read_csv(file_path)

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect("researchers.db")
cursor = conn.cursor()

# Drop and create `researchers` table
cursor.execute("DROP TABLE IF EXISTS researchers")
cursor.execute("""
CREATE TABLE IF NOT EXISTS researchers (
    orcid_id TEXT PRIMARY KEY,
    full_name TEXT,
    email TEXT
)
""")

# Drop and create `employment` table
cursor.execute("DROP TABLE IF EXISTS employment")
cursor.execute("""
CREATE TABLE IF NOT EXISTS employment (
    employment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    orcid_id TEXT,
    employment TEXT,
    department TEXT,
    role TEXT,
    start_year INTEGER,
    end_year INTEGER,
    FOREIGN KEY(orcid_id) REFERENCES researchers(orcid_id)
)
""")

# Drop and create `works` table
cursor.execute("DROP TABLE IF EXISTS works")
cursor.execute("""
CREATE TABLE IF NOT EXISTS works (
    work_id INTEGER PRIMARY KEY AUTOINCREMENT,
    orcid_id TEXT,
    work_title TEXT,
    DOI_URL TEXT,
    work_url TEXT,
    FOREIGN KEY(orcid_id) REFERENCES researchers(orcid_id)
)
""")

# Insert data into `researchers` table
researchers_data = data[['orcid_id', 'full_name', 'email']].drop_duplicates()
researchers_data.to_sql('researchers', conn, if_exists='append', index=False)

# Insert data into `employment` table
employment_data = data[['orcid_id', 'employment', 'department', 'role', 'start_year', 'end_year']].drop_duplicates()
employment_data.to_sql('employment', conn, if_exists='append', index=False)

# Insert data into `works` table
works_data = data[['orcid_id', 'work_title', 'DOI_URL', 'work_url']].drop_duplicates()
works_data.to_sql('works', conn, if_exists='append', index=False)

# Commit and close the connection
conn.commit()
conn.close()
print("Data successfully stored in SQLite database.")
