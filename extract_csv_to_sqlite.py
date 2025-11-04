import sqlite3
import csv
import os

# Database file name
DB_FILE = 'university_data.db'

# CSV files
CSV_FILES = {
    'departments': 'Data/Department_Information.csv',
    'employees': 'Data/Employee_Information.csv',
    'student_counseling': 'Data/Student_Counceling_Information.csv',
    'student_performance': 'Data/Student_Performance_Data.csv'
}

def create_connection():
    """Create a database connection"""
    conn = sqlite3.connect(DB_FILE)
    return conn

def create_tables(conn):
    """Create tables for each CSV file"""
    cursor = conn.cursor()
    
    # Drop existing tables if they exist (to recreate with correct schema)
    cursor.execute('DROP TABLE IF EXISTS student_performance')
    cursor.execute('DROP TABLE IF EXISTS student_counseling')
    cursor.execute('DROP TABLE IF EXISTS employees')
    cursor.execute('DROP TABLE IF EXISTS departments')
    
    # Department table - no primary key to allow duplicates
    cursor.execute('''
        CREATE TABLE departments (
            "Department_ID" TEXT,
            "Department_Name" TEXT,
            "DOE" TEXT
        )
    ''')
    
    # Employee table - no primary key to allow duplicates
    cursor.execute('''
        CREATE TABLE employees (
            "Employee ID" TEXT,
            "DOB" TEXT,
            "DOJ" TEXT,
            "Department_ID" TEXT
        )
    ''')
    
    # Student Counseling table - no primary key to allow duplicates
    cursor.execute('''
        CREATE TABLE student_counseling (
            "Student_ID" TEXT,
            "DOA" TEXT,
            "DOB" TEXT,
            "Department_Choices" TEXT,
            "Department_Admission" TEXT
        )
    ''')
    
    # Student Performance table - note: "Semster_Name" is misspelled in CSV
    cursor.execute('''
        CREATE TABLE student_performance (
            "Student_ID" TEXT,
            "Semster_Name" TEXT,
            "Paper_ID" TEXT,
            "Paper_Name" TEXT,
            "Marks" INTEGER,
            "Effort_Hours" INTEGER
        )
    ''')
    
    conn.commit()
    print("Tables created successfully!")

def import_csv_to_table(conn, csv_file, table_name):
    """Import data from CSV file to SQLite table"""
    cursor = conn.cursor()
    
    with open(csv_file, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        # Get column names from CSV
        columns = csv_reader.fieldnames
        
        # Prepare insert statement - quote column names to handle spaces
        placeholders = ','.join(['?' for _ in columns])
        column_names = ','.join([f'"{col}"' for col in columns])  # Quote column names
        
        # Use INSERT to preserve all records including duplicates
        insert_query = f'INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})'
        
        row_count = 0
        for row in csv_reader:
            values = [row[col] for col in columns]
            cursor.execute(insert_query, values)
            row_count += 1
        
        conn.commit()
        print(f"Imported {row_count} rows from {csv_file} to {table_name} table")

def main():
    """Main function to extract CSV data to SQLite"""
    # Create connection
    conn = create_connection()
    
    try:
        # Create tables
        create_tables(conn)
        
        # Import each CSV file
        import_csv_to_table(conn, CSV_FILES['departments'], 'departments')
        import_csv_to_table(conn, CSV_FILES['employees'], 'employees')
        import_csv_to_table(conn, CSV_FILES['student_counseling'], 'student_counseling')
        import_csv_to_table(conn, CSV_FILES['student_performance'], 'student_performance')
        
        # Display summary
        cursor = conn.cursor()
        print("\n=== Database Summary ===")
        for table in ['departments', 'employees', 'student_counseling', 'student_performance']:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            print(f"{table}: {count} rows")
        
        print(f"\nDatabase '{DB_FILE}' created successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()