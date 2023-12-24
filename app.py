#--------------------------------------------------------------------------------------------------------------------------
# This code will set up a basic Flask project structure and then interact with a PostegreSQL database through psycopg2 API
#--------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------
# Author: Igor Cleto S. De Araujo
# Version: 0.0.1
#--------------------------------------------------------------------------------------------------------------------------

# Importing necessary libraries
from flask import Flask, request, jsonify
import csv
import os
import signal
import threading
import time
import psycopg2
from psycopg2 import sql

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "raw"
DB_USER = "postgres"
DB_PASS = "admin"
DB_PORT = "5433"

#--------------------------------------------------------------------------------------------------------------------------
# FLASK DATA INGESTION API
#--------------------------------------------------------------------------------------------------------------------------

# Initialize the Flask application
app = Flask(__name__)

#--------------------------------------------------------------------------------------------------------------------------
# Create the API Endpoints
@app.route('/upload-csv', methods=['POST'])
def upload_csv():

    # Check if a file is part of the request
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    # If the user does not select a file, the browser submits an empty file without a filename.
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and file.filename.endswith('.csv'):
        # Parse the CSV file
        try:
            # Temporary save the file
            file_path = os.path.join('temp', file.filename)
            file.save(file_path)
            table_name = os.path.splitext(file.filename)[0]  # Table name is the filename without extension

            # Create the postegresql RAW Database
            create_database_if_not_exists(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)

            # Insert data into the RAW Database
            create_table_and_insert_data(DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT, file_path, table_name)

            # Cleanup
            os.remove(file_path)
            return jsonify({'message': 'File uploaded and saved to database successfully'}), 200

        except Exception as e:
            return jsonify({'error': str(e)}), 500

    else:
        return jsonify({'error': 'Invalid file format'}), 400
    
    
def create_database_if_not_exists(db_user, db_pass, db_host, db_port, target_db):
    """
    Connects to the PostgreSQL server and creates the target database if it does not exist.

    :param db_user: Database username
    :param db_pass: Database password
    :param db_host: Database host address
    :param db_port: Database port
    :param target_db: Name of the database to check and create if it doesn't exist
    """

    conn = psycopg2.connect(user=db_user, password=db_pass, host=db_host, port=db_port, dbname='postgres')
    conn.autocommit = True

    with conn.cursor() as cursor:
        # Check if the target database exists
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (target_db,))
        exists = cursor.fetchone()

        # If the database does not exist, create it
        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))

    conn.close()

def create_table_and_insert_data(db_name, db_user, db_pass, db_host, db_port, file_path, file_name):
    """
    Creates a table in the PostgreSQL database based on the CSV file's headers and inserts data.

    :param db_name: Database name
    :param db_user: Database username
    :param db_pass: Database password
    :param db_host: Database host address
    :param db_port: Database port
    :param file_path: Path to the CSV file
    :param file_name: Name of the file (used to name the table)
    """

    # Connect to the database
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    conn.autocommit = True

    with conn.cursor() as cursor, open(file_path, mode='r') as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader)  # Assuming the first row is the header
        table_name = os.path.splitext(file_name)[0]  # Table name is the filename without extension

        # Create table
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
        cursor.execute(sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join([sql.Identifier(header) + sql.SQL(' VARCHAR') for header in headers])
        ))

        # Insert data
        for row in reader:
            placeholders = sql.SQL(', ').join(sql.Placeholder() * len(row))
            insert_query = sql.SQL("INSERT INTO {} VALUES ({})").format(sql.Identifier(table_name), placeholders)
            cursor.execute(insert_query, row)

    conn.close()

#--------------------------------------------------------------------------------------------------------------------------

@app.route('/restart-server', methods=['POST'])
def trigger_restart():
    # Start a separate thread to restart the server
    threading.Thread(target=restart_server).start()
    return jsonify({'message': 'Server restarting...'}), 200

def restart_server():
    time.sleep(1)  # Short delay to ensure the response is sent
    os.kill(os.getpid(), signal.SIGINT)

#--------------------------------------------------------------------------------------------------------------------------
    
# Run the Flask app
if __name__ == '__main__':
    app.run(port=8000,debug=True)