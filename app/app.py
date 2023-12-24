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
import json
from flask_socketio import SocketIO

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "raw"
DB_USER = "postgres"
DB_PASS = "admin"
DB_PORT = "5433"

#--------------------------------------------------------------------------------------------------------------------------
# Methods
#--------------------------------------------------------------------------------------------------------------------------

def create_database_if_not_exists(db_name, db_user, db_pass, db_host, db_port):
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
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (db_name,))
        exists = cursor.fetchone()

        # If the database does not exist, create it
        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    conn.close()

def create_table_and_insert_data(db_name, db_user, db_pass, db_host, db_port, file_path, file_name, overwrite=True):
    """
    Creates a table in the PostgreSQL database based on the CSV file's headers and inserts data.
    Can optionally overwrite the existing table or append to it.

    :param db_name: Database name
    :param db_user: Database username
    :param db_pass: Database password
    :param db_host: Database host address
    :param db_port: Database port
    :param file_path: Path to the CSV file
    :param file_name: Name of the file (used to name the table)
    :param overwrite: If True, overwrite existing table; if False, append to it
    """

    # Connect to the database
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    conn.autocommit = True

    with conn.cursor() as cursor, open(file_path, mode='r') as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader)  # Assuming the first row is the header
        table_name = os.path.splitext(file_name)[0]  # Table name is the filename without extension

        if overwrite:
            # Drop the table if it exists and then create it
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

def transform_data_to_bronze(raw_db_name, raw_table_name, db_user, db_pass, db_host, db_port, bronze_db_name='bronze'):
    """
    Transforms data from the 'raw' database table and loads it into the 'bronze' database.

    :param raw_db_name: Name of the 'raw' database
    :param bronze_db_name: Name of the 'bronze' database
    :param db_user: Database username
    :param db_pass: Database password
    :param db_host: Database host address
    :param db_port: Database port
    :param raw_table_name: Name of the table in the 'raw' database to transform
    """

    # Connect to the raw database
    conn_raw = psycopg2.connect(dbname=raw_db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    conn_raw.autocommit = True

    # Connect to the bronze database
    conn_bronze = psycopg2.connect(dbname=bronze_db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    conn_bronze.autocommit = True

    with conn_raw.cursor() as cursor_raw, conn_bronze.cursor() as cursor_bronze:
        # Execute CTE on raw database
        cte_query = sql.SQL("""
            WITH cte AS (
                SELECT
                    CAST(SPLIT_PART(datetime, ' ', 1) AS DATE) AS date,
                    CAST(SPLIT_PART(datetime, ' ', 2) AS TIME) AS time,
                    CAST(TRIM(SPLIT_PART(SPLIT_PART(origin_coord, '(', 2), ' ', 1)) AS FLOAT) AS origin_latitude,
                    CAST(TRIM(SPLIT_PART(SPLIT_PART(origin_coord, '(', 2), ' ', 2), ')') AS FLOAT) AS origin_longitude,
                    CAST(TRIM(SPLIT_PART(SPLIT_PART(destination_coord, '(', 2), ' ', 1)) AS FLOAT) AS destination_latitude,
                    CAST(TRIM(SPLIT_PART(SPLIT_PART(destination_coord, '(', 2), ' ', 2), ')') AS FLOAT) AS destination_longitude,
                    UPPER(region) AS region,
                    UPPER(datasource) AS datasource
                FROM {}
            )
            SELECT * FROM cte;
        """).format(sql.Identifier(raw_table_name))

        cursor_raw.execute(cte_query)

        # Fetch the transformed data
        transformed_data = cursor_raw.fetchall()

        # Define the structure of the bronze table
        cursor_bronze.execute("""
            CREATE TABLE IF NOT EXISTS bronze_trips (
                date DATE,
                time TIME,
                origin_latitude FLOAT,
                origin_longitude FLOAT,
                destination_latitude FLOAT,
                destination_longitude FLOAT,
                region VARCHAR,
                datasource VARCHAR
            );
        """)

        # Insert the transformed data into the bronze table
        insert_query = """
            INSERT INTO bronze_trips (
                date, time, origin_latitude, origin_longitude, 
                destination_latitude, destination_longitude, region, datasource
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor_bronze.executemany(insert_query, transformed_data)

    # Close the connections
    conn_raw.close()
    conn_bronze.close()

def transform_data_to_silver(db_user, db_pass, db_host, db_port, bronze_table_name, silver_table_name, silver_db_name='silver', bronze_db_name='bronze'):
    """
    Groups trips by similar origin, destination, and time of day from the bronze database and saves the result to the silver database.

    :param db_user: Database username
    :param db_pass: Database password
    :param db_host: Database host address
    :param db_port: Database port
    :param bronze_db_name: Name of the bronze database
    :param bronze_table_name: Name of the table in the bronze database
    :param silver_db_name: Name of the silver database
    :param silver_table_name: Name of the table to create in the silver database
    """

    # Connect to the bronze database
    conn_bronze = psycopg2.connect(dbname=bronze_db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    conn_bronze.autocommit = True

    # Connect to the silver database
    conn_silver = psycopg2.connect(dbname=silver_db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    conn_silver.autocommit = True

    with conn_bronze.cursor() as cursor_bronze:
        # Assuming columns for origin, destination, and time are named 'origin', 'destination', and 'time' respectively
        cursor_bronze.execute("""
            SELECT
                origin_latitude,
                origin_longitude,
                destination_latitude,
                destination_longitude,
                EXTRACT(HOUR FROM time) AS hour_of_day,
                COUNT(*) AS trip_count
            FROM {}
            GROUP BY origin_latitude, origin_longitude, destination_latitude, destination_longitude, hour_of_day;
        """.format(bronze_table_name))

        result = cursor_bronze.fetchall()

    conn_bronze.close()

    with conn_silver.cursor() as cursor_silver:
        # Create the table in the silver database
        cursor_silver.execute("""
            CREATE TABLE IF NOT EXISTS {} (
                origin_latitude FLOAT,
                origin_longitude FLOAT,
                destination_latitude FLOAT,
                destination_longitude FLOAT,
                hour_of_day INT,
                trip_count INT
            );
        """.format(silver_table_name))

        # Insert the grouped data into the silver table
        cursor_silver.executemany("""
            INSERT INTO {} (origin_latitude, origin_longitude, destination_latitude, destination_longitude, hour_of_day, trip_count)
            VALUES (%s, %s, %s, %s,  %s,  %s)
        """.format(silver_table_name), result)

    conn_silver.close()

def fetch_weekly_average_trips(db_user, db_pass, db_host, db_port, db_name, table_name, min_lat=None, max_lat=None, min_lon=None, max_lon=None, region=None):
    """
    Calculates and fetches the weekly average number of trips from the specified table.
    The data can be filtered by either a bounding box (coordinates) or a region.

    :param db_user: Database username
    :param db_pass: Database password
    :param db_host: Database host address
    :param db_port: Database port
    :param bronze_db_name: Name of the bronze database
    :param bronze_table_name: Name of the table in the bronze database
    :param min_lat: Minimum latitude of the bounding box (optional)
    :param max_lat: Maximum latitude of the bounding box (optional)
    :param min_lon: Minimum longitude of the bounding box (optional)
    :param max_lon: Maximum longitude of the bounding box (optional)
    :param region: Name of the region (optional)
    :return: JSON payload of the data
    """

    # Connect to the bronze database
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)

    try:
        with conn.cursor() as cursor:
            # Construct the WHERE clause based on provided parameters
            where_clause = ""
            if region:
                where_clause = "WHERE UPPER(region) = UPPER(%s)"
                params = (region,)
            elif all([min_lat, max_lat, min_lon, max_lon]):
                where_clause = "WHERE origin_latitude BETWEEN %s AND %s AND origin_longitude BETWEEN %s AND %s"
                params = (min_lat, max_lat, min_lon, max_lon)
            else:
                raise ValueError("Either region or bounding box coordinates must be provided")

            # Execute the query
            cursor.execute(sql.SQL("""
                SELECT
                    DATE_TRUNC('week', date) AS week,
                    COUNT(*) / COUNT(DISTINCT DATE_TRUNC('week', date)) AS weekly_avg_trips
                FROM {}
                {}
                GROUP BY week;
            """).format(sql.Identifier(table_name), sql.SQL(where_clause)), params)

            # Fetch the result and convert to JSON
            result = cursor.fetchall()
            result_json = json.dumps([{"week": row[0].strftime("%Y-%m-%d"), "weekly_avg_trips": row[1]} for row in result])

            return result_json

    except Exception as e:
        raise e

    finally:
        conn.close()

def restart_server():
    time.sleep(1)  # Short delay to ensure the response is sent
    os.kill(os.getpid(), signal.SIGINT)

#--------------------------------------------------------------------------------------------------------------------------
# Flask Data Ingestion API
#--------------------------------------------------------------------------------------------------------------------------

# Initialize the Flask application and socket status checking
app = Flask(__name__)
socketio = SocketIO(app)

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
            socketio.emit('data_ingestion_status', {'message': 'Data ingestion started'})
            # Temporary save the file
            file_path = os.path.join('temp', file.filename)
            file.save(file_path)
            table_name = os.path.splitext(file.filename)[0]  # Table name is the filename without extension

            # Create the postegresql RAW Database
            create_database_if_not_exists(db_user=DB_USER, db_pass=DB_PASS, db_host=DB_HOST, db_port=DB_PORT,db_name=DB_NAME)

            # Insert data into the RAW Database
            create_table_and_insert_data(db_user=DB_USER, db_pass=DB_PASS, db_host=DB_HOST, db_port=DB_PORT, db_name=DB_NAME, file_path=file_path, file_name=table_name, overwrite=True)

            # Transform raw data into bronze data
            create_database_if_not_exists(db_user=DB_USER, db_pass=DB_PASS, db_host=DB_HOST, db_port=DB_PORT,db_name='bronze')
            transform_data_to_bronze(raw_db_name=DB_NAME,raw_table_name=table_name,bronze_db_name='bronze',db_user=DB_USER,db_pass=DB_PASS,db_host=DB_HOST,db_port=DB_PORT)

            # Transform bronze data into silver data
            bronze_table_name = f"bronze_{table_name}"
            create_database_if_not_exists(db_user=DB_USER, db_pass=DB_PASS, db_host=DB_HOST, db_port=DB_PORT,db_name='silver')
            transform_data_to_silver(db_user=DB_USER, db_pass=DB_PASS, db_host=DB_HOST, db_port=DB_PORT, bronze_db_name='bronze', bronze_table_name=bronze_table_name, silver_db_name='silver', silver_table_name='silver_grouped_trips')

            # Cleanup
            os.remove(file_path)
            socketio.emit('data_ingestion_status', {'message': 'Data ingestion completed'})
            return jsonify({'message': 'File uploaded and saved to database successfully'}), 200

        except Exception as e:
            return jsonify({'error': str(e)}), 500

    else:
        return jsonify({'error': 'Invalid file format'}), 400

#--------------------------------------------------------------------------------------------------------------------------

@app.route('/weekly-average-trips', methods=['GET'])
def weekly_average_trips():
    db_name = request.args.get('db_name')
    table_name = request.args.get('table_name')
    region = request.args.get('region')
    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lon = request.args.get('min_lon', type=float)
    max_lon = request.args.get('max_lon', type=float)

    try:
        if region:
            data_json = fetch_weekly_average_trips(db_user=DB_USER, db_pass=DB_PASS, db_host=DB_HOST, db_port=DB_PORT, db_name=db_name, table_name=table_name, region=region)
        elif all([min_lat, max_lat, min_lon, max_lon]):
            data_json = fetch_weekly_average_trips(db_user=DB_USER, db_pass=DB_PASS, db_host=DB_HOST, db_port=DB_PORT, db_name=db_name, table_name=table_name, min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon)
        else:
            return jsonify({'error': 'Insufficient parameters. Please provide either a region or coordinates.'}), 400

        return jsonify({'data': json.loads(data_json)})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

#--------------------------------------------------------------------------------------------------------------------------

@app.route('/restart-server', methods=['POST'])
def trigger_restart():
    # Start a separate thread to restart the server
    threading.Thread(target=restart_server).start()
    return jsonify({'message': 'Server restarting...'}), 200

#--------------------------------------------------------------------------------------------------------------------------
# Run the Flask app
if __name__ == '__main__':
    socketio.run(app, port=8000, debug=True)