# This code will set up a basic Flask project structure.

# Importing necessary libraries
from flask import Flask, request, jsonify
import csv
import os
import signal
import threading
import time

# Initialize the Flask application
app = Flask(__name__)

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

            # Process the file
            data = process_csv(file_path)

            # Clean up the temporary file
            os.remove(file_path)

            return jsonify({'message': 'File uploaded successfully', 'data': data}), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    else:
        return jsonify({'error': 'Invalid file format'}), 400

#Parse the CSV File
def process_csv(file_path):
    # Open the CSV file and read its contents
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data_list = [row for row in csv_reader]
    return data_list

@app.route('/restart-server', methods=['POST'])
def trigger_restart():
    # Start a separate thread to restart the server
    threading.Thread(target=restart_server).start()
    return 'Server restarting...', 200

def restart_server():
    time.sleep(1)  # Short delay to ensure the response is sent
    os.kill(os.getpid(), signal.SIGINT)

# Run the Flask app
if __name__ == '__main__':
    app.run(port=8000,debug=True)