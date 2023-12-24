import socketio

# Define the namespace
namespace = '/my_namespace'

# Create a SocketIO client
sio = socketio.Client()

@sio.event(namespace=namespace)
def connect():
    print("Connected to the server")

@sio.event
def connect_error():
    print("Connection failed")

@sio.event
def connect_failed():
    print("Connection failed")

@sio.event(namespace=namespace)
def disconnect():
    print("Disconnected from the server")

@sio.event(namespace=namespace)
def data_ingestion_status(data):
    print('data_ingestion_status:', data)

@sio.event(namespace=namespace)
def data_transformation_status(data):
    print('data_transformation_status:', data)

@sio.event(namespace=namespace)
def trigger_restart(data):
    print('trigger_restart:', data)

@sio.event(namespace=namespace)
def weekly_average_trips_status(data):
    print('weekly_average_trips_status:', data)

# Connect to the Flask-SocketIO server
sio.connect('http://localhost:8000', namespaces=[namespace])

# Keep the program running to listen for events
try:
    while True:
        pass
except KeyboardInterrupt:
    sio.disconnect()
