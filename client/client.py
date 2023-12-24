import socketio

# Define the namespace
namespace = '/test'

# Create a SocketIO client
sio = socketio.Client()

@sio.event(namespace=namespace)
def connect():
    print("Connected to the server")

@sio.event(namespace=namespace)
def disconnect():
    print("Disconnected from the server")

@sio.on('data_ingestion_update', namespace=namespace)
def on_message(data):
    print('Update:', data['message'])

# Connect to the Flask-SocketIO server
sio.connect('http://localhost:8000', namespaces=[namespace])

# Keep the program running to listen for events
try:
    while True:
        pass
except KeyboardInterrupt:
    sio.disconnect()
