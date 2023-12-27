# CSV Data Ingestion Workflow

**Author** <br />
**Igor Cleto S. De Araujo** <br />
**Version 0.0.1** <br />

**Overview** <br />
This Flask-based API provides an automated process for ingesting and processing trip data. It is designed to handle large datasets, with a focus on scalability and efficiency. The data represents trips taken by various vehicles, including information about cities, points of origin, and destinations.

The solution is based on the data architecture illustrated in the image below and features a complete ELT (Extract, Load, and Transform) data engineering process through the PostgreSQL database. <br />

<img width="800" alt="image" src="https://github.com/cletoigor/csvingestion/assets/50745958/56b51afe-b6ec-425e-9333-33adf2728781">

<br /> **Features** <br />
* **Automated Data Ingestion**: <br /> An automated process to ingest data from CSV files. <br />
* **Data Grouping:** <br /> Trips with similar origin, destination, and time of day are grouped together. <br />
* **Weekly Average Trips Calculation:** <br />  Ability to calculate the weekly average number of trips for a specified area, either defined by a bounding box (coordinates) or a region. <br />
* **Real-Time Status Updates:** <br /> Real-time updates on the status of data ingestion and processing, without the need for polling, using Flask-SocketIO. <br />
* **Scalability:** <br /> The solution is scalable to handle large datasets, up to 100 million entries. <br />
* **SQL Database Integration:** <br /> Utilizes a PostgreSQL database for data storage and processing.
  
**API Docs** <br />
[Flask API SwaggerHub](https://app.swaggerhub.com/apis/IgorCleto/data_engineering_challenge_api/1.0#/)

**Data Warehouse layers** <br />
* **Bronze Layer:** <br />
Stores the raw data derived from the uploaded CSV file. There is no transformation, and it serves as the single source of truth for PostgreSQL.
* **Silver Layer:** <br />
The first layer that structures the raw data, performing data type conversions, cleaning, and optimizations to prepare the data for queries and use by the business team.
* **Gold Layer:** <br />
Deals with data grouping for the business area and displays metrics about the ingested data.

# Project Setup
This guide will walk you through the steps to set up the project on your local machine.

**Prerequisites** <br />

Ensure you have the following installed:
* Python 3.x
* pip (Python package manager)
* PostgreSQL 16 (Ensure that PostgreSQL server is installed and running). For database management, you can use tools like DBeaver or pgAdmin. These tools provide a user-friendly interface for handling database tasks.

**Installation**

1) **Clone the Repository**

Clone the repository to your local machine using Git:

```console
git clone https://github.com/cletoigor/csv-ingestion-flask-api.git
```
2) **Create a Virtual Environment (Optional but Recommended)**
   
Using a virtual environment for your Python projects is a good practice. This keeps your project dependencies separate from your global Python installation.

Navigate to the project directory:
```console
cd [Project Directory]
```
Create the virtual environment:
```console
python -m venv venv
```
or
```console
python3 -m venv venv
```

Activate the virtual environment:
* On Windows:
```console
venv\Scripts\activate
```
* On macOS/Linux:
```console
source venv/bin/activate
```

3) **Install Dependencies**
   
With the virtual environment activated, install the project dependencies:
```console
pip install -r requirements.txt\
```
or
```console
pip3 install -r requirements.txt\
```

5) **Database Setup**  <br />
Configure the PostgreSQL database connection using the db_config.txt file provided in the project. Edit this file to input your local PostgreSQL details (host, database name, user, password, and port).
The format of db_config.txt should be key-value pairs, like so:

```make
DB_HOST=localhost
DB_NAME=mydatabase
DB_USER=myuser
DB_PASS=mypassword
DB_PORT=5433
```
You can manage your PostgreSQL database using tools like DBeaver or pgAdmin, which provide a graphical interface to interact with your database.

6) **Run the Application**  <br />

After installing the dependencies and setting up the database, you can run the application:
```console
python app.py
```
or
```console
python3 app.py
```
This will start the Flask server. Make sure it's running without any errors.

7) **Run the Client Application**  <br />
Open a new terminal or command prompt window. This is important because your Flask application is running and occupying the current terminal.  <br />
Navigate to the directory where your client application script is located. This is the script that connects to the Flask-SocketIO server and listens for real-time updates.
Run the client application script.

```console
python client.py
```
or
```console
python3 client.py
```

The client should now connect to the Flask-SocketIO server and start receiving real-time updates.

8) **Perform Actions to Trigger Updates**  <br />

With both the server and the client running, you can now perform actions that trigger updates.  <br />
For example, if your application has a feature to upload files and process data, doing so should trigger updates that the client will receive.  <br />
Watch the client's terminal or command prompt window for real-time status updates as you interact with your Flask application.
 <br /> It is recommended to use API testing software like Postman or alternatives such as Insomnia or Swagger UI.

 9) **Monitoring**
<br /> Keep an eye on both the Flask application's terminal and the client's terminal. The Flask application terminal will show server logs and any potential errors, while the client's terminal will display the real-time status updates.

10) In the 'sql' directory, there is also an SQL file containing the necessary query to replicate the result of weekly trips grouped by region or by coordinates directly in the PostgreSQL database generated.










