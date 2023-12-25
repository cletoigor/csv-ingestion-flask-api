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
