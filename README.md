# Sales Data Warehouse Project - PostgreSQL

## Project Overview
This project demonstrates the development of a data warehouse using PostgreSQL, focusing on the ETL (Extract, Transform, Load) processes. The data warehouse is designed to store sales data across three schema layers: landing, staging, and core. This multi-layered architecture ensures efficient data processing, transformation, and storage.
## Key Concepts
### 1. Data Warehousing
A data warehouse is a central repository for storing large volumes of data from multiple sources. It supports analytical querying and reporting, providing valuable business insights.

### 2. ETL (Extract, Transform, Load)
ETL is a process that extracts data from various sources, transforms it into a suitable format, and loads it into the data warehouse. This project implements the ETL process in three main stages:
- **Extract**: Importing raw data into the landing schema.
- **Transform**: Processing and cleansing data in the staging schema.
- **Load**: Storing transformed data in the core schema.

### 3. Schema Layers
The project is structured into three schema layers:
- **Landing Layer**: Serves as the raw data source.
- **Staging Layer**: Holds cleansed and transformed data.
- **Core Layer**: Contains final fact and dimension tables for analytical querying.

### 4. Incremental Load
Incremental load involves loading only new or updated data based on a specified column (e.g., transactional_date). This ensures efficient data processing and storage management.

### 5. De-duplication
De-duplication ensures that no duplicate records are loaded into the data warehouse. This is achieved by comparing new records with existing ones based on unique identifiers.

## Data Source
For this project, we use two simple CSV files to simulate the sales data. These files are loaded from a local computer, demonstrating how to handle data import in the absence of production or large datasets.

## Project Structure
### Landing Schema
The landing schema serves as the raw data source. Data is imported here before any processing. This schema represents the initial step of the ETL pipeline, where raw, unprocessed data is ingested. Working with this layer has enhanced my understanding of data ingestion techniques and the importance of raw data management.

### Staging Schema
The staging schema holds cleansed and transformed data, mirroring the structure of the landing schema to facilitate seamless data transfer. This intermediate layer is critical for data validation and transformation. Through this, I improved my knowledge of data cleansing, transformation techniques, and the practical implementation of business rules.

### Core Schema
The core schema contains the final fact and dimension tables designed for efficient analytical querying and reporting. This layer is optimized for performance, ensuring quick retrieval of insights. Creating this schema has deepened my understanding of star and snowflake schema designs, indexing strategies, and query optimization.

## Technologies Used
- **Database**: PostgreSQL
- **ETL**: SQL

## Skills Learned
- Data Warehousing Concepts
- ETL Process Implementation
- SQL for Data Manipulation
- Incremental Data Loading
- Data De-duplication Techniques

## How to Use
1. **Set up PostgreSQL**: Ensure PostgreSQL is installed and running.
2. **Create Schemas**: Create the landing, staging, and core schemas.
3. **Load Data**: Import raw data into the landing schema.
4. **Run ETL Processes**: Execute SQL scripts to transform and load data into the staging and core schemas.
5. **Query Data**: Use SQL to query data from the core schema for analysis and reporting.

## Conclusion
This project showcases the complete development cycle of a data warehouse using PostgreSQL, including the ETL process, data transformation, and loading strategies. The structured approach ensures data integrity, efficient processing, and valuable insights through comprehensive analytical querying.
