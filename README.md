# LiquorSales Data Migration Project

This is the LiquorSales Data Migration Project, which I was assigned during my internship as a Data Engineer at Bluebik Vulcan.
The project provided an opportunity to explore various aspects of data engineering, including the PySpark module, data modeling, coding standards, and Python object-oriented programming (OOP), as well as gain experience with GCP and Azure cloud platforms.

However, given the constraints of a 3-month timeframe, there are some areas for improvement that I plan to address in the future.

### Data Architecture:
![alt text](data_architecture_v3.png)

### Entity-Relationship Diagram: 
![alt text](ERD.png)

## Table of Contents

- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Instructions](#instructions)
- [Project Structure](#project-structure)
- [Features](#features)
- [Contributing](#contributing)
- [License](#license)

## Project Overview

This project entails extracting data from the liquor_sales dataset, normalizing it into 11 tables, and loading it into Google Cloud Storage (GCS) and BigQuery using Apache Airflow as the orchestration tool. Subsequently, the data is transferred to Azure Data Lake Storage (ADLS) Gen 2 and Azure SQL Database, with Azure Data Factory managing the orchestration and Azure Databricks handling the reconciliation process.

## Technologies Used

- **Apache Spark**: For efficient big data processing, manipulation, and transformation.
- **Apache Airflow**: Orchestrates the ETL process.
- **Python**: For scripting and data transformation.
- **Docker Compose**: To manage the Spark, Airflow and PostgreSQL services.
- **Google Cloud Platform (GCP)**:
    - **Google Cloud Storage (GCS)**: For storing raw and intermediate data.
    - **BigQuery**: For data warehousing and querying.
- **Microsoft Azure**:
    - **Azure Data Factory**: For orchestrating data workflows and managing data movement.
    - **Azure Data Lake Storage (ADLS) Gen 2**: For storing and managing large-scale data.
    - **Azure SQL Database**: For relational data storage and querying.
    - **Azure Databricks**: For data processing and reconciliation with Spark.
    - **Azure Key Vault**: For managing and safeguarding sensitive information, such as secrets and keys.

## Setup and Installation

### Prerequisites

Ensure you have the following installed on your system:

- Docker
- Docker Compose

### Steps

1. **Clone the repository**
    ```bash
    git clone https://github.com/MekWiset/PostgresToMongoDB_migration_project.git
    cd PostgresToMongoDB_migration_project
    ```

2. **Create an `airflow.env` file from the example and configure your environment variables**
    ```bash
    cp airflow.env.example airflow.env
    ```

3. **Edit the `airflow.env` file and fill in the necessary values**
    ```bash
    nano airflow.env
    ```

4. **Create an `.env` file from the example and configure your sensitive information**
    ```bash
    cp env.example .env
    nano .env
    ```

6. **Build and start the services using Docker Compose**
    ```bash
    docker-compose up -d
    ```

7. **Run the `postgres_to_mongodb` pipeline**
    ```bash
    airflow trigger_dag postgres_to_mongodb
    ```

## Usage

To run the ETL pipeline with Airflow UI, follow these steps:

1. Access the Airflow UI at `http://localhost:8080` and trigger the DAG for the ETL process.
2. Monitor the DAG execution and check logs for any issues.
3. Verify the transformed data in the output directory or the specified destination.

## Instructions

1. **Extract Data**:
   - Install the required dependencies:
     ```bash
     pip install -r requirements.txt
     ```
   - Set up your Postgres database connection in the environment variables or configuration files.
   - The data extraction is handled by the `postgres_extractor.py` script located in the `plugins/extract` directory. The script retrieves data from the Postgres database connection and stores it in the `data/medq_data.csv` file.

2. **Transform Data**:
   - Data transformation is performed using the `data_transformer.py` scripts in the `plugins/transform` directory.
   - `data_transformer.py` processes the raw data and saves the transformed data to `data/medq_data_transformed.csv`.

3. **Load Data**:
   - The `mongo_loader.py` script in the `plugins/load` directory handles data loading.
   - It exports the transformed data to MongoDB as specified.

4. **Run the DAG**:
   - Ensure the DAG defined in `dags/pg_to_mongo_dag.py` is scheduled and triggered as required:
     ```bash
     airflow trigger_dag postgres_to_mongodb
     ```

## Project Structure

- `azure_databricks/`: Directory containing Azure Databricks notebooks and jobs.
  - `adls_mount_job.ipynb`: Notebook for mounting ADLS.
  - `spark_job/`: Directory for Spark jobs.
    - `category_job.ipynb`
    - `invoice_job.ipynb`
    - `item_job.ipynb`
    - `item_number_bridge_job.ipynb`
    - `item_price_history_job.ipynb`
    - `store_address_bridge_job.ipynb`
    - `store_address_history_job.ipynb`
    - `store_address_job.ipynb`
    - `store_county_job.ipynb`
    - `store_number_bridge_job.ipynb`
    - `vendor_job.ipynb`
- `cloud_snapshots/`: Directory for cloud platform snapshots.
  - `google_cloud_platform/`: Google Cloud Platform snapshots.
    - `bigquery/`: BigQuery table snapshots.
    - `gcs/`: Google Cloud Storage snapshots.
  - `microsoft_azure/`: Microsoft Azure snapshots.
    - `SQLDB/`: SQL Database snapshots.
    - `data_factory/`: Data Factory pipeline snapshots.
- `dags/`: Directory containing Airflow DAGs.
  - `jobs/`: Directory for Airflow job scripts.
    - `table_category.py`
    - `table_invoice.py`
    - `table_item.py`
    - `table_item_number_bridge.py`
    - `table_item_price_history.py`
    - `table_store_address.py`
    - `table_store_address_bridge.py`
    - `table_store_address_history.py`
    - `table_store_county.py`
    - `table_store_number_bridge.py`
    - `table_vendor.py`
    - `upload_gcs_to_bq.py`
    - `upload_invoice_gcs_to_bq.py`
    - `upload_invoice_to_gcs.py`
    - `upload_to_gcs.py`
    - `utils/`: Directory for utility scripts.
      - `checkpointmanager.py`: Script for managing checkpoints.
      - `configparser.py`: Script for parsing configuration files.
      - `normalizer.py`: Script for normalizing data.
  - `liquor_sales_dag.py`: Main DAG for the LiquorSales data migration process.
  - `invoice_feeding.py`: DAG for feeding invoice data.
- `datasets/`: Directory for dataset files.
- `outputs/`: Directory for output files (e.g., transformed data).
  - `liquor_sales.csv`: Raw data file for liquor sales.
- `airflow.env.example`: Template for Airflow environment variables.
- `config.yaml`: Configuration file for the project.
- `Dockerfile`: Dockerfile for setting up the project environment.
- `docker-compose.yaml`: Docker Compose configuration file for orchestrating services.
- `README.md`: Documentation for the project.
- `requirements.txt`: Python dependencies file.

## Features

- **Data Extraction**: Retrieves data from MedQ Database.
- **Data Transformation**: Processes and transforms the data using Pandas.
- **Data Loading**: Exports transformed data to MongoDB.
- **Incremental Data Updates**: Efficiently handles newly added data.

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes and commit them (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Open a pull request.

Please ensure your code follows the project's coding standards and includes relevant tests.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.