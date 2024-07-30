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
    git clone https://github.com/MekWiset/LiquorSales_Data_Pipeline.git
    cd LiquorSales_Data_Pipeline
    ```

2. **Create an `airflow.env` file from the example and configure your environment variables**
    ```bash
    cp airflow.env.example airflow.env
    ```

3. **Edit the `airflow.env` file and fill in the necessary values**
    ```bash
    nano airflow.env
    ```

4. **Build and start the services using Docker Compose**
    ```bash
    docker-compose up -d
    ```

5. **Create a Spark connection in Airflow**
    - Open the Airflow UI at `http://localhost:8080`
    - Navigate to **Admin** > **Connections**
    - Click the **+** button to add a new connection
    - Set the following details:
        - **Connection ID**: `spark-conn`
        - **Connection Type**: `Spark`
        - **Host**: `spark://spark-master`
        - **Port**: `8080`
    - Click **Save**

6. **Run the `liquor_sales_dag` pipeline**
    ```bash
    airflow trigger_dag liquor_sales_dag
    ```

7. **Run the `invoice_feeding` DAG to simulate data feeding**
    ```bash
    airflow trigger_dag invoice_feeding
    ```

8. **Set up Microsoft Azure services**
    - Create Azure SQL Database Instance and connect it to Azure Data Studio
    - Create data pipelines in `Data Factory` as shown in `cloud_snapshots/microsoft_azure`
    - Set up Azure Databricks service to perform reconciliation:
        - Create Databricks compute instance
        - mount `raw_data` directory to Databricks using the `azure_databricks/adls_mount_job.ipynb` script
        - Copy `azure_databricks/spark_job` to Databricks workspace
    - Run the Data Factory pipelines once each sink destination has been linked to the respective table names

## Usage

To run the ETL pipeline with Airflow UI, follow these steps:

1. Access the Airflow UI at `http://localhost:8080` and trigger the DAG for the ETL process:
    - Trigger the `liquor_sales_dag` for the main data pipeline.
    - Trigger the `invoice_feeding` DAG to simulate data feeding.

2. Monitor the DAG execution and check logs for any issues:
    - Use the Airflow UI to view the progress of the DAG runs.
    - Check the logs for each task to ensure there are no errors.

3. Verify the transformed data in the output directory or the specified destination:
    - Confirm that the transformed data is stored in the `outputs/` directory locally.
    - Verify that the data has been loaded into Google Cloud Storage and BigQuery.
    - Check Azure Data Lake Storage Gen 2 and Azure SQL Database for the reconciled data.


## Instructions

1. **Extract Data**:
   - Install the required dependencies:
     ```bash
     pip install -r requirements.txt
     ```
   - Set up your connections to the data sources (Kaggle, Google Cloud Storage, and BigQuery) in the environment variables or configuration files.

2. **Transform Data**:
   - Run the Spark jobs for each table located in the `dags/jobs` directory. These jobs handle the extraction, transformation, and loading (ETL) processes and store the output in the `outputs/` directory of the local directory.
   - The Spark jobs also perform the necessary data transformations. Utility scripts such as `normalizer.py`, `configparser.py`, and `checkpointmanager.py` are applied within these Spark jobs to standardize, parse configurations, and manage checkpoints.

3. **Load Data**:
   - The transformed data is saved in the `outputs/` directory of the local directory.
   - Use the Airflow DAG to load the transformed data to Google Cloud Storage and BigQuery:
     ```bash
     airflow trigger_dag liquor_sales_dag
     ```

4. **Run the Data Pipeline**:
   - Ensure the DAG defined in `dags/liquor_sales_dag.py` is scheduled and triggered as required:
     ```bash
     airflow trigger_dag liquor_sales_dag
     ```
   - To simulate data feeding, run the `invoice_feeding` DAG:
     ```bash
     airflow trigger_dag invoice_feeding
     ```
   - Use Apache Airflow to monitor and manage the execution of the data pipeline.

5. **Spark Configuration in Airflow**:
   - Create a Spark connection in Airflow with the following details:
     - Connection ID: `spark-conn`
     - Host: `spark://spark-master`
     - Port: `8080`

6. **Secure Data Handling**:
   - Make sure to configure Azure Key Vault for managing sensitive connection strings and credentials as specified in your configuration files.

7. **Azure Setup**:
   - Set up your Azure account and configure the necessary resources including Azure Data Factory, Azure Data Lake Storage Gen 2 (ADLS Gen 2), and Azure SQL Database.
   - Store the Google Cloud Platform (GCP) credentials JSON file in Azure Key Vault. This will be used as a key to securely access data from BigQuery.

8. **Data Factory Configuration**:
   - Use Azure Data Factory to import data from BigQuery. The credentials stored in Azure Key Vault will be used to authenticate and access the data.
   - Configure pipelines in Azure Data Factory to reconcile the imported data from `raw_data/` and store it in `transformed_data/` in ADLS Gen 2.

9. **Data Reconciliation and Storage**:
   - The reconciled data in ADLS Gen 2 can be further processed and analyzed using Azure Databricks as required.
   - Finally, load the reconciled data from ADLS Gen 2 to Azure SQL Database for querying and analysis.

10. **Monitor and Manage**:
    - Use the Azure portal to monitor and manage the data pipelines, storage, and databases to ensure smooth operation and timely updates.

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

- **Data Extraction**: Retrieves liquor sales data from multiple sources including Kaggle (CSV files), Google Cloud Storage, and BigQuery.
- **Data Transformation**: Utilizes Apache Spark within Docker containers to process and transform the data, ensuring it meets the desired format and quality.
- **Data Loading**: Loads transformed data into Azure Data Lake Storage Gen 2 and Azure SQL Database for further analysis and querying.
- **Orchestration and Workflow Management**: Employs Apache Airflow to manage and automate the end-to-end data pipeline, ensuring smooth data flow and timely updates.
- **Incremental Data Updates**: Efficiently handles newly added data by ensuring only new and updated records are processed and loaded into the target systems.
- **Secure Data Handling**: Uses Azure Key Vault to manage and secure sensitive connection strings and credentials.
- **Data Processing and Analysis**: Leverages Azure Databricks for advanced data processing and analytics, enabling deeper insights and reporting capabilities.

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