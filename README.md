# ETL Workflow for Employee Data Analysis Using Apache Airflow and PySpark
## ETL for Airflow Data Processing - Employees Analysis Workflow ###
> This repository contains an Apache Airflow DAG (Directed Acyclic Graph) designed for data processing workflows using PySpark. The DAG is orchestrated to load, process, and transform data from CSV files into a structured format for further analysis and storage.
![image](https://github.com/GIZELLYPY/airflow_processing_data/assets/52425554/b0895a0d-f7d6-4ec3-bc7f-e066c8d350bd)


### Data Flow
Data is initially read from CSV files, processed through various transformations in the Silver layer, and ultimately aggregated into structured tables in the Golden layer. Each task within the DAG handles a specific part of this pipeline, ensuring data is methodically transformed and stored at each stage.

### Quality Checks Using Great Expectations
At the Silver layer, quality checks are implemented using the Great Expectations library to ensure data integrity before it is loaded into the Gold layer. These checks validate data quality aspects such as:

  * Correctness of data formats.
  * Non-duplication of key fields.
  * Completeness and accuracy of the data.



#### Repository Structure
The codebase is structured into various directories, mainly focusing on the scripts under src/dag/tasks, which include the individual tasks that make up the DAG.

#### Overview of dag.py
The main DAG definition can be found in src/dag/dag.py. This script sets up the DAG with the following tasks:

#### Start the workflow.
Process data from multiple CSV inputs.
Load processed data into a staging area (Silver layer).
Aggregate transformed data into the final Gold layer for analytics.
Each task is implemented as a Python function that gets executed by a PythonOperator in Airflow.

#### Task Implementations
Here's a brief on some of the key tasks:

##### input_base_cargos.py
<b>Purpose</b>: Processes job position data from BaseCargos.csv.
<b>Functions</b>:
  - treat_deslocated_columns: Normalizes and cleans the DataFrame.
  - rename_columns: Maps Portuguese column names to English.
  - input_on_silver_base_cargos: Main function to process the data and save it.
##### input_base_cep.py
<b>Purpose</b>: Handles postal code data transformation from BaseCEP.csv.
<b>Functions</b>:
  - read_base_cep: Reads and formats the CSV to a DataFrame.
  - rename_columns: Renames columns to standardized English names.
  - input_on_silver_base_cep: Entry point for processing and storing postal code data.
#####  input_base_cliente.py
<b>Purpose</b>: Processes client data from BaseClientes.csv.
<b>Functions</b>:
  - Various preprocessing functions to clean and transform data fields.
  - input_on_silver_base_cliente: Integrates all preprocessing steps and saves the output.
#####   input_base_funcionarios.py
<b>Purpose</b>:Manages employee data from BaseFuncionarios.csv.
<b>Functions</b>:
  - drop_unnecessary_columns: Removes columns not needed.
  - process_date_columns: Converts date formats.
  - input_on_silver_base_funcionarios: Processes and anonymizes employee data before saving.
#####  utils.py
Contains utility functions like read_csv_to_df and save_data_agg to assist with file handling and data storage operations.
#####  input_golden.py
<b>Purpose</b>:  Aggregates data from the Silver layer to the Golden layer.
<b>Functions</b>:
  - join_and_group_by_region: Combines datasets and groups by region.
  - calculate_average_time_on_job: Calculates average employment duration.
  - process_and_input_on_golden: Executes all steps to process data into the Golden layer.

## Some Data Vizualization

![image](https://github.com/GIZELLYPY/airflow_processing_data/assets/52425554/bb46d37d-9b06-45be-8507-5ecb2bf6004d)

![image](https://github.com/GIZELLYPY/airflow_processing_data/assets/52425554/c5690a82-35ae-4aeb-b8c2-7057d5d9482c)

![image](https://github.com/GIZELLYPY/airflow_processing_data/assets/52425554/9e822f6c-c8cf-480a-9710-44bff31a44be)

![image](https://github.com/GIZELLYPY/airflow_processing_data/assets/52425554/ad35fc48-20fe-40e7-9afe-ddd68b7b4922)

![image](https://github.com/GIZELLYPY/airflow_processing_data/assets/52425554/988ce184-5daa-4e3b-97e6-f73428e4989f)
