# **Data Pipeline Automation: BMW Sales (2010-2024)**

Source: https://www.kaggle.com/datasets/y0ussefkandil/bmw-sales2010-2024

## **Project Overview**
This project focuses on building an **Automated ETL (Extract, Transform, Load) Pipeline** to process large-scale sales data from BMW. Orchestrated with **Apache Airflow**, this pipeline ensures that raw sales records are systematically cleaned, transformed, and loaded into a **MongoDB** database for scalable storage and downstream analytics.

## **Key Objectives**
* **Automate** the end-to-end data processing workflow using Airflow DAGs.
* **Process & Clean** raw sales data, including handling missing values and standardizing formats.
* **Feature Engineering**: Create derived attributes such as "Sales Classification" to enrich the dataset.
* **Scalable Storage**: Load over **150,000 records** into a NoSQL database (MongoDB) ensuring data integrity.

---

## **Technology Stack**
* **Orchestration**: Apache Airflow.
* **Language**: Python.
* **Data Manipulation**: Pandas.
* **Database**: MongoDB (NoSQL).
* **Containerization**: Docker (used for hosting Airflow).

---

## **The Pipeline Architecture**

The pipeline is defined in `P2M3_raihan_alfain_DAG.py` and executes the following sequential tasks:

### **1. Extraction (`extract.py`)**
* Reads the raw dataset `P2M3_Raihan_Alfain_data_raw.csv` containing historical BMW sales data (2010-2024).
* Handles initial data ingestion to prepare for transformation.

### **2. Transformation (`transform.py`)**
* **Cleaning**: Standardizes field names and removes inconsistencies in the raw data.
* **Feature Engineering**: Implements logic to classify sales performance and derive new insights from existing columns.
* **Validation**: Ensures data quality before loading it into the production database.

### **3. Loading (`load.py`)**
* Connects to a local **MongoDB** instance.
* Ingests the transformed data into a dedicated collection, validating the successful insertion of documents.
* Optimized for handling large-scale document storage (150K+ records).

---

## **How to Run**
1.  **Setup Environment**: Ensure Docker and Apache Airflow are installed and running.
2.  **Deploy DAG**: Copy `P2M3_raihan_alfain_DAG.py` to your Airflow `dags/` folder.
3.  **Prepare Scripts**: Place `extract.py`, `transform.py`, and `load.py` in the appropriate script directory accessible by Airflow.
4.  **Trigger DAG**: Access the Airflow UI and trigger the `P2M3_raihan_alfain_DAG` workflow.
5.  **Verify Data**: Check your MongoDB collection to see the loaded data.

---

*“This project highlights my capability to design and implement automated data workflows, bridging the gap between raw data sources and analytical databases.”*
