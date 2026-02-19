# E-commerce sales Dashboard

## Summary

* Introduction
* Project overview
* About this dataset
* Tools
* Nomenclature
* Data Flow & Schema
* Data Architecture
* Repository structure
* How to run this project
* License
* About me

---

## Introduction

This project builds a modern data warehouse with SQL Server, including ETL processes, data modeling, and analytics to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

---

## 📖 Project Overview

This project involves:

* **Data Architecture** : Designing a Modern Data Warehouse Using Medallion Architecture  **Bronze**, **Silver**, and **Gold** layers.
* **ETL Pipeline Development** : Building data pipelines to handle extraction, transformation, and loading, from source systems into the warehouse.
* **SQL Development** : Writing efficient, scalable, and complex SQL queries for data transformation and analysis.
* **Python Development** : Writing efficient, scalable, and complex python for data pipeline, data transformation and analysis.
* **Data Modeling** : Creating logical and physical data models (Star Schema) optimized for analytics.
* **Data Analysis** : Translating business requirements into technical queries to deliver actionable insights.
* **Data Visualization & Reporting** : Creating PowerBi-based reports and dashboards for actionable insights.

---

## 🏗️ About this dataset

(Copy pasted from Kaggle)

This dataset provides an in-depth look at the profitability of e-commerce sales. It contains data on a variety of sales channels, including Shiprocket and INCREFF, as well as financial information on related expenses and profits. The columns contain data such as SKU codes, design numbers, stock levels, product categories, sizes and colors. In addition to this we have included the MRPs across multiple stores like Ajio MRP, Amazon MRP, Amazon FBA MRP, Flipkart MRP, Limeroad MRP, Myntra MRP and PaytmMRP along with other key parameterslike amount paid by customer for the purchase, rate per piece for every individual transaction. Also we have added transactional parameters like Date of sale months category fulfilledby B2b Status Qty Currency Gross amt. This is a must-have dataset for anyone trying to uncover the profitability of e-commerce sales in today's marketplace

---

## 🛠️ Tools:

* **[Datasets](https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data/data) :** Access to the project dataset (excel files).
* SQLite3 :
* **[Git Repository](https://github.com/) :** Set up a GitHub account and repository to manage, version, and collaborate on your code efficiently.
* **[DrawIO](https://www.drawio.com/) :** Design data architecture, models, flows, and diagrams.
* PowerBi :
* Docker:
* Airflow:

### Librairies:

The main queries required for this project are:

```
1. sqlite3                              # Database creation, connexion, SQL queries
2. pandas                               # ...
3. xlsxwriter                           # Writing Excel files for data exploration
4. Geopy                                # ...
5. Timezonefinder                       # ...
6. regex                                # ...
7. request                              # ...
```

For a complete list, check the file ./requirements.txt.

### Nomenclature:

PEP 8 library structure

1. Standard library
2. Third-party
3. Imports internes au projet

### Naming conventions:

For more information, check ./docs/naming_conventions.md

### Recommanded VSCode extensions:

- [Code organizer by ran-codes](https://marketplace.visualstudio.com/items?itemName=ran-codes.code-organizer)

![1770985325939](image/README/1770985325939.png)

![1770985191504](image/README/1770985191504.png)

---

## 📈 Data Flow

(img_data_flow)

The data flows from raw files through the Bronze and Silver layers, ultimately landing in the Gold layer, which is structured as a star schema. This design places the core business metrics ( **Fact Table** ) at the center, linked to descriptive attributes ( **Dimension Tables** ).

## 🏗️ Data Architecture

The data architecture for this project follows **Medallion Architecture** (**Bronze**, **Silver**, and **Gold** layers):

(img_high_architecture_planning)

1. **Bronze Layer** : Stores raw data as-is from the source systems. Raw data files (excel files with sheets) are converted to individual csv files. These CSV files are ingested into SQLite3 Database.
2. **Silver Layer** : This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer** : Houses business-ready data modeled into a star schema required for reporting and analytics.

## Data model : Star Schema

(img)

Automation:

SQLite only works ith SequentialExecutor (run tasks one by one, no parallelism).

For paralellism, Air's LocalExecutor requires a proper database like PostgreSQL.

---

## How to Run This Project

1. step1
2. step2
3. step3...

---

## 📂 Repository Structure

```
WCS_PROJECT3_ENV/

WCS_PROJECT3_ONLINE_RETAIL_II_ETL_EDA_RFM/
│
├── airflow/
│   ├── dags/  
│   ├── logs/  
│   └── plugins/ 
│
├── dashboard/				# PowerBi file for data visualization
│
├── data/                           # Contains all the data (raw and transformed) and the sqlite database
│   ├── business_inputs/
│   │	└── rfm/
│   │	│	└── RFM_SCORING.xlsx
│   ├── csv/                            # Datasets are converted from raw to csv files in this folder
│   ├── data_exploration/               # Excel files create by scripts to facilitate data exploration
│   ├── database/                       # Sqlite3 database
│   └── raw/                            # Raw datasets used for the project
│
├── diagram_files/                      # Draw.io files used to create diagram
│
├── docker/  
│   ├── docker-compose.yml
│   └── Dockerfile
│
├── docs/                               # Project documentation and architecture details
│   ├── step_by_step_process/           # Contains each step of this guided project with markdown files
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── command_line.md                 # Reminder of useful command lines
│   └── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── .env				# Hidden environement files, for Airflow
│
├── images/                           	# Contains all the images used in this project
│
├── src/                            	# Contains all the python scripts used for this project
│   ├── utils/
│   │	├── __init__.py
│   │	├── db.py
│   │	├── connecting_to_database.py
│   │	├── create_table.py
│   │	├── data_exploration.py
│   │	├── export_data_to_xlsx.py
│   │	└── watermark.py
│   │
│   ├── ingestion/  
│   │	├── __init__.py
│   │	├── data_xlsx_to_csv.py
│   │	└── creating_database.py
│   │
│   ├── bronze/  
│   │	├── __init__.py
│   │	└── script_layer_bronze.py
│   │
│   ├── silver/  
│   │	├── __init__.py
│   │	├── script_layer_silver.py
│   │	├── silver_country_mapping.py
│   │	├── silver_exchange_rate_historic.py
│   │	└── silver_product_mapping.py
│   │
│   ├── gold/  
│   │	├── __init__.py
│   │	├── script_layer_gold.py
│   │	├── script_rfm_scoring.py
│   │	└── script_cltv.py
│   │
│   ├── __init__.py
│   ├── exploring_layer_bronze.py
│   ├── exploring_layer_silver.py
│   └── exploring_layer_gold.py
│
├── tests/                              # Test scripts and quality files
│
├── .gitignore                          # Files and directories to be ignored by Git
├── LICENSE                             # License information for the repository
├── requirements.txt                    # List of all required libraires for this project
└── README.md                           # The present project overview and instructions

```

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

---

## 🌟About Me

Hello there. #insert-kenobi-meme-gif

I'm Matthew, a Data Analyst student transitioning toward a Data Engineering position, with Analytics Engineer as my long-term goal. I bring 11 years of professional experience, including 8 years in digital marketing, with strategic and operational expertise across diverse industries (tires, merchant marine, Virtual Reality accessories, and e-commerce) and targets (B2C, B2B2C, B2B, retail, army and defense forces).

This extensive background has given me a solid understanding of marketing challenges, product strategy, and ROI optimization. Currently pursuing training in Data Analysis and Data Engineering, I'm drawn to these fields for their strong business focus and natural synergy with my marketing expertise.

I'm eager to leverage my skills in data structuring, analysis, and value creation to contribute to your projects as a Data Engineer through a work-study contract (contrat de professionnalisation) starting in March 2026.

*Ô rage ! ô désespoir ! ô dataset ennemi !
N'ai-je donc tant vécu que pour ne pas avoir ce KPI
Et ne suis-je blanchi dans tous ces rapports clairs
Que pour voir en un jour flétrir tant d'OKRs ?*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/matthew-rives/)
