# E-commerce sales Dashboard

https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data/data

Welcome to the Data Warehouse and Analytics Project repository! 🚀
This project builds a modern data warehouse with SQL Server, including ETL processes, data modeling, and analytics to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

This project is the third and last from the Data Analyst bootcamp from Wild Code School (WCS) / Simplon. The main

This project follows the guided project of 'Data with Baraa' on Youtube : [SQL Data Warehouse from Scratch | Full Hands-On Data Engineering Project](https://www.youtube.com/watch?v=9GVqKuTVANE).



## How to Run This Project

[](https://github.com/ahmedmajid22/Data-Warehouse-Sql#how-to-run-this-project)

1. **Set up MySQL** : Ensure you have a running instance of MySQL.
2. **Create Database** : Create a new database for this project.
3. **Run Scripts** : Execute the SQL scripts in the following order:
4. `scripts/1_bronze/`: Load the raw data from the `datasets` folder.
5. `scripts/2_silver/`: Run the transformation scripts to clean and integrate the data.
6. `scripts/3_gold/`: Build the final star schema and run the analytical queries to see the insights.

---

## Key Skills Demonstrated

[](https://github.com/ahmedmajid22/Data-Warehouse-Sql#key-skills-demonstrated)

This project showcases my expertise in:

* **SQL Development** : Writing efficient, scalable, and complex SQL queries for data transformation and analysis.
* **Data Architecture** : Designing and implementing a modern data warehouse using the Medallion Architecture.
* **ETL Pipeline Development** : Building data pipelines to handle extraction, transformation, and loading.
* **Data Modeling** : Creating logical and physical data models (Star Schema) optimized for analytics.
* **Data Analysis** : Translating business requirements into technical queries to deliver actionable insights.

---

## 🏗️ About this dataset

(Copy pasted from Kaggle)

This dataset provides an in-depth look at the profitability of e-commerce sales. It contains data on a variety of sales channels, including Shiprocket and INCREFF, as well as financial information on related expenses and profits. The columns contain data such as SKU codes, design numbers, stock levels, product categories, sizes and colors. In addition to this we have included the MRPs across multiple stores like Ajio MRP, Amazon MRP, Amazon FBA MRP, Flipkart MRP, Limeroad MRP, Myntra MRP and PaytmMRP along with other key parameterslike amount paid by customer for the purchase, rate per piece for every individual transaction. Also we have added transactional parameters like Date of sale months category fulfilledby B2b Status Qty Currency Gross amt. This is a must-have dataset for anyone trying to uncover the profitability of e-commerce sales in today's marketplace

---



## 📈 Data Flow & Schema

[](https://github.com/ahmedmajid22/Data-Warehouse-Sql#-data-flow--schema)

The
 data flows from raw files through the Bronze and Silver layers,
ultimately landing in the Gold layer, which is structured as a star
schema. This design places the core business metrics ( **Fact Table** ) at the center, linked to descriptive attributes ( **Dimension Tables** ).


### Data Flow Diagram



### Gold Layer: Star Schema

[](https://github.com/ahmedmajid22/Data-Warehouse-Sql#gold-layer-star-schema)

This schema simplifies complex queries and allows for fast aggregations, making it ideal for analytics.

## 🏗️ Data Architecture

The data architecture for this project follows **Medallion Architecture** (**Bronze**, **Silver**, and **Gold** layers):

![img_high_architecture_planning](_img/img_high_architecture_planning.png)

1. **Bronze Layer** : Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer** : This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer** : Houses business-ready data modeled into a star schema required for reporting and analytics.

---

## 📖 Project Overview

This project involves:

1. **Data Architecture** : Designing a Modern Data Warehouse Using Medallion Architecture  **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines** : Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling** : Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting** : Creating PowerBi-based reports and dashboards for actionable insights.

🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:

* SQL Development
* Data Architect
* Data Engineering
* ETL Pipeline Developer
* Data Modeling
* Data Analytics

---

## 🛠️ Tools:

(Copy pasted from sir Salkini's repository)

* **[Datasets](https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data/data):** Access to the project dataset (csv files).
* **[SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads):** Lightweight server for hosting your SQL database.
* **[SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16):** GUI for managing and interacting with databases.
* **[Git Repository](https://github.com/):** Set up a GitHub account and repository to manage, version, and collaborate on your code efficiently.
* **[DrawIO](https://www.drawio.com/):** Design data architecture, models, flows, and diagrams.
* PowerBi:

---

📂 Source files and data origin

Ajio MRP

Amazon MRP

Amazon FBA MRP

Flipkart MRP

Limeroad MRP

Myntra MRP

PaytmMR

---

## 📂 Repository Structure

```
sql_data_warehouse_project_with_baraa/
│
├── _diagram_files/                     # Draw.io files used to create diagram
│
├── _img/                           	# Contains all the images files (.png) used in this project
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── step_by_step_process/           # Contains each step of this guided project with markdown files
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   └── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   └── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── .gitignore                          # Files and directories to be ignored by Git
├── LICENSE                             # License information for the repository
└── README.md                           # Project overview and instructions
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
