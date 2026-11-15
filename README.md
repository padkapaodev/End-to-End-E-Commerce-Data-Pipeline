# End-to-End E-Commerce Data Pipeline (Medallion Architecture)

An end-to-end Data Engineering pipeline built on **Databricks** that processes raw e-commerce data into analytics-ready Data Marts. The codebase is structured using **Object-Oriented Programming (OOP)** and a **Config-Driven** approach to ensure modularity, scalability, and ease of maintenance.

## Architecture & Data Flow

The pipeline follows the **Medallion Architecture** across three distinct layers and runs as an automated workflow using Databricks Serverless Compute:

```text
[Landing Zone (Volumes)] 
       │
       ▼  (Auto Loader / Streaming)
[Bronze Layer]  ──► Ingests raw data with audit metadata (_file_name, _processing_time)
       │
       ▼  (Data Cleaning, Validation & Quarantine)
[Silver Layer]  ──► Performs data quality checks (Nulls/Duplicates), routes invalid records to Quarantine,
       │            and structures clean data into a Star Schema (Fact & Dimension tables)
       ▼  (Business Logic, Join & Aggregation)
[Gold Layer]    ──► Builds business-level Data Marts ready for BI and analytical reporting
```

## Tech Stack & Tools

* **Platform:** Databricks (Unity Catalog, Volumes, Databricks Workflows)
* **Processing Engine:** Apache Spark (PySpark, Structured Streaming / Auto Loader)
* **Storage Format:** Delta Lake
* **Language & Design:** Python (`@dataclass`, OOP, Modular Scripts), SQL

## Key Highlights

* **Metadata & Config-Driven Framework:** Decouples table configurations (schemas, primary keys, file paths) from core transformation logic, making it easy to onboard new tables without duplicating code.
* **Modular & OOP Design:** Encapsulates layer-specific logic into dedicated Python classes (`bronzeLayer`, `silverLayer`, `goldLayer`) separated from execution notebooks for cleaner code organization.
* **Automated Data Quality & Quarantine:** Implements validation rules in the Silver layer to catch null keys and duplicate records, isolating bad data into a dedicated `quarantine_layer` schema before loading clean data.
* **On-Demand Data Loading:** Optimizes resource usage in the Gold layer by loading only the specific Silver tables required for each individual Data Mart transformation.

## Data Models

### 1. Silver Layer (Star Schema)
* **Dimensions:** `dim_customers`, `dim_products`
* **Facts:** `fact_orders`, `fact_sales`

### 2. Gold Layer (Data Marts)
* **`sales_item` (Wide Table):** Granular, item-level sales records joined with product, customer, and sales channel attributes for detailed revenue and profit analysis.
* **`product_performance` (Wide Table):** Product-level metrics combining unit pricing, costs, ratings, and sales volume.
* **`monthly_summary` (Aggregated Summary):** Monthly customer purchasing behavior tracking total orders, item quantities, net spend, and total profit per customer.


## Future Improvements

* Adopt **Databricks Asset Bundles (DAB)** to define and manage pipeline jobs and workspace resources as Infrastructure as Code (`.yml`).
* Configure **GitHub Actions** to automate CI/CD deployments upon pushing code updates to the repository.