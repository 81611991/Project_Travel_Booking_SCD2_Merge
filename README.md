# Travel Booking SCD2 Merge

## Project Overview
This project implements an **incremental load with Slowly Changing Dimension Type 2 (SCD2) merge** for a travel booking system. It ingests booking and customer data, processes it using **PySpark on Databricks**, and maintains a historical record of customer changes using **SCD Type 2**.

## Technologies Used
- **Azure Data Lake Storage (ADLS)**
- **PySpark**
- **Databricks**
- **Delta Lake**
- **Databricks Workflows**
- **GitHub**

## Architecture Diagram
![Architecture Diagram](path/to/architecture-diagram.png)

---

## Implementation Details

### **1. Data Sources**
- **Booking Data** (stored in `booking_data` folder in ADLS)
- **Customer Data** (stored in `customer_data` folder in ADLS)

### **2. Data Ingestion & Transformation**
- Read booking and customer data from ADLS using PySpark.
- Join booking data with customer data on `customer_id`.
- Compute `total_cost` as `amount - discount`.
- Aggregate by `booking_type` and `customer_id` to compute:
  - `total_amount_sum`
  - `total_quantity_sum`

### **3. Fact Table Processing (booking_fact)**
- **Step 1:** Check if `booking_fact` exists.
- **Step 2:** If the table exists:
  - Read existing fact table.
  - Append new aggregated data.
  - Recompute aggregates for `booking_type` and `customer_id`.
- **Step 3:** If the table does not exist:
  - Create `booking_fact` with aggregated data.
- **Step 4:** Write the final aggregated data to the fact table using `overwriteSchema=True`.

### **4. Dimension Table Processing (customer_dim) - SCD Type 2**
- **Step 1:** Check if `customer_dim` exists.
- **Step 2:** If the table exists:
  - Perform a **merge operation** on `customer_id` where `valid_to = '9999-12-31'`.
  - When a match is found, update `valid_to` with `valid_from` of the incoming record.
  - Insert new records with updated `valid_from` and `valid_to` values.
- **Step 3:** If the table does not exist:
  - Overwrite table with new customer data.

---

## **Project Workflow**
### **Step 1: Setup Azure Resources**
- Create **Databricks workspace**.
- Set up **Azure Data Lake Storage (ADLS)**.
- Create folders:
  - `booking_data` (for booking CSVs)
  - `customer_data` (for customer CSVs)

### **Step 2: Run Data Pipeline**
- **Read Booking and Customer Data:**
  - Load data into PySpark DataFrames.
  - Apply transformations (join, aggregations, calculations).
- **Write to Fact and Dimension Tables:**
  - Merge and update `booking_fact`.
  - Perform SCD2 merge in `customer_dim`.
- **Workflow Execution:**
  - Automate process using Databricks Workflows.

### **Step 3: Validate Data**
- Check `booking_fact` table for correct aggregations.
- Check `customer_dim` table for historical records (SCD2 merge).

```python
# Read booking data
booking_df = spark.read.csv(booking_data, header=True, inferSchema=True, multiLine=True, quote='"')

# Read customer data
customer_df = spark.read.csv(customer_data, header=True, inferSchema=True, multiLine=True, quote='"')
```

---

## **How to Run the Project**
1. Clone the repository and configure Databricks.
2. Upload data files to `booking_data` and `customer_data` folders in ADLS.
3. Workflow will trigger automatically based on file arrival.
4. Validate the processed data in `booking_fact` and `customer_dim` tables.

---

## **Project Folder Structure**
```
├── notebooks/            # Databricks Notebooks
│   ├── booking_fact.ipynb
│   ├── customer_dim_scd2.ipynb
│
├── data/                 # Sample datasets
│   ├── booking_sample.csv
│   ├── customer_sample.csv
│
├── docs/                 # Documentation & workflow files
│   ├── project-explanation.txt
│   ├── workflow_in_json.json
│
├── README.md             # Main documentation
```

---

## **Future Enhancements**
- Implement **SCD Type 2 optimization** with partitioning.
- Automate deployment using **Databricks Repos & GitHub Actions**.
- Improve **error handling & monitoring** using Databricks Jobs.

---

## **Contact**
For queries or contributions, contact [Your Name] at [Your Email].
