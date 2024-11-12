##1. Data Loading and Processing
Load data into DuckDB. The dbt scripts ensure data accuracy by detecting and removing duplicates. This establishes a reliable data foundation.

#2. Data Transformation with dbt
Staging Models: Use dbt to create staging models that:

Standardize column names
Cast data types
Filter out irrelevant columns
Fact and Dimension Tables: Designed to improve query efficiency and structure the data to support advanced analysis.

#3. SQL-Based Analysis
SQL queries were crafted in dbt, incorporating joins, subqueries, and window functions to analyze trends.
