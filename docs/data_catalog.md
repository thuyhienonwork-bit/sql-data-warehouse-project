Gold Layer – Data Catalog
Overview
======================================================================
The Gold Layer represents the business-ready data model used for analytics and reporting.
It is built using a star schema, consisting of dimension tables and fact tables, optimized for 
performance, consistency, and ease of use by downstream consumers such as BI tools and data analysts.
=====================================================================

**1. gold.dim_customers**

Purpose
Stores customer details enriched with demographic and geographic attributes.

| Column Name     | Data Type    | Description                                              |
| --------------- | ------------ | -------------------------------------------------------- |
| customer_key    | INT          | Surrogate key uniquely identifying each customer record. |
| customer_id     | INT          | Unique numerical identifier assigned to each customer.   |
| customer_number | NVARCHAR(50) | Alphanumeric identifier representing the customer.       |
| first_name      | NVARCHAR(50) | Customer’s first name.                                   |
| last_name       | NVARCHAR(50) | Customer’s last name or family name.                     |
| country         | NVARCHAR(50) | Country of residence (e.g., `Australia`).                |
| marital_status  | NVARCHAR(50) | Marital status (e.g., `Married`, `Single`).              |
| gender          | NVARCHAR(50) | Gender of the customer (`Male`, `Female`, `n/a`).        |
| birthdate       | DATE         | Date of birth (`YYYY-MM-DD`).                            |
| create_date     | DATE         | Date the customer record was created.                    |


**📦 gold.dim_products**

Purpose
Provides information about products and their attributes.
| Column Name          | Data Type    | Description                                              |
| -------------------- | ------------ | -------------------------------------------------------- |
| product_key          | INT          | Surrogate key uniquely identifying each product.         |
| product_id           | INT          | Unique identifier assigned to the product.               |
| product_number       | NVARCHAR(50) | Structured alphanumeric product code.                    |
| product_name         | NVARCHAR(50) | Descriptive name of the product.                         |
| category_id          | NVARCHAR(50) | Identifier for the product’s category.                   |
| category             | NVARCHAR(50) | High-level classification (e.g., `Bikes`).               |
| subcategory          | NVARCHAR(50) | Detailed classification within the category.             |
| maintenance_required | NVARCHAR(50) | Indicates whether maintenance is required (`Yes`, `No`). |
| cost                 | INT          | Base cost of the product.                                |
| product_line         | NVARCHAR(50) | Product line (e.g., `Road`, `Mountain`).                 |
| start_date           | DATE         | Date when the product became available.                  |
| end_date             | DATE         | Date when the product was discontinued.                  |


**🧾 gold.fact_sales**

Purpose
Stores transactional sales data at the order line level.
| Column Name   | Data Type    | Description                                      |
| ------------- | ------------ | ------------------------------------------------ |
| order_number  | NVARCHAR(50) | Unique sales order identifier (e.g., `SO54496`). |
| product_key   | INT          | References `gold.dim_products.product_key`.      |
| customer_key  | INT          | References `gold.dim_customers.customer_key`.    |
| order_date    | DATE         | Date when the order was placed.                  |
| shipping_date | DATE         | Date when the order was shipped.                 |
| due_date      | DATE         | Payment due date.                                |
| sales_amount  | INT          | Total monetary value of the sales line item.     |
| quantity      | INT          | Number of units sold.                            |
| price         | INT          | Price per unit.                                  |


🔗 Relationships
