# Pharmaceutical Sales Data Warehouse Project

## Project Overview

This project involves building a data warehouse to analyze pharmaceutical sales data. The data is extracted from XML files and loaded into a MySQL database using **R**. The warehouse is designed using a **Kimball Dimensional Table approach** with a **star schema** to optimize for **fast analytical queries** and reporting. The warehouse supports insights into **top-selling products**, **sales trends by representative**, and **sales performance by country** and quarter. The project also focuses on optimizing query performance through **data partitioning**.

## Key Features

- **Data Modeling**: Designed and implemented **fact tables** for products, sales representatives, and regions to support efficient data retrieval.
- **ETL Process**: Loaded **XML sales transaction data** into a **MySQL database** using R and transformed it to improve performance and scalability.
- **Partitioning**: Split large sales tables by year for improved query performance.
- **Data Analysis**: Generated SQL queries to analyze top-selling products, sales trends, and performance metrics.
- **Reporting**: Created visual reports using **ggplot2** and **kableExtra** in R to present key insights.

## Technologies Used

- **R**: For data extraction, transformation, and loading (ETL), and for running SQL queries.
- **MySQL**: For building and managing the database.
- **ggplot2 & kableExtra**: For visualizing data and creating formatted tables in R.
- **XML**: Sales transaction data is loaded from XML files.

## Project Structure

- `LoadXML2DB.R`: This script handles loading XML data into a relational schema in the MySQL database.
- `CreateStarSchema.R`: This script is responsible for creating and populating the star schema, including the fact tables.
- `AnalyzeData.Rmd`: An R Markdown file used for running analytical queries and generating reports.

## How to Run the Project

1. Clone the repository to your local machine.
2. Set up a **MySQL** database and configure your connection details.
3. Run `LoadXML2DB.R` to load the XML data into the database.
4. Run `CreateStarSchema.R` to create and populate the star schema.
5. Open and run `AnalyzeData.Rmd` in **RStudio** to perform the analysis and generate visual reports.

## Analytical Queries

1. **Top Five Products by Sales**: Identifies the top five products generating the most revenue across all years.
2. **Revenue and Units Sold Per Quarter**: Analyzes total revenue and units sold for each product by quarter.
3. **Sales Performance by Country**: Reports total sales revenue per product by country, visualized in a line chart.
4. **Sales Representative Performance**: Displays the average sales per representative for each quarter.

## Reports

A detailed analysis of sales data is available in the PDF report. You can view the report [here](docs/SalesAnalysis.pdf).

## Future Improvements

- Implement advanced partitioning techniques for handling larger datasets.
- Enhance the data mining capabilities with more complex queries and machine learning integration.

## Contact

For any inquiries, please contact: Yedian Cheng at [chengyedian@gmail.com].
