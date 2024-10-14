## Practicum II TASK2
## Author: Edie
## Date: 2024-08-02  

# Load necessary libraries
library(DBI)
library(RSQLite)
library(RMySQL)

# Connect to the SQLite database
conSQLite <- dbConnect(RSQLite::SQLite(), dbname = "pharma_data_warehouse.sqlite")

# Connect to the MySQL database
# Set database connection parameters
db_name_fh <- "sql3723989"
db_user_fh <- "sql3723989"
db_host_fh <- "sql3.freemysqlhosting.net"
db_pwd_fh <- "LPCtnDnj61"
db_port_fh <- 3306

# Connect to the remote server database
mydb.fh <-  dbConnect(RMySQL::MySQL(), user = db_user_fh, password = db_pwd_fh,
                      dbname = db_name_fh, host = db_host_fh, port = db_port_fh)
mydb <- mydb.fh

# reset everything
dbExecute(mydb, "DROP TABLE IF EXISTS sales_facts")
dbExecute(mydb, "DROP TABLE IF EXISTS reps_facts")
dbExecute(mydb, "DROP TABLE IF EXISTS product_dim")
dbExecute(mydb, "DROP TABLE IF EXISTS time_dim")

# create dimension tables
dbExecute(mydb, "
CREATE TABLE IF NOT EXISTS time_dim (
  date_id INT AUTO_INCREMENT PRIMARY KEY,
  date DATE,
  month INT,
  quarter INT,
  year INT
)")

dbExecute(mydb, "
CREATE TABLE IF NOT EXISTS product_dim (
  productID INT PRIMARY KEY, 
  product_name VARCHAR(255) NOT NULL UNIQUE, 
  unitcost DECIMAL(10,2),
  currency VARCHAR(3)
)")

# create fact tables
dbExecute(mydb, "
CREATE TABLE IF NOT EXISTS sales_facts (
  sale_fact_id INT AUTO_INCREMENT PRIMARY KEY,
  date_id INT,
  productID INT,
  country VARCHAR(255),
  total_amount DECIMAL(10, 2),
  total_units INT,
  FOREIGN KEY (date_id) REFERENCES time_dim(date_id),
  FOREIGN KEY (productID) REFERENCES product_dim(productID),
  FOREIGN KEY (country) REFERENCES country_dim(country_name)
)")

dbExecute(mydb, "
CREATE TABLE IF NOT EXISTS reps_facts (
  fact_id INT AUTO_INCREMENT PRIMARY KEY,
  rep_name VARCHAR(255) NOT NULL,
  repID INT NOT NULL,
  date_id INT NOT NULL,
  total_sales_amount DECIMAL(10, 2) NOT NULL,
  average_sales_amount DECIMAL(10, 2) NOT NULL,
  FOREIGN KEY (date_id) REFERENCES time_dim(date_id)
)")

# Retrieve list of sales tables
sales_tables <- dbGetQuery(conSQLite, "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'sales_%'")$name

# loading data for product dimension
product_data <- dbGetQuery(conSQLite, "SELECT DISTINCT productID, product_name, unitcost, currency FROM products")
dbWriteTable(mydb, 'product_dim', product_data, append=TRUE, row.names=FALSE)
print(dbGetQuery(mydb, "SELECT * FROM product_dim"))

# loading data for time_dim
for (sales_table in sales_tables) {
  time_data <- dbGetQuery(conSQLite, sprintf("
    SELECT DISTINCT 
      strftime('%%Y%%m%%d', date) AS date_id,
      date,
      strftime('%%m', date) AS month,
      (CAST(strftime('%%m', date) AS INTEGER) + 2) / 3 AS quarter,
      strftime('%%Y', date) AS year
    FROM %s
  ", sales_table))
  dbWriteTable(mydb, 'time_dim', time_data, append=TRUE, row.names=FALSE)
}
print(dbGetQuery(mydb, "SELECT * FROM time_dim"))

# Create the date_mapping object
time_dim_data <- dbGetQuery(mydb, "SELECT date_id, date FROM time_dim")
date_mapping <- setNames(time_dim_data$date_id, as.character(time_dim_data$date))

# loading data for sales_facts from each sales table
for (sales_table in sales_tables) {
  sales_facts_data <- dbGetQuery(conSQLite, sprintf("
    SELECT
      s.date,
      p.productID,
      s.country,
      SUM(s.qty * p.unitcost) AS total_amount,
      SUM(s.qty) AS total_units
    FROM %s s
    LEFT JOIN products p ON s.product_name = p.product_name
    GROUP BY s.date, p.productID, s.country
  ", sales_table))
  
  sales_facts_data$date <- format(as.Date(sales_facts_data$date), "%Y-%m-%d")
  sales_facts_data$date_id <- date_mapping[as.character(sales_facts_data$date)]
  sales_facts_data$date <- NULL
  
  print(sales_facts_data)
  
  if (nrow(sales_facts_data) > 0) {
    dbBegin(mydb)  
    tryCatch({
      dbWriteTable(mydb, 'sales_facts', sales_facts_data, append=TRUE, row.names=FALSE)
      dbCommit(mydb)  
    }, error = function(e) {
      dbRollback(mydb) 
      print("Error during data insertion:")
      print(e)
    })
  }
}

# print and check
result <- dbGetQuery(mydb, "SELECT * FROM sales_facts")
print(result)

# loading data for reps_facts from each sales table
for (sales_table in sales_tables) {
  reps_facts_data <- dbGetQuery(conSQLite, sprintf("
    SELECT
      s.repID,
      r.first_name || ' ' || r.surname AS rep_name,
      s.date,
      SUM(s.qty * p.unitcost) AS total_sales_amount,
      AVG(s.qty * p.unitcost) AS average_sales_amount
    FROM %s s
    LEFT JOIN products p ON s.product_name = p.product_name
    LEFT JOIN reps r ON s.repID = r.repID
    GROUP BY s.repID, r.first_name, r.surname, s.date
  ", sales_table))
  
  reps_facts_data$date <- format(as.Date(reps_facts_data$date), "%Y-%m-%d")
  reps_facts_data$date_id <- date_mapping[as.character(reps_facts_data$date)]
  reps_facts_data$date <- NULL
  
  print(reps_facts_data)
  if (nrow(reps_facts_data) > 0) {
    dbBegin(mydb)  
    tryCatch({
      dbWriteTable(mydb, 'reps_facts', reps_facts_data, append=TRUE, row.names=FALSE)
      dbCommit(mydb)  
    }, error = function(e) {
      dbRollback(mydb) 
      print("Error during data insertion:")
      print(e)
    })
  }
}

# print and check
result <- dbGetQuery(mydb, "SELECT * FROM reps_facts")
print(result)

# Query 1: Total Amount Sold in Each Quarter of 2023 for 'Alaraphosol'
query1 <- dbGetQuery(mydb, "SELECT 
  t.quarter,
  SUM(s.total_amount) AS total_amount_sold
FROM 
  sales_facts s
JOIN 
  time_dim t ON s.date_id = t.date_id
JOIN 
  product_dim p ON s.productID = p.productID
WHERE 
  t.year = 2023 AND p.product_name = 'Alaraphosol'
GROUP BY 
  t.quarter
ORDER BY 
  t.quarter;")
print(query1)

# Query 2: Sales Rep Who Sold the Most in 2022
query2 <- dbGetQuery(mydb, "SELECT 
  rep_name,
  SUM(total_sales_amount) AS total_sales
FROM 
  reps_facts
JOIN 
  time_dim ON reps_facts.date_id = time_dim.date_id
WHERE 
  time_dim.year = 2022
GROUP BY 
  rep_name
ORDER BY 
  total_sales DESC
LIMIT 1;")
print(query2)

# Query 3: Number of Units Sold in Brazil in 2022 for 'Alaraphosol'
query3 <- dbGetQuery(mydb, "
SELECT 
  SUM(s.total_units) AS total_units_sold
FROM 
  sales_facts s
JOIN 
  time_dim t ON s.date_id = t.date_id
JOIN 
  product_dim p ON s.productID = p.productID
WHERE 
  t.year = 2022 AND s.country = 'Brazil' AND p.product_name = 'Alaraphosol';
")
print(query3)

# Disconnect from databases
dbDisconnect(conSQLite)
dbDisconnect(mydb)

