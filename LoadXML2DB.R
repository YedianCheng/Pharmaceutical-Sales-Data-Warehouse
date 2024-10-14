## Practicum II TASK1
## Author: Edie
## Date: 2024-08-02  

# Load necessary libraries
library(XML)
library(DBI)
library(RSQLite)

# Connect to SQLite database
con <- dbConnect(RSQLite::SQLite(), dbname = "pharma_data_warehouse.sqlite")

# Create tables
dbExecute(con, "DROP TABLE IF EXISTS reps")
dbExecute(con, "CREATE TABLE reps (
  repID INTEGER PRIMARY KEY,
  surname TEXT,
  first_name TEXT,
  phone TEXT,
  hire_date DATE,
  commission REAL,
  territory TEXT,
  certified BOOLEAN
)")

dbExecute(con, "DROP TABLE IF EXISTS products")
dbExecute(con, "CREATE TABLE products (
  productID INTEGER PRIMARY KEY AUTOINCREMENT, 
  product_name TEXT NOT NULL UNIQUE,  
  unitcost REAL,
  currency TEXT
)")

dbExecute(con, "DROP TABLE IF EXISTS customers")
dbExecute(con, "CREATE TABLE customers (
  customerID INTEGER PRIMARY KEY AUTOINCREMENT, 
  customer_name TEXT NOT NULL UNIQUE,  
  country TEXT
)")

dbExecute(con, "DROP TABLE IF EXISTS sales")
dbExecute(con, "CREATE TABLE sales (
  saleID INTEGER PRIMARY KEY AUTOINCREMENT,
  txnID INTEGER,
  repID INTEGER,
  customer_name TEXT,
  product_name TEXT, 
  country TEXT,
  date DATE,
  qty INTEGER,
  FOREIGN KEY(repID) REFERENCES reps(repID),
  FOREIGN KEY(customer_name) REFERENCES customers(customer_name),
  FOREIGN KEY(product_name) REFERENCES products(product_name)  
)")

# Function to load and process XML files based on their types
load_and_process_xml_files <- function(folder_path, con) {
  xml_files <- list.files(path = folder_path, pattern = "\\.xml$", full.names = TRUE)
  
  if (length(xml_files) == 0) {
    print("No XML files found in the specified folder.")
    return()
  }
  
  print("Processing the following XML files:")
  print(xml_files)
  
  for (file_path in xml_files) {
    print(paste("Currently processing:", file_path))
    tryCatch({
      if (grepl("pharmaReps", basename(file_path))) {
        print("Identified as a reps file.")
        load_reps_data(file_path, con)
      } else if (grepl("pharmaSalesTxn", basename(file_path))) {
        print("Identified as a transaction file.")
        load_txn_data(file_path, con)
      } else {
        print("File does not match known patterns.")
      }
    }, error = function(e) {
      print(paste("Error processing file:", file_path, "Error:", e$message))
    })
  }
}

# Function to parse and load sales reps data
load_reps_data <- function(file, con) {
  doc <- xmlParse(file)
  reps <- getNodeSet(doc, "//rep")
  
  # Parse data for each rep
  reps_data <- lapply(reps, function(rep) {
    demo <- getNodeSet(rep, "./demo")[[1]]
    data.frame(
      repID = as.integer(sub("r", "", xmlGetAttr(rep, "rID"))),
      surname = xmlValue(getNodeSet(demo, "./sur")[[1]]),
      first_name = xmlValue(getNodeSet(demo, "./first")[[1]]),
      phone = xmlValue(getNodeSet(demo, "./phone")[[1]]),
      hire_date = as.Date(xmlValue(getNodeSet(demo, "./hiredate")[[1]]), "%b %d %Y"),
      commission = as.numeric(sub("%", "", xmlValue(getNodeSet(rep, "./commission")[[1]]))) / 100,
      territory = xmlValue(getNodeSet(rep, "./territory")[[1]]),
      certified = length(getNodeSet(rep, "./certified")) > 0,
      stringsAsFactors = FALSE
    )
  })
  reps_df <- do.call(rbind, reps_data)
  
  # Insert into the database
  dbWriteTable(con, "reps", reps_df, append = TRUE, row.names = FALSE)
}

# Function to process transaction data and handle products and customers
load_txn_data <- function(file, con) {
  doc <- xmlParse(file)
  txns <- getNodeSet(doc, "//txn")
  
  for (txn in txns) {
    # Extract basic and sale attributes
    txnID <- as.integer(xmlGetAttr(txn, "txnID"))
    repID <- as.integer(xmlGetAttr(txn, "repID"))
    customer_name <- xmlValue(getNodeSet(txn, "./customer")[[1]])
    product_name <- xmlValue(getNodeSet(txn, "./sale/product")[[1]])
    unitcost <- as.numeric(xmlValue(getNodeSet(txn, "./sale/unitcost")[[1]]))
    currency <- xmlGetAttr(getNodeSet(txn, "./sale/unitcost")[[1]], "currency")
    country <- xmlValue(getNodeSet(txn, "./country")[[1]])
    date <- as.Date(xmlValue(getNodeSet(txn, "./sale/date")[[1]]), "%m/%d/%Y")
    qty <- as.integer(xmlValue(getNodeSet(txn, "./sale/qty")[[1]]))
    
    # Ensure product exists
    if (nrow(dbGetQuery(con, sprintf("SELECT 1 FROM products WHERE product_name = '%s'", product_name))) == 0) {
      dbExecute(con, sprintf("INSERT INTO products (product_name, unitcost, currency) VALUES ('%s', %f, '%s')", product_name, unitcost, currency))
    }
    
    # Ensure customer exists
    if (nrow(dbGetQuery(con, sprintf("SELECT 1 FROM customers WHERE customer_name = '%s'", customer_name))) == 0) {
      dbExecute(con, sprintf("INSERT INTO customers (customer_name, country) VALUES ('%s', '%s')", customer_name, country))
    }
    
    # Insert into sales
    dbExecute(con, sprintf("INSERT INTO sales (txnID, repID, customer_name, product_name, country, date, qty) VALUES (%d, %d, '%s', '%s', '%s', '%s', %d)",
                           txnID, repID, customer_name, product_name, country, as.character(date), qty))
  }
}

# Run the process
load_and_process_xml_files("txn-xml", con)

# Print out the tables to verify 
print(dbReadTable(con, "products"))
print(dbReadTable(con, "customers"))
print(dbReadTable(con, "reps"))
print(dbReadTable(con, "sales"))

# Query to fetch years from the sales table
years <- dbGetQuery(con, "SELECT DISTINCT strftime('%Y', date) AS year FROM sales")$year

for (year in years) {
  table_name <- sprintf("sales_%s", year)
  dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", table_name))
  query_create <- sprintf(
    "CREATE TABLE IF NOT EXISTS %s (
      saleID INTEGER PRIMARY KEY AUTOINCREMENT,
      txnID INTEGER,
      repID INTEGER,
      customer_name TEXT,
      product_name TEXT,
      country TEXT,
      date DATE,
      qty INTEGER,
      FOREIGN KEY(repID) REFERENCES reps(repID),
      FOREIGN KEY(customer_name) REFERENCES customers(customer_name),
      FOREIGN KEY(product_name) REFERENCES products(product_name)
    )", table_name)
  dbExecute(con, query_create)
  
  query_migrate <- sprintf(
    "INSERT INTO %s (txnID, repID, customer_name, product_name, country, date, qty)
    SELECT txnID, repID, customer_name, product_name, country, date, qty FROM sales WHERE strftime('%%Y', date) = '%s'",
    table_name, year)
  dbExecute(con, query_migrate)
}

# Query the tables to verify 
for (year in years) {
  table_name <- paste("sales", year, sep="_")
  query <- sprintf("SELECT COUNT(*) AS count FROM %s", table_name)
  count <- dbGetQuery(con, query)$count
  print(paste("Records in", table_name, ":", count))
}

# Drop the original sales table 
dbExecute(con, "DROP TABLE sales")

# Disconnect from the database
dbDisconnect(con)

