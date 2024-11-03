import pandas as pd
import sqlite3

# Load the data from the CSV file
data = pd.read_csv("supermarket_sales.csv")

# Combine Date and Time into a single Datetime column
data['sales_timestamp'] = pd.to_datetime(data['Date'] + ' ' + data['Time'])

# Drop the original Date and Time columns as they are now redundant
data = data.drop(columns=['Date', 'Time'])

# Creating the Branch Dimension Table
branch_dim = data[['Branch', 'City']].drop_duplicates().reset_index(drop=True)
branch_dim['Branch_ID'] = range(1, len(branch_dim) + 1)

# Rearrange columns to make Branch_ID the first column
branch_dim = branch_dim[['Branch_ID', 'Branch', 'City']]

# Creating the Product Dimension Table
product_dim = data[['Product line', 'Unit price']].drop_duplicates().reset_index(drop=True)
product_dim['Product_ID'] = range(1, len(product_dim) + 1)

# Rename columns for consistency
product_dim = product_dim.rename(columns={'Product line': 'Product_line', 'Unit price': 'Unit_price'})

# Rearrange columns to make Product_ID the first column
product_dim = product_dim[['Product_ID', 'Product_line', 'Unit_price']]


# Merging Branch_ID and Product_ID into the main data table
sales_data = data.merge(branch_dim, on=['Branch', 'City'], how='left') \
                 .merge(product_dim, left_on=['Product line', 'Unit price'], right_on=['Product_line', 'Unit_price'], how='left')

# Select and rename columns for the fact table
sales_data = sales_data[['Invoice ID', 'Branch_ID', 'Product_ID', 'Customer type', 'Gender', 
                         'Quantity', 'Tax 5%', 'Total', 'sales_timestamp', 'Payment', 'cogs', 
                         'gross margin percentage', 'gross income', 'Rating']]

# Rename columns for consistency
sales_data = sales_data.rename(columns={
    'Invoice ID': 'Invoice_ID',
    'Customer type': 'Customer_type',
    'Tax 5%': 'Tax_5_percent',
    'Payment': 'Payment_type',
    'cogs': 'COGS',
    'gross margin percentage': 'Gross_margin_percentage',
    'gross income': 'Gross_income'
})

# Save dimension and fact tables as CSV files if needed
branch_dim.to_csv("branch_dim.csv", index=False)
product_dim.to_csv("product_dim.csv", index=False)
sales_data.to_csv("sales_data.csv", index=False)


# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect("sales_data.db")
cursor = conn.cursor()

# Create tables in the database
cursor.execute("""
CREATE TABLE IF NOT EXISTS Branch (
    Branch_ID INTEGER PRIMARY KEY,
    Branch TEXT,
    City TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Product (
    Product_ID INTEGER PRIMARY KEY,
    Product_line TEXT,
    Unit_price REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Sales (
    Invoice_ID TEXT PRIMARY KEY,
    Branch_ID INTEGER,
    Product_ID INTEGER,
    Customer_type TEXT,
    Gender TEXT,
    Quantity INTEGER,
    Tax_5_percent REAL,
    Total REAL,
    Sales_timestamp TEXT,
    Payment TEXT,
    COGS REAL,
    Gross_margin_percentage REAL,
    Gross_income REAL,
    Rating REAL,
    FOREIGN KEY (Branch_ID) REFERENCES Branch (Branch_ID),
    FOREIGN KEY (Product_ID) REFERENCES Product (Product_ID)
)
""")

# Insert data into Branch table
branch_dim.to_sql('Branch', conn, if_exists='replace', index=False)

# Insert data into Product table
product_dim.to_sql('Product', conn, if_exists='replace', index=False)

# Insert data into Fact_Table
sales_data.to_sql('Sales', conn, if_exists='replace', index=False)

# Commit the transaction and close the connection
conn.commit()
conn.close()
