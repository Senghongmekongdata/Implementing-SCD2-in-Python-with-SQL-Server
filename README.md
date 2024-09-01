# IMPLEMENTING SCD2 IN PYTHON WITH SQL SERVER

## SLOWLY CHANGE DIMENSION (SCD)
A Slowly Changing Dimension (SCD) is a design pattern in data warehousing that manages and tracks changes to dimension data over time. Specifically, a Slowly Changing Dimension Type 2 (SCD Type 2) keeps a full history of changes by creating a new row each time a change occurs, while marking the previous record as outdated. This allows you to maintain historical accuracy and analyze data as it was during different periods.

### Key Components of SCD Type 2
***Surrogate Key:*** A unique identifier for each record, typically an auto-incrementing integer.

***Business Key (Natural Key):*** This is the primary key used in the source system, such as customer_id. It uniquely identifies the entity in the source system but may be duplicated in the dimension table due to versioning.

***Attributes:*** These are the descriptive details of the entity (e.g., first_name, last_name, email).

***Effective Date:*** Indicates when the record became active.

***Expiry Date:*** Marks the date when the record was superseded by a newer version.

***Current Indicator:*** A flag (typically a bit or boolean) that indicates whether the record is the most recent version (1 for current, 0 for historical).

## SCD Type 2 Process Flow
- **New Record Insertion:**

  When a new entity is introduced in the source system, a new record is inserted into the dimension table with the is_current flag set to 1.

- **Updating an Existing Record:**

  When a change occurs in the source data, a new record is inserted with updated attribute values. The effective_date is set to the current timestamp.
The previous record's is_current flag is set to 0, and the expiry_date is updated to reflect when it became obsolete.



## IMPLEMENTING SCD TYPE 2
Here’s an example schema for an SCD Type 2 implementation:
1. CREATE DATABASE AND TABLE IN SQL SERVER
```sql server
  -- Create a new database
CREATE DATABASE DWH_PROJECT
ON PRIMARY 
(
    NAME = YourDatabaseName_Data,
    FILENAME = 'D:\DWH_PROJECT\1_DATABASE\DWH_PROJECT.mdf',
    SIZE = 10MB,
    MAXSIZE = 100MB,
    FILEGROWTH = 5MB
)
LOG ON 
(
    NAME = YourDatabaseName_Log,
    FILENAME = 'D:\DWH_PROJECT\1_DATABASE\DWH_PROJECT.ldf',
    SIZE = 5MB,
    MAXSIZE = 50MB,
    FILEGROWTH = 5MB
);
GO 


USE DWH_PROJECT;

CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(15),
    date_of_birth DATE,
    address VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(10),
    country VARCHAR(50),
    effective_date DATETIME DEFAULT GETDATE(),
    expiry_date DATETIME NULL,
    is_current BIT DEFAULT 1
);

CREATE INDEX IX_dim_customer_customer_id_is_current
ON dim_customer(customer_id, is_current)
WHERE is_current = 1;
```
2. CREATE STORED PROCEDURE
```sql server
USE DWH_PROJECT
GO

CREATE PROCEDURE UpsertCustomer
    @customer_id INT,
    @first_name VARCHAR(50),
    @last_name VARCHAR(50),
    @email VARCHAR(100),
    @phone_number VARCHAR(15),
    @date_of_birth DATE,
    @address VARCHAR(100),
    @city VARCHAR(50),
    @state VARCHAR(50),
    @postal_code VARCHAR(10),
    @country VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @existing_customer_key INT;

    -- Check if the current version of the record exists
    SELECT @existing_customer_key = customer_key
    FROM dim_customer
    WHERE customer_id = @customer_id
      AND is_current = 1;

    -- If the record exists, compare and update if there are changes
    IF @existing_customer_key IS NOT NULL
    BEGIN
        -- Check for changes
        IF EXISTS (
            SELECT 1
            FROM dim_customer
            WHERE customer_key = @existing_customer_key
              AND (first_name <> @first_name OR
                   last_name <> @last_name OR
                   email <> @email OR
                   phone_number <> @phone_number OR
                   date_of_birth <> @date_of_birth OR
                   address <> @address OR
                   city <> @city OR
                   state <> @state OR
                   postal_code <> @postal_code OR
                   country <> @country)
        )
        BEGIN
            -- Expire the old record
            UPDATE dim_customer
            SET expiry_date = GETDATE(),
                is_current = 0
            WHERE customer_key = @existing_customer_key;

            -- Insert the new record
            INSERT INTO dim_customer (
                customer_id, first_name, last_name, email, phone_number, 
                date_of_birth, address, city, state, postal_code, country, 
                effective_date, is_current
            )
            VALUES (
                @customer_id, @first_name, @last_name, @email, @phone_number, 
                @date_of_birth, @address, @city, @state, @postal_code, @country, 
                GETDATE(), 1
            );
        END
    END
    ELSE
    BEGIN
        -- Insert a new record since it doesn't exist
        INSERT INTO dim_customer (
            customer_id, first_name, last_name, email, phone_number, 
            date_of_birth, address, city, state, postal_code, country, 
            effective_date, is_current
        )
        VALUES (
            @customer_id, @first_name, @last_name, @email, @phone_number, 
            @date_of_birth, @address, @city, @state, @postal_code, @country, 
            GETDATE(), 1
        );
    END
END
```

3. TESTING USING STORED PROCEDURE IN SQL SERVER
```sql server
EXEC UpsertCustomer
    @customer_id = 1,
    @first_name = 'John',
    @last_name = 'Doe',
    @email = 'john.doe@example.com',
    @phone_number = '123-456-7890',
    @date_of_birth = '1980-01-01',
    @address = '123 Main St',
    @city = 'New York',
    @state = 'NY',
    @postal_code = '10001',
    @country = 'USA';
```

4. IMPLEMENTING SCD 2 IN PYTHON BY UPLOADING DATA FROM CSV FILE
```python
import pandas as pd
import pyodbc


# Define the connection parameters
server_name = 'SENGHONG'
database_name = 'DWH_PROJECT'

# Establish a connection to SQL Server
conn = pyodbc.connect('Driver={SQL Server};'
                          f'Server={server_name};'
                          f'Database={database_name};'
                          'Trusted_Connection=yes;')

cursor = conn.cursor()

# Path to your CSV file
csv_file_path = 'customer_data.csv'

# Read CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Iterate over DataFrame rows and call the stored procedure
for index, row in df.iterrows():
    cursor.execute('''
        EXEC UpsertCustomer @customer_id=?, @first_name=?, @last_name=?, @email=?, @phone_number=?, @date_of_birth=?, @address=?, @city=?, @state=?, @postal_code=?, @country=?
    ''', 
    row['customer_id'], row['first_name'], row['last_name'], row['email'], 
    row['phone_number'], row['date_of_birth'], row['address'], 
    row['city'], row['state'], row['postal_code'], row['country'])

# Commit changes and close connection
conn.commit()
conn.close()

print("Data has been inserted into DimCustomer using the stored procedure.")
```
## ADVANTAGES AND CHALLENGES OF SCD 2
### 1. Advantages of SCD Type 2

  ***Historical Data Preservation:*** Allows you to see how dimensions looked at any point in time.
  
  ***Comprehensive Analysis:*** Facilitates time-based analyses, such as customer behavior before and after an event.

### 2. Challenges

  ***Data Volume:*** Keeping a full history increases the size of the dimension table.
  
  ***Complexity:*** ETL processes become more complex, requiring careful handling of changes and updates.
