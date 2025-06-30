sql_select_bronze_product = """ 
select 
    ProductId as ID_PRODUCT
    , ProductName as NAME_PRODUCT
    , ProductSubcategory as NAME_PRODUCT_SUBCATEGORY
    , ProductCategory as NAME_PRODUCT_CATEGORY
    , ProductDescription as TEXT_DESCRIPTION
    , Color as NAME_COLOR
    , CAST(StandardCost as DOUBLE) as AMT_PRODUCT_COST
    , file_path as TEXT_SOURCE_FILEPATH
    , CAST(ingest_timestamp as TIMESTAMP) as DATETIME_INGESTION
from bronze.PRODUCT
"""

sql_create_silver_product = """ 
CREATE TABLE IF NOT EXISTS silver.PRODUCT (
    ID_PRODUCT STRING
    , NAME_PRODUCT STRING
    , NAME_PRODUCT_SUBCATEGORY STRING
    , NAME_PRODUCT_CATEGORY STRING
    , TEXT_DESCRIPTION STRING
    , NAME_COLOR STRING
    , AMT_PRODUCT_COST DOUBLE
    , TEXT_SOURCE_FILEPATH STRING
    , DATETIME_INGESTION TIMESTAMP
) USING iceberg
"""
# ---------------------------------------------------------------
sql_select_bronze_customer = """ 
select 
    UserId as ID_USER
    , FirstName as NAME_FIRST
    , LastName as NAME_LAST
    , CAST(BirthDate as DATE) as DATE_BIRTH
    , MaritalStatus as TYPE_MARITAL_STATUS
    , Gender as TYPE_GENDER
    , CAST(YearlyIncome as DOUBLE) as AMT_YEARLY_INCOME
    , Education as NAME_EDUCATION
    , Occupation as NAME_OCCUPATION
    , CASE WHEN HouseOwnerFlag = "1" THEN TRUE ELSE FALSE END as FLAG_HOUSE_OWNER
    , CAST(TotalChildren AS INT) as NUM_CHILDREN_TOTAL
    , CAST(NumberChildrenAtHome AS INT) as NUM_CHILDREN_AT_HOME
    , CAST(NumberCarsOwned AS INT) as NUM_CARS_OWNED
    , CAST(DateFirstPurchase AS DATE) as DATE_FIRST_PURCHASE
    , CommuteDistance as TYPE_COMMUTE_DISTANCE
    , file_path as TEXT_SOURCE_FILEPATH
    , CAST(ingest_timestamp as TIMESTAMP) as DATETIME_INGESTION
from bronze.CUSTOMER
"""

sql_create_silver_customer = """ 
CREATE TABLE IF NOT EXISTS silver.CUSTOMER (
    ID_USER STRING
    , NAME_FIRST STRING
    , NAME_LAST STRING
    , DATE_BIRTH DATE
    , TYPE_MARITAL_STATUS STRING
    , TYPE_GENDER STRING
    , AMT_YEARLY_INCOME DOUBLE
    , NAME_EDUCATION STRING
    , NAME_OCCUPATION STRING
    , FLAG_HOUSE_OWNER BOOLEAN
    , NUM_CHILDREN_TOTAL INT
    , NUM_CHILDREN_AT_HOME INT
    , NUM_CARS_OWNED INT
    , DATE_FIRST_PURCHASE DATE
    , TYPE_COMMUTE_DISTANCE STRING
    , TEXT_SOURCE_FILEPATH STRING
    , DATETIME_INGESTION TIMESTAMP
) USING iceberg
"""

# ---------------------------------------------------------------
sql_select_bronze_currency = """ 
select 
    CurrencyAlternateKey as ID_CURRENCY
    , CurrencyName as NAME_CURRENCY
    , file_path as TEXT_SOURCE_FILEPATH
    , CAST(ingest_timestamp as TIMESTAMP) as DATETIME_INGESTION
from bronze.CURRENCY
"""

sql_create_silver_currency = """ 
CREATE TABLE IF NOT EXISTS silver.CURRENCY (
    ID_CURRENCY STRING
    , NAME_CURRENCY STRING
    , TEXT_SOURCE_FILEPATH STRING
    , DATETIME_INGESTION TIMESTAMP
) USING iceberg
"""