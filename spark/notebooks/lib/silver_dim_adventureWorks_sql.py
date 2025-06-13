sql_select_bronze_product = """ 
select 
    ProductId as ID_PRODUCT
    , ProductName as NAME_PRODUCT
    , ProductSubcategory as NAME_PRODUCT_SUBCATEGORY
    , ProductCategory as NAME_PRODUCT_CATEGORY
    , ProductDescription as TEXT_DESCRIPTION
    , Color as NAME_COLOR
    , StandardCost as AMT_PRODUCT_COST
    , source_filepath as TEXT_SOURCE_FILEPATH
    , ingestion_datetime as DATETIME_INGESTION
from nessie.bronze.product
"""

sql_create_silver_product = """ 
CREATE TABLE IF NOT EXISTS nessie.silver.product (
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
    , BirthDate as DATE_BIRTH
    , MaritalStatus as TYPE_MARITAL_STATUS
    , Gender as TYPE_GENDER
    , YearlyIncome as AMT_YEARLY_INCOME
    , Education as NAME_EDUCATION
    , Occupation as NAME_OCCUPATION
    , HouseOwnerFlag as FLAG_HOUSE_OWNER
    , TotalChildren as NUM_CHILDREN_TOTAL
    , NumberChildrenAtHome as NUM_CHILDREN_AT_HOME
    , NumberCarsOwned as NUM_CARS_OWNED
    , DateFirstPurchase as DATE_FIRST_PURCHASE
    , CommuteDistance as TYPE_COMMUTE_DISTANCE
    , source_filepath as TEXT_SOURCE_FILEPATH
    , ingestion_datetime as DATETIME_INGESTION
from nessie.bronze.customer
"""

sql_create_silver_customer = """ 
CREATE TABLE IF NOT EXISTS nessie.silver.customer (
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