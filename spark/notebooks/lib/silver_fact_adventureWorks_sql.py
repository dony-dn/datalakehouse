sql_select_bronze_sales_header = """ 
with cte_sales_header as
(
    select 
        SalesOrderNumber
        , ProductId
        , CustomerUsername
        , OrderDate
        , DueDate
        , ShipDate
        , SalesTerritoryRegion
        , SalesTerritoryCountry
        , SalesTerritoryContinent
        , Currency
        , source_filepath
        , ingestion_datetime
        , row_number() over(partition by SalesOrderNumber order by SalesOrderLineNumber) as sales_row_number
    from nessie.bronze.sales
)
select 
    SalesOrderNumber as CODE_SALES_ORDER_NUMBER
    , ProductId as ID_PRODUCT
    , CustomerUsername as ID_CUSTOMER
    , OrderDate as DATE_ORDER
    , DueDate as DATE_DUE
    , ShipDate as DATE_SHIP
    , SalesTerritoryRegion as NAME_SALES_TERRITORY_REGION
    , SalesTerritoryCountry as NAME_SALES_TERRITORY_COUNTRY
    , SalesTerritoryContinent as NAME_SALES_TERRITORY_CONTINENT
    , Currency as ID_CURRENCY
    , source_filepath as TEXT_SOURCE_FILEPATH
    , ingestion_datetime as DATETIME_INGESTION
from cte_sales_header
where sales_row_number = 1
"""

sql_create_silver_sales_header = """ 
CREATE TABLE IF NOT EXISTS nessie.silver.sales_header (
    CODE_SALES_ORDER_NUMBER STRING
    , ID_PRODUCT STRING
    , ID_CUSTOMER STRING
    , DATE_ORDER DATE
    , DATE_DUE DATE
    , DATE_SHIP DATE
    , NAME_SALES_TERRITORY_REGION STRING
    , NAME_SALES_TERRITORY_COUNTRY STRING
    , NAME_SALES_TERRITORY_CONTINENT STRING
    , ID_CURRENCY STRING
    , TEXT_SOURCE_FILEPATH STRING
    , DATETIME_INGESTION TIMESTAMP
) USING iceberg
"""
# ---------------------------------------------------------------
sql_select_bronze_sales_detail = """ 
with cte_sales_detail as
(
    select 
        SalesOrderNumber
        , SalesOrderLineNumber
        , ProductId
        , CustomerUsername
        , OrderDate
        , DueDate
        , ShipDate
        , SalesTerritoryRegion
        , SalesTerritoryCountry
        , SalesTerritoryContinent
        , OrderQuantity
        , SalesAmount
        , TaxAmt
        , Freight
        , Currency
        , AverageRate
        , source_filepath
        , ingestion_datetime
    from nessie.bronze.sales
)
select 
    SalesOrderNumber as CODE_SALES_ORDER_NUMBER
    , SalesOrderLineNumber as NUM_SALES_LINE
    , OrderQuantity as NUM_ORDER_QUANTITY
    , SalesAmount as AMT_SALES
    , TaxAmt as AMT_TAX
    , Freight as NUM_FREIGHT
    , Currency as ID_CURRENCY
    , source_filepath as TEXT_SOURCE_FILEPATH
    , ingestion_datetime as DATETIME_INGESTION
from cte_sales_detail
"""

sql_create_silver_sales_detail = """ 
CREATE TABLE IF NOT EXISTS nessie.silver.sales_detail (
    CODE_SALES_ORDER_NUMBER STRING
    , NUM_SALES_LINE INT
    , NUM_ORDER_QUANTITY INT
    , AMT_SALES DOUBLE
    , AMT_TAX DOUBLE
    , NUM_FREIGHT DOUBLE
    , ID_CURRENCY STRING
    , TEXT_SOURCE_FILEPATH STRING
    , DATETIME_INGESTION TIMESTAMP
) USING iceberg
"""
# ---------------------------------------------------------------
sql_select_bronze_currency_rate_history = """ 
select 
    currency as ID_CURRENCY
    , date as DATE_CURRENCY_RATE
    , rate as NUM_CURRENCY_RATE
    , source_filepath as TEXT_SOURCE_FILEPATH
    , ingestion_datetime as DATETIME_INGESTION
from nessie.bronze.currency_rate
"""
sql_create_silver_currency_rate_history = """ 
CREATE TABLE IF NOT EXISTS nessie.silver.currency_rate_history (
    ID_CURRENCY STRING
    , DATE_CURRENCY_RATE DATE
    , NUM_CURRENCY_RATE DOUBLE
    , TEXT_SOURCE_FILEPATH STRING
    , DATETIME_INGESTION TIMESTAMP
) USING iceberg
"""
# ---------------------------------------------------------------
