create external table jh_dk(
    ROWKEY STRING,
    C1 STRING,
    C2 STRING,
    C15 STRING) stored as parquet location '/dharmik/dkjob/tables/jh';
    
create external table jh_dk(ROWKEY STRING,C1 STRING,C2 STRING,C15 STRING) stored as parquet location '/dharmik/dkjob/tables/jh';
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create external table dq_exception_table(
	org_code STRING,
	application_id STRING,
	entity_name STRING,
	seq_id STRING,
	rule_pattern STRING,
	error_code STRING,
	error_description STRING,
	status STRING,
	created_datetime timestamp,
	scry_role_cd STRING,
	aud_load_id STRING,
	aud_load_tm STRING
) stored as parquet location '/dharmik/dkjob/tables/exceptions';

create external table dq_exception_table(org_code STRING,application_id STRING,entity_name STRING,seq_id STRING,rule_pattern STRING,error_code STRING,error_description STRING,status STRING,created_datetime timestamp,scry_role_cd STRING,aud_load_id STRING,aud_load_tm STRING) stored as parquet location '/dharmik/dkjob/tables/exceptions';
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
															PURCHASE ORDER TABLE CREATION SCRIPT
create external table dk_po_data(A01 STRING,A02 STRING,A03 STRING,A04 STRING,A05 STRING,A06 STRING,A07 STRING,A08 STRING,A09 STRING,A10 STRING,A11 STRING,A12 STRING,A13 STRING,A14 STRING) stored as parquet location '/dharmik/dkjob/tables/dk_data';
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT
A01 AS ADDRESS_ID,
A02 AS CITY,
A03 AS COUNTRY,
A04 AS POSTAL_CODE,
A05 AS STREET,
A07 AS PURCHASE_ORDER_ID,
A08 AS ORDER_DATE,
A09 AS CUSTOMER_ID,
A10 AS ITEM_ID,
A11 AS PRODUCT_NAME,
A12 AS QUANTITY,
A13 AS PRICE,
A14 AS CUSTOMER_NAME,
A06 AS ENTITY_NAME
FROM dk_po_data;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
