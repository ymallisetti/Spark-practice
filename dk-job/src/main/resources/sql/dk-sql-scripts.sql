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