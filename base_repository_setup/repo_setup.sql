create user profiling_automation identified by delphix;

grant dba to profiling_automation;

alter user profiling_automation account unlock;


create table profiling_automation.context_master(
context_id NUMBER GENERATED ALWAYS AS IDENTITY start with 1 increment by 1,
context_name varchar2(50),
status varchar2(50),
context_log varchar2(4000),
create_ts timestamp,
last_modified_ts timestamp
);

create table profiling_automation.profiling_runs(
profiling_run_id NUMBER GENERATED ALWAYS AS IDENTITY start with 1 increment by 1,
run_ts timestamp,
status varchar2(50)
);


create table profiling_automation.metadata_refreshes(
refresh_id NUMBER GENERATED ALWAYS AS IDENTITY start with 1 increment by 1,
refresh_ts timestamp,
status varchar2(50)
);


create table profiling_automation.metadata_inventory
(
inventory_id NUMBER GENERATED ALWAYS AS IDENTITY start with 1 increment by 1,
context_id NUMBER,
TABLE_NAME varchar2(50),
NUM_ROWS NUMBER(20),
COLUMN_NAME varchar2(50),
DATA_TYPE varchar2(100),
DATA_LENGTH NUMBER,
INVENTORY_STATUS varchar2(30),
INVENTORY_LOG varchar2(4000),
LAST_REFRESH_ID NUMBER,
LAST_PROFILING_RUN_ID NUMBER,
DOMAIN varchar2(50),
ALGORITHM_APPLIED varchar2(200),
REVIEWED VARCHAR2(10)
);

create index PROFILING_AUTOMATION.METADATA_INVENTORY_IDX1 on PROFILING_AUTOMATION.METADATA_INVENTORY(INVENTORY_ID);

create table profiling_automation.hyperscale_dataset_refreshes(
refresh_id NUMBER GENERATED ALWAYS AS IDENTITY start with 1 increment by 1,
refresh_ts timestamp,
status varchar2(50)
);

create table profiling_automation.hyperscale_datasets (
RECORD_ID NUMBER GENERATED ALWAYS AS IDENTITY start with 1 increment by 1,
CONTEXT_ID NUMBER,
HYPERSCALE_DATASET_ID NUMBER,
LAST_HYPERSCALE_DATASET_REFRESH_ID NUMBER
);

create table PROFILING_AUTOMATION.DATE_FORMATS (
DATE_FORMAT_ID NUMBER GENERATED ALWAYS AS IDENTITY start with 1 increment by 1,
CONTEXT_ID NUMBER,
COLUMN_NAME varchar2(200),
DATE_FORMAT varchar2(100)
);