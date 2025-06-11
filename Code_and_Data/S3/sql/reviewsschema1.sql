drop table if exists hiveads.sales_part;
drop schema if exists hiveads ;
drop table if exists public.sales;

CREATE EXTERNAL DATABASE IF NOT EXISTS hiveads;  -- Moved this line

CREATE EXTERNAL SCHEMA hiveads
FROM DATA CATALOG
DATABASE 'hiveads'
IAM_ROLE 'arn:aws:iam::225989376206:role/service-role/AmazonRedshift-CommandsAccessRole-20250209T212925'
CREATE EXTERNAL database IF NOT EXISTS; -- Moved this line

create external table hiveads.sales_part(
salesid integer,
listid integer,
sellerid integer,
buyerid integer,
eventid integer,
dateid smallint,
qtysold smallint,
pricepaid decimal(8,2),
commission decimal(8,2),
saletime timestamp)
partitioned by (saledate date)
row format delimited
fields terminated by '|'
stored as textfile
location 's3://mysourcebucket-project3/src_files_unzip_upload_to_bucket_with_same_folder_structure/'  -- Base location
table properties ('numRows'='170000');

-- No need for individual ALTER TABLE ADD PARTITION statements.  Redshift Spectrum will discover them.

create table public.sales(
salesid integer,
listid integer,
sellerid integer,
buyerid integer,
eventid integer,
dateid smallint,
qtysold smallint,
pricepaid decimal(8,2),
commission decimal(8,2),
saletime timestamp,
saledate varchar(10))
SORTKEY (saledate);

-- Insert data from the external table into the internal table
INSERT INTO public.sales
SELECT salesid, listid, sellerid, buyerid, eventid, dateid, qtysold, pricepaid, commission, saletime, saledate::varchar(10)
FROM hiveads.sales_part;