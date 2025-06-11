drop table if exists hiveads.sales_part;
drop schema if exists hiveads ;
drop table if exists public.sales;

CREATE EXTERNAL SCHEMA  hiveads
from data catalog
database 'hiveads'
iam_role 'arn:aws:iam::225989376206:role/service-role/AmazonRedshift-CommandsAccessRole-20250209T212925'
CREATE EXTERNAL database IF NOT EXISTS;

create external table  hiveads.sales_part(
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
location 's3://mysourcebucket-project3/src_files_unzip_upload_to_bucket_with_same_folder_structure/'
table properties ('numRows'='170000');

alter table hiveads.sales_part
add if not exists partition (saledate='2008-01-01')
location 's3://mysourcebucket-project3/src_files_unzip_upload_to_bucket_with_same_folder_structure/saledate=2008-01/';
alter table hiveads.sales_part
add if not exists partition (saledate='2008-03-01')
location 's3://mysourcebucket-project3/src_files_unzip_upload_to_bucket_with_same_folder_structure/saledate=2008-03/';
alter table hiveads.sales_part
add if not exists partition (saledate='2008-04-01')
location 's3://mysourcebucket-project3/src_files_unzip_upload_to_bucket_with_same_folder_structure/saledate=2008-04/';

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
  SORTKEY (
     saledate
    );
