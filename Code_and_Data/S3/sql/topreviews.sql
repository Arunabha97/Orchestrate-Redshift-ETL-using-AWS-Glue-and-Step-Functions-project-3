UNLOAD ('SELECT salesid, dateid, buyerid, sum(qtysold) as total_qtysold FROM public.sales GROUP BY dateid,salesid,buyerid order by total_qtysold desc')
TO 's3://my-redshift-bucket-project-3/output/'
iam_role 'arn:aws:iam::225989376206:role/service-role/AmazonRedshift-CommandsAccessRole-20250209T212925'
allowoverwrite 
CSV DELIMITER AS ',' 
header
parallel off;