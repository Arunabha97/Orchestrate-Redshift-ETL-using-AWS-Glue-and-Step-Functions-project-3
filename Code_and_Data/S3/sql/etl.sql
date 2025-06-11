truncate table public.sales;
INSERT INTO public.sales
SELECT * FROM hiveads.sales_part where qtysold>=5;


