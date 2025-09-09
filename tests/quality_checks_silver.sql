

SELECT TOP (1000) [prd_id],
	 REPLACE(SUBSTRING (prd_key, 1, 5), '-', '_') as cat_id,
	 substring(prd_key, 7, len(prd_key)) as prd_key,
      [prd_name],
      ISNULL (prd_cost, 0) as prd_cost,
     case upper(trim(prd_line)) 
		when 'M' then 'Mountain'
		when  'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a'
end as prd_line,
      CAST([prd_start_date] as date) as prd_start_date
      ,cast(LEAD(prd_start_date) over (partition by prd_key order by prd_start_date)-1 as date) as prd_end_dt_test
  FROM [DataWarehouse].[silver].[crm_prd_info]
 -- where REPLACE(SUBSTRING (prd_key, 1, 5), '-', '_')
  --not in (select distinct id from silver.erp_px_cat_g1v2)


 

  select cst_id
  from silver.crm_cust_info
  where cst_id is null or cst_id = 0

select cst_id,
count(*) from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

select * from(
select * ,
row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from silver.crm_cust_info
where cst_id is not null

) t where flag_last = 1
  

--check for unwanted spaces
select
trim(cst_key) cst_key,
trim(cst_firstname) cst_firstname,
trim(cst_lastname) cst_lastname

from silver.crm_cust_info

select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)

select distinct
cst_firstname,cst_key,
case when upper(trim(cst_marital_status)) = 'M' then 'Married'
when upper(trim(cst_marital_status)) = 'S' then 'Single'

else 'n/a'

end cst_marital_status

from silver.crm_cust_info

select distinct 
cst_firstname,
case when upper(trim(cst_gndr)) = 'M' then 'Male'
	 when upper(trim(cst_gndr)) = 'F' then 'Female'
	 else 'n/a'

end cst_gndr
from silver.crm_cust_info
