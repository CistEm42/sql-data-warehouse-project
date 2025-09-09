-- to execute a stored procedure
--exec silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	--LOADING INTO SILVER.CRM_CUST_INFO
	truncate table silver.crm_cust_info;
	print '>> Inserting data into: Silver.crm_cust_info'
	insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)

	SELECT [cst_id]
		  , trim(cst_key) cst_key,
			trim(cst_firstname) cst_firstname,
			trim(cst_lastname) cst_lastname
		  ,case when upper(trim(cst_marital_status)) = 'M' then 'Married'
				when upper(trim(cst_marital_status)) = 'S' then 'Single'
				else 'n/a'
	end cst_marital_status
		  ,case when upper(trim(cst_gndr)) = 'M' then 'Male'
		 when upper(trim(cst_gndr)) = 'F' then 'Female'
		 else 'n/a'

	end cst_gndr
		  ,[cst_create_date]
	  FROM [DataWarehouse].[bronze].[crm_cust_info]


	  --LOADING INTO SILVER.CRM_PRD_INFO
	truncate table silver.crm_prd_info;
	print '>> Inserting data into: Silver.crm_prd_info'
	  insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key ,
	prd_name ,
	prd_cost,
	prd_line,
	prd_start_date,
	prd_end_date

	)

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
	  FROM [DataWarehouse].[bronze].[crm_prd_info]


	  --LOADING INTO SILVER.CRM_SALES_DETAILS
	 truncate table silver.crm_sales_details;
	print '>> Inserting data into: Silver.crm_sales_details'
	  insert into silver.crm_sales_details(
	sls_ord_num ,
	sls_prd_key,
	sls_cust_id ,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT  [sls_ord_num]
		  ,[sls_prd_key]
		  ,[sls_cust_id]
		  , case when sls_order_dt = 0 or LEN(sls_order_dt) != 8 then null
		  else CAST(cast(sls_order_dt as varchar) as date)
		  end as sls_order_dt
		  ,  case when sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 then null
		  else CAST(cast(sls_ship_dt as varchar) as date)
		  end as sls_ship_dt
		  ,  case when sls_due_dt = 0 or LEN(sls_due_dt) != 8 then null
		  else CAST(cast(sls_due_dt as varchar) as date)
		  end as sls_due_dt

		  ,case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
		then sls_quantity * abs(sls_price)
		else sls_sales
	end as sls_sales,
	  
		  [sls_quantity]

		  ,case when sls_price is null or sls_price <= 0
		then sls_Sales / nullif(sls_quantity,0)
		else sls_price
	end as sls_price
	  FROM [DataWarehouse].[bronze].[crm_sales_details]






	  --LOADING INTO SILVER.CRM_ERP_CUST_AZ12
	truncate table silver.erp_cust_az12;
	print '>> Inserting data into: Silver.erp_cust_az12'
	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen

	)
	SELECT 
			case when cid like 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			else cid
			end cid
		  ,
		  case when bdate > getdate() then null
		  else bdate
		  end bdate
		  ,
		  case when upper(trim(gen)) in ('F', 'Female') then 'Female'
				when upper(trim(gen)) in ('M', 'Male') then 'Male'
			else 'n/a'
			end gen
	  FROM [DataWarehouse].[bronze].[erp_cust_az12]



	--LOADING INTO SILVER.ERP_LOC_A101
	 truncate table silver.erp_loc_a101;
	print '>> Inserting data into: Silver.erp_loc_a101'
	 insert into silver.erp_loc_a101(
	cid,
	cntry
	)
	 select
	replace(cid, '-', '') cid,
	  case when trim(cntry) = 'DE' THEN 'Germany'
	when trim(cntry) in ('US', 'USA')  THEN 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else cntry
	end cntry
	  from bronze.erp_loc_a101

	  --LOADING INTO SILVER.ERP_PX_CAT
	  insert into silver.erp_px_cat_g1v2(
	  id,
	  cat,
	  subcat,
	  maintenance
	  )
		select
		id,
		cat,
		subcat,
		maintenance
		from silver.erp_px_cat_g1v2

END
