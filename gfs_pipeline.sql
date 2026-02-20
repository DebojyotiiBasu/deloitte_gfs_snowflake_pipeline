CREATE WAREHOUSE GFS_WH WITH WAREHOUSE_SIZE = 'XSMALL';
CREATE DATABASE GFS_DEMO;
USE DATABASE GFS_DEMO;
CREATE SCHEMA RAW;

--Creating Company table with Sample Data
USE SCHEMA RAW;
CREATE OR REPLACE TABLE COMPANIES AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY SEQ4()) as company_id,
    CASE MOD(ABS(HASH(SEQ4())), 10)
        WHEN 0 THEN 'Apple Inc'
        WHEN 1 THEN 'Apple'
        WHEN 2 THEN 'Google LLC'
        WHEN 3 THEN 'Google'
        WHEN 4 THEN 'Microsoft Corp'
        WHEN 5 THEN 'Microsoft'
        WHEN 6 THEN 'Amazon Web Services'
        WHEN 7 THEN 'AWS'
        WHEN 8 THEN 'Tesla Motors'
        ELSE 'Company ' || ROW_NUMBER() OVER (ORDER BY SEQ4())
    END as company_name,
    
    CASE MOD(ABS(HASH(SEQ4())), 5)
        WHEN 0 THEN 'Technology'
        WHEN 1 THEN 'Finance'
        WHEN 2 THEN 'Healthcare'
        WHEN 3 THEN 'Retail'
        WHEN 4 THEN 'Auto'
    END as industry,
    
    CASE MOD(ABS(HASH(SEQ4())), 5)
        WHEN 0 THEN 'USA'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'India'
        WHEN 3 THEN 'Germany'
        WHEN 4 THEN 'Canada'
    END as country,
    
    100 + MOD(ABS(HASH(SEQ4())), 500000) as revenue_millions,
    50 + MOD(ABS(HASH(SEQ4())), 100000) as employees,
    'company' || ROW_NUMBER() OVER (ORDER BY SEQ4()) || '.com' as website,
    
    CASE MOD(ABS(HASH(SEQ4())), 4)
        WHEN 0 THEN 'Crunchbase'
        WHEN 1 THEN 'D&B'
        WHEN 2 THEN 'OpenCorporates'
        WHEN 3 THEN 'Internal'
    END as data_source

FROM TABLE(GENERATOR(ROWCOUNT => 50));

SELECT * FROM COMPANIES ;
SELECT COUNT(*) as total_records, 
       MIN(revenue_millions) as min_revenue,
       MAX(revenue_millions) as max_revenue
FROM COMPANIES;



--Data Quality Check :
SELECT 
    'Total Records' as metric, COUNT(*) as value 
FROM COMPANIES
UNION ALL
SELECT 'Unique Companies', COUNT(DISTINCT company_name) 
FROM COMPANIES
UNION ALL
SELECT 'Duplicate Names Count', COUNT(*) 
FROM (SELECT company_name FROM COMPANIES GROUP BY company_name HAVING COUNT(*) > 1);


--Outlier Detection:
WITH stats AS (
    SELECT 
        AVG(revenue_millions) as mean_rev, STDDEV(revenue_millions) as std_rev,
        AVG(employees) as mean_emp, STDDEV(employees) as std_emp
    FROM COMPANIES
)
SELECT 
    c.company_id, c.company_name,
    c.revenue_millions,
    c.employees,
    ROUND((c.revenue_millions - s.mean_rev) / s.std_rev, 2) as rev_zscore,
    ROUND((c.employees - s.mean_emp) / s.std_emp, 2) as emp_zscore,
    CASE 
        WHEN ABS((c.revenue_millions - s.mean_rev) / s.std_rev) > 1.5 
        OR ABS((c.employees - s.mean_emp) / s.std_emp) > 1.5 
        THEN  'OUTLIER'
        ELSE 'NORMAL'
    END as status
FROM COMPANIES c, stats s
WHERE ABS((c.revenue_millions - s.mean_rev) / s.std_rev) > 1.5 
   OR ABS((c.employees - s.mean_emp) / s.std_emp) > 1.5 
ORDER BY ABS(rev_zscore) DESC, ABS(emp_zscore) DESC
;



--Validation Pipeline:
CREATE SCHEMA IF NOT EXISTS VALIDATED;
CREATE OR REPLACE TABLE VALIDATED.COMPANIES_CLEAN AS
WITH duplicates AS (
    SELECT company_name FROM COMPANIES 
    GROUP BY company_name HAVING COUNT(*) > 1
),
revenue_outliers AS (
    SELECT company_id 
    FROM COMPANIES c
    CROSS JOIN (SELECT AVG(revenue_millions) mean_rev, STDDEV(revenue_millions) std_rev FROM COMPANIES) rs
    WHERE ABS((c.revenue_millions - rs.mean_rev) / rs.std_rev) > 1.5
),
emp_outliers AS (
    SELECT company_id 
    FROM COMPANIES c
    CROSS JOIN (SELECT AVG(employees) mean_emp, STDDEV(employees) std_emp FROM COMPANIES) es
    WHERE ABS((c.employees - es.mean_emp) / es.std_emp) > 1.5
)
SELECT 
    c.*,
    CASE 
        WHEN c.company_id IN (SELECT company_id FROM revenue_outliers) 
             OR c.company_id IN (SELECT company_id FROM emp_outliers) 
        THEN 'OUTLIER_DETECTED'
        WHEN c.company_name IN (SELECT company_name FROM duplicates) 
        THEN 'DUPLICATE_DETECTED'
        ELSE 'VERIFIED'
    END as validation_status,
    CURRENT_TIMESTAMP() as validated_at
FROM COMPANIES c;

--Result Summary:
SELECT validation_status, COUNT(*) as records
FROM VALIDATED.COMPANIES_CLEAN
GROUP BY validation_status
ORDER BY records DESC;

--Duplicates:
SELECT *
FROM VALIDATED.COMPANIES_CLEAN
where validation_status = 'DUPLICATE_DETECTED';


--Exported Results to CSV:
SELECT * FROM VALIDATED.COMPANIES_CLEAN 
WHERE validation_status != 'VERIFIED'
ORDER BY validation_status;

