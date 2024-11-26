COPY dev.public.salaries FROM 's3://mydbproject/project/cleaned_salaries.csv' IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843' FORMAT AS CSV DELIMITER ',' QUOTE '"' REGION AS 'us-east-2'


-- Copy data for industries table
COPY dev.public.industries
FROM 's3://mydbproject/project/cleaned_industries.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Copy data for skills table
COPY dev.public.skills
FROM 's3://mydbproject/project/cleaned_skills.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Copy data for benefits table
COPY dev.public.benefits
FROM 's3://mydbproject/project/cleaned_benefits.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Copy data for job_industries table
COPY dev.public.job_industries
FROM 's3://mydbproject/project/cleaned_job_industries.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

select * from sys_load_error_detail

-- Copy data for job_skills table
COPY dev.public.job_skills
FROM 's3://mydbproject/project/cleaned_job_skills.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Copy data for salaries table
COPY dev.public.salaries
FROM 's3://mydbproject/project/cleaned_salaries.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Copy data for companies table
COPY dev.public.companies
FROM 's3://mydbproject/project/cleaned_companies.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Copy data for company_industries table
COPY dev.public.company_industries
FROM 's3://mydbproject/project/cleaned_company_industries.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Copy data for company_specialities table
COPY dev.public.company_specialities
FROM 's3://mydbproject/project/cleaned_company_specialities.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Copy data for employee_counts table
COPY dev.public.employee_counts
FROM 's3://mydbproject/project/cleaned_employee_counts.csv'
IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843'
FORMAT AS CSV
DELIMITER ','
QUOTE '"'
REGION AS 'us-east-2';

-- Optional: Verify row counts after loading
SELECT 
    'postings' as table_name, COUNT(*) as row_count FROM dev.public.postings
UNION ALL
SELECT 'industries', COUNT(*) FROM dev.public.industries
UNION ALL
SELECT 'skills', COUNT(*) FROM dev.public.skills
UNION ALL
SELECT 'benefits', COUNT(*) FROM dev.public.benefits
UNION ALL
SELECT 'job_industries', COUNT(*) FROM dev.public.job_industries
UNION ALL
SELECT 'job_skills', COUNT(*) FROM dev.public.job_skills
UNION ALL
SELECT 'salaries', COUNT(*) FROM dev.public.salaries
UNION ALL
SELECT 'companies', COUNT(*) FROM dev.public.companies
UNION ALL
SELECT 'company_industries', COUNT(*) FROM dev.public.company_industries
UNION ALL
SELECT 'company_specialities', COUNT(*) FROM dev.public.company_specialities
UNION ALL
SELECT 'employee_counts', COUNT(*) FROM dev.public.employee_counts
ORDER BY table_name;