COPY dev.public.postings FROM 's3://mydbproject/project/cleaned_postings.csv' IAM_ROLE 'arn:aws:iam::339712854011:role/service-role/AmazonRedshift-CommandsAccessRole-20241125T034843' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-2'

select * from sys_load_error_detail