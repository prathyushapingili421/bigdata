CREATE TABLE IF NOT EXISTS postings (
    job_id BIGINT PRIMARY KEY,
    company_name VARCHAR(65535),
    title VARCHAR(65535),
    description VARCHAR(65535),
    max_salary VARCHAR(65535),
    pay_period VARCHAR(65535),
    location VARCHAR(65535),
    company_id FLOAT8 NULL,
    views VARCHAR(65535),
    med_salary VARCHAR(65535),
    min_salary VARCHAR(65535),
    formatted_work_type VARCHAR(65535),
    applies VARCHAR(65535),
    original_listed_time VARCHAR(65535),
    remote_allowed VARCHAR(65535),
    job_posting_url VARCHAR(65535),
    application_url VARCHAR(65535),
    application_type VARCHAR(65535),
    expiry VARCHAR(65535),
    closed_time VARCHAR(65535),
    formatted_experience_level VARCHAR(65535),
    skills_desc VARCHAR(65535),
    listed_time VARCHAR(65535),
    posting_domain VARCHAR(65535),
    sponsored VARCHAR(65535),
    work_type VARCHAR(65535),
    currency VARCHAR(65535),
    compensation_type VARCHAR(65535),
    normalized_salary VARCHAR(65535),
    zip_code VARCHAR(65535),
    fips VARCHAR(65535)
)
DISTSTYLE KEY
DISTKEY(job_id)
SORTKEY(job_id);

CREATE TABLE IF NOT EXISTS industries (
    industry_id INTEGER PRIMARY KEY,
    industry_name VARCHAR(65535)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS skills (
    skill_abr VARCHAR(65535) PRIMARY KEY,
    skill_name VARCHAR(65535)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS benefits (
    job_id BIGINT,
    inferred INTEGER,
    type VARCHAR(65535),
    PRIMARY KEY (job_id, type)
)
DISTSTYLE KEY
DISTKEY(job_id);

CREATE TABLE IF NOT EXISTS job_industries (
    job_id BIGINT,
    industry_id INTEGER,
    PRIMARY KEY (job_id, industry_id)
)
DISTSTYLE KEY
DISTKEY(job_id);

CREATE TABLE IF NOT EXISTS job_skills (
    job_id BIGINT,
    skill_abr VARCHAR(65535),
    PRIMARY KEY (job_id, skill_abr)
)
DISTSTYLE KEY
DISTKEY(job_id);

CREATE TABLE IF NOT EXISTS salaries (
    salary_id INTEGER PRIMARY KEY,
    job_id BIGINT,
    max_salary DOUBLE PRECISION,
    med_salary DOUBLE PRECISION,
    min_salary DOUBLE PRECISION,
    pay_period VARCHAR(65535),
    currency VARCHAR(65535),
    compensation_type VARCHAR(65535)
)
DISTSTYLE KEY
DISTKEY(job_id)
SORTKEY(salary_id);

drop table employee_counts
CREATE TABLE IF NOT EXISTS companies (
    company_id FLOAT8 PRIMARY KEY,
    name VARCHAR(65535),
    description VARCHAR(65535),
    company_size VARCHAR(65535),
    state VARCHAR(65535),
    country VARCHAR(65535),
    city VARCHAR(65535),
    zip_code VARCHAR(65535),
    address VARCHAR(65535),
    url VARCHAR(65535)
)
DISTSTYLE KEY
DISTKEY(company_id)
SORTKEY(company_id);

CREATE TABLE IF NOT EXISTS company_industries (
    company_id FLOAT8,
    industry VARCHAR(65535),
    PRIMARY KEY (company_id, industry)
)
DISTSTYLE KEY
DISTKEY(company_id);

CREATE TABLE IF NOT EXISTS company_specialities (
    company_id FLOAT8,
    speciality VARCHAR(65535),
    PRIMARY KEY (company_id, speciality)
)
DISTSTYLE KEY
DISTKEY(company_id);

CREATE TABLE IF NOT EXISTS employee_counts (
    company_id FLOAT8,
    employee_count INTEGER,
    follower_count INTEGER,
    time_recorded INTEGER,
    PRIMARY KEY (company_id, time_recorded)
)
DISTSTYLE KEY
DISTKEY(company_id)
SORTKEY(time_recorded);