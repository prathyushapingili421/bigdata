# Databricks notebook source
# First, disable Delta format check since you explicitly want to read CSV files
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# Define file paths
file_paths = {
    "postings": "/FileStore/tables/postings.csv",
    "industries": "/FileStore/tables/industries.csv",
    "skills": "/FileStore/tables/skills.csv",
    "benefits": "/FileStore/tables/benefits.csv",
    "job_industries": "/FileStore/tables/job_industries.csv",
    "job_skills": "/FileStore/tables/job_skills.csv",
    "salaries": "/FileStore/tables/salaries.csv",
    "companies": "/FileStore/tables/companies.csv",
    "company_industries": "/FileStore/tables/company_industries.csv",
    "company_specialities": "/FileStore/tables/company_specialities.csv",
    "employee_counts": "/FileStore/tables/employee_counts.csv"
}

# Load datasets into Spark DataFrames with additional CSV options for better reliability
dataframes = {}
for name, path in file_paths.items():
    try:
        dataframes[name] = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(path)
    except Exception as e:
        print(f"Error loading {name}: {str(e)}")

# Access each DataFrame as needed
postings_df = dataframes.get("postings")
industries_df = dataframes.get("industries")
skills_df = dataframes.get("skills")
benefits_df = dataframes.get("benefits")
job_industries_df = dataframes.get("job_industries")
job_skills_df = dataframes.get("job_skills")
salaries_df = dataframes.get("salaries")
companies_df = dataframes.get("companies")
company_industries_df = dataframes.get("company_industries")
company_specialities_df = dataframes.get("company_specialities")
employee_counts_df = dataframes.get("employee_counts")

# Verify that DataFrames were loaded successfully
for name, df in dataframes.items():
    if df is not None:
        print(f"{name}: {df.count()} rows")
    else:
        print(f"{name}: Failed to load")

# COMMAND ----------

# 1. Data Cleaning
# Handle salary data - convert string to numeric and standardize
from pyspark.sql.functions import col, when, regexp_replace, lower, trim, to_timestamp, datediff

# Clean postings dataframe
cleaned_postings_df = postings_df \
    .withColumn("max_salary", col("max_salary").cast("double")) \
    .withColumn("min_salary", col("min_salary").cast("double")) \
    .withColumn("med_salary", col("med_salary").cast("double")) \
    .withColumn("views", col("views").cast("integer")) \
    .withColumn("applies", col("applies").cast("integer")) \
    .withColumn("remote_allowed", when(lower(col("remote_allowed")) == "true", True).otherwise(False)) \
    .withColumn("listed_time", to_timestamp("listed_time")) \
    .withColumn("closed_time", to_timestamp("closed_time")) \
    .withColumn("original_listed_time", to_timestamp("original_listed_time"))

# Clean up description text
cleaned_postings_df = cleaned_postings_df \
    .withColumn("description", regexp_replace(col("description"), "[^a-zA-Z0-9\\s]", " ")) \
    .withColumn("description", trim(regexp_replace(col("description"), "\\s+", " ")))

# Remove any duplicate job postings
cleaned_postings_df = cleaned_postings_df.dropDuplicates(["job_id"])

# Remove rows with null values in critical columns
critical_columns = ["job_id", "title", "company_id", "description"]
cleaned_postings_df = cleaned_postings_df.dropna(subset=critical_columns)

# Cache the cleaned dataframe as we'll use it multiple times
cleaned_postings_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reservoir Sampling

# COMMAND ----------

from pyspark.sql.functions import rand
import random

def reservoir_sampling(df, sample_size=1000):
    """
    Implement reservoir sampling on a DataFrame
    """
    # Convert to RDD for easier sampling
    rdd = df.rdd
    
    def reservoir_sample_map(iterator):
        reservoir = []
        for i, item in enumerate(iterator):
            if i < sample_size:
                reservoir.append(item)
            else:
                j = random.randrange(i + 1)
                if j < sample_size:
                    reservoir[j] = item
        return iter(reservoir)
    
    sampled_rdd = rdd.mapPartitions(reservoir_sample_map)
    return spark.createDataFrame(sampled_rdd, df.schema)

# Get a representative sample of job postings
sampled_postings = reservoir_sampling(cleaned_postings_df, 1000)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bloom Filter

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, ArrayType, StringType
import hashlib  # Using hashlib instead of mmh3 for compatibility


class BloomFilter:
    def __init__(self, size, num_hash_functions):
        self.size = size
        self.num_hash_functions = num_hash_functions
        self.bit_array = [0] * size
    
    def _get_hash_values(self, item):
        hash_values = []
        for i in range(self.num_hash_functions):
            value = f"{item}_{i}"
            hash_value = int(hashlib.md5(value.encode()).hexdigest(), 16)
            hash_values.append(hash_value % self.size)
        return hash_values
    
    def add(self, item):
        for index in self._get_hash_values(item):
            self.bit_array[index] = 1
    
    def check(self, item):
        return all(self.bit_array[index] == 1 for index in self._get_hash_values(item))

# Create Bloom Filter for skills
bloom_size = 10000
num_hash_functions = 5
skills_bloom = BloomFilter(bloom_size, num_hash_functions)

# Add all skills to Bloom Filter using skill_abr (since that's what we have in our schema)
skills_list = [row.skill_abr for row in skills_df.select("skill_abr").collect()]
print(f"\nTotal number of skills loaded: {len(skills_list)}")
print("Sample skills:", skills_list[:5])

for skill in skills_list:
    if skill is not None:  # Add null check
        skills_bloom.add(skill)

# Create UDF for skill checking
def check_skill(skill):
    if skill is None:
        return False
    return skills_bloom.check(skill)

check_skill_udf = udf(check_skill, BooleanType())

# Let's test the Bloom Filter with some sample data
test_df = spark.createDataFrame([
    (skills_list[0],),  # Known skill
    ("NONEXISTENT_SKILL",),  # Unknown skill
    (None,),  # Null value
], ["skill"])

test_results = test_df.withColumn("is_known_skill", check_skill_udf("skill"))
print("\nTest Results:")
test_results.show()

# Now let's analyze skills in job postings
from pyspark.sql.functions import explode, split, lower, col

# Create a DataFrame with job skills
job_skills_analysis = job_skills_df \
    .join(skills_df, "skill_abr") \
    .groupBy("skill_abr", "skill_name") \
    .count() \
    .orderBy(col("count").desc())

print("\nMost Common Skills in Job Postings:")
job_skills_analysis.show(10)

# Optional: Save results for later use
job_skills_analysis.write.mode("overwrite").csv("/tmp/job_skills_analysis")

# Additional Analysis: Skills by Job Title
job_title_skills = job_skills_df \
    .join(postings_df, "job_id") \
    .join(skills_df, "skill_abr") \
    .groupBy("title", "skill_name") \
    .count() \
    .orderBy(col("count").desc())

print("\nTop Skills by Job Title:")
job_title_skills.show(10)

# Create a summary of skills data
print("\nSkills Data Summary:")
print(f"Total unique skills: {skills_df.count()}")
print(f"Total job-skill associations: {job_skills_df.count()}")
print(f"Average skills per job: {job_skills_df.groupBy('job_id').count().agg({'count': 'avg'}).collect()[0][0]:.2f}")

# Optional: Export results for Tableau visualization
job_title_skills.write.mode("overwrite").csv("/tmp/job_title_skills_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DGIM

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, unix_timestamp
from collections import deque
from datetime import datetime, timedelta

# First, let's clean and prepare the timestamps
cleaned_skills_over_time = job_skills_df.join(cleaned_postings_df, "job_id") \
    .select(
        "skill_abr",
        unix_timestamp("listed_time").cast("long").alias("timestamp")
    ) \
    .filter(col("timestamp").isNotNull()) \
    .orderBy("timestamp")

class DGIM:
    def __init__(self, window_size):
        self.window_size = window_size
        self.buckets = deque()
    
    def add_bit(self, bit, timestamp):
        # Remove outdated buckets
        current_time = timestamp
        while self.buckets and (current_time - self.buckets[0][1]) >= self.window_size:
            self.buckets.popleft()
        
        if bit == 1:
            # Add new bucket
            self.buckets.append((1, current_time))
            
            # Merge buckets of same size
            i = len(self.buckets) - 1
            while i > 0 and self.buckets[i][0] == self.buckets[i-1][0]:
                size = self.buckets[i][0]
                self.buckets.pop()
                self.buckets.pop()
                self.buckets.append((size * 2, current_time))
                i -= 2
    
    def count_ones(self, timestamp):
        total = 0
        for size, ts in self.buckets:
            if timestamp - ts < self.window_size:
                total += size
        return total

# Initialize DGIM
window_size = 30 * 24 * 60 * 60  # 30 days in seconds
dgim_tracker = DGIM(window_size)

# Process skills mentions over time
# Collect the data to process row by row (note: for large datasets, you might want to batch this)
skill_timestamps = cleaned_skills_over_time.collect()

# Track skill occurrences using DGIM
for row in skill_timestamps:
    if row.timestamp is not None:
        dgim_tracker.add_bit(1, row.timestamp)

# Function to get trending skills in last N days
def get_trending_skills(days=30):
    window_size = days * 24 * 60 * 60  # Convert days to seconds
    current_time = int(datetime.now().timestamp())
    
    trending_skills = cleaned_skills_over_time \
        .filter(col("timestamp") >= current_time - window_size) \
        .groupBy("skill_abr") \
        .count() \
        .orderBy(col("count").desc())
    
    return trending_skills

# Get trending skills
trending_skills_df = get_trending_skills()
trending_skills_df.show(10)

# Let's also create a function to analyze skill trends over different time windows
def analyze_skill_trends(skill_abr, windows=[7, 30, 90]):
    """
    Analyze skill trends over different time windows
    windows: list of days to analyze
    """
    current_time = int(datetime.now().timestamp())
    
    trends = []
    for days in windows:
        window_size = days * 24 * 60 * 60
        count = cleaned_skills_over_time \
            .filter((col("timestamp") >= current_time - window_size) & 
                   (col("skill_abr") == skill_abr)) \
            .count()
        
        trends.append({
            'window_days': days,
            'skill': skill_abr,
            'count': count
        })
    
    return spark.createDataFrame(trends)

# Example: Analyze trends for a specific skill
# Replace 'PYTHON' with any skill abbreviation from your dataset
skill_trends = analyze_skill_trends('PYTHON')
skill_trends.show()

# Let's also create a function to find emerging skills
def find_emerging_skills(short_window=7, long_window=90):
    """
    Find skills that are growing in popularity
    by comparing recent activity to historical activity
    """
    current_time = int(datetime.now().timestamp())
    short_window_seconds = short_window * 24 * 60 * 60
    long_window_seconds = long_window * 24 * 60 * 60
    
    # Recent activity
    recent = cleaned_skills_over_time \
        .filter(col("timestamp") >= current_time - short_window_seconds) \
        .groupBy("skill_abr") \
        .count() \
        .withColumnRenamed("count", "recent_count")
    
    # Historical activity
    historical = cleaned_skills_over_time \
        .filter((col("timestamp") >= current_time - long_window_seconds) &
                (col("timestamp") < current_time - short_window_seconds)) \
        .groupBy("skill_abr") \
        .count() \
        .withColumnRenamed("count", "historical_count")
    
    # Compare and find emerging skills
    emerging_skills = recent.join(historical, "skill_abr") \
        .withColumn("growth_ratio", 
                   (col("recent_count") * (long_window/short_window)) / col("historical_count")) \
        .orderBy(col("growth_ratio").desc())
    
    return emerging_skills

# Find emerging skills
emerging_skills_df = find_emerging_skills()
emerging_skills_df.show(10)


# COMMAND ----------

# MAGIC %pip install mmh3
