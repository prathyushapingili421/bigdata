# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Cleaning

# COMMAND ----------

# Define file paths as per your dictionary
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

# Load datasets into Spark DataFrames
dataframes = {name: spark.read.option("header", True).csv(path) for name, path in file_paths.items()}

# Access each DataFrame as needed
postings_df = dataframes["postings"]
industries_df = dataframes["industries"]
skills_df = dataframes["skills"]
benefits_df = dataframes["benefits"]
job_industries_df = dataframes["job_industries"]
job_skills_df = dataframes["job_skills"]
salaries_df = dataframes["salaries"]
companies_df = dataframes["companies"]
company_industries_df = dataframes["company_industries"]
company_specialities_df = dataframes["company_specialities"]
employee_counts_df = dataframes["employee_counts"]


# COMMAND ----------

from pyspark.sql.functions import col, when

# Drop duplicates
postings_df = postings_df.dropDuplicates()

# Fill missing values
postings_df = postings_df.fillna({"min_salary": 0, "max_salary": 0, "med_salary": 0, "location": "Unknown", "currency": "USD"})

# Convert salary columns to integer
postings_df = postings_df.withColumn("min_salary", col("min_salary").cast("integer")) \
                         .withColumn("max_salary", col("max_salary").cast("integer")) \
                         .withColumn("med_salary", col("med_salary").cast("integer"))

# Standardize pay period
postings_df = postings_df.withColumn("pay_period", when(col("pay_period") == "HOURLY", "hourly")
                                     .when(col("pay_period") == "YEARLY", "yearly")
                                     .otherwise(col("pay_period")))

# Convert timestamp columns
postings_df = postings_df.withColumn("listed_time", col("listed_time").cast("timestamp")) \
                         .withColumn("expiry", col("expiry").cast("timestamp")) \
                         .withColumn("closed_time", col("closed_time").cast("timestamp"))


# COMMAND ----------

# Standardize skill names
skills_df = skills_df.dropDuplicates().fillna({"skill_name": "Unknown"})

# Standardize industry names
industries_df = industries_df.dropDuplicates().fillna({"industry_name": "Unknown"})


# COMMAND ----------

from pyspark.sql.functions import col, when, lit, regexp_replace

# Drop duplicates in all dataframes and fill missing values where appropriate
postings_df = postings_df.dropDuplicates().fillna({"min_salary": 0, "max_salary": 0, "med_salary": 0, "currency": "USD", "location": "Unknown"})
industries_df = industries_df.dropDuplicates().fillna({"industry_name": "Unknown"})
skills_df = skills_df.dropDuplicates().fillna({"skill_name": "Unknown"})
benefits_df = benefits_df.dropDuplicates().fillna({"type": "Unknown"})
job_industries_df = job_industries_df.dropDuplicates()
job_skills_df = job_skills_df.dropDuplicates()
salaries_df = salaries_df.dropDuplicates().fillna({"pay_period": "Unknown", "currency": "USD", "compensation_type": "BASE_SALARY"})
companies_df = companies_df.dropDuplicates().fillna({"name": "Unknown", "state": "Unknown", "country": "Unknown", "zip_code": "Unknown"})
company_industries_df = company_industries_df.dropDuplicates()
company_specialities_df = company_specialities_df.dropDuplicates().fillna({"speciality": "Unknown"})
employee_counts_df = employee_counts_df.dropDuplicates().fillna({"employee_count": 0, "follower_count": 0})

# Convert salary columns to integer for consistency
postings_df = postings_df.withColumn("min_salary", col("min_salary").cast("integer")) \
                         .withColumn("max_salary", col("max_salary").cast("integer")) \
                         .withColumn("med_salary", col("med_salary").cast("integer"))
                         
salaries_df = salaries_df.withColumn("min_salary", col("min_salary").cast("integer")) \
                         .withColumn("max_salary", col("max_salary").cast("integer")) \
                         .withColumn("med_salary", col("med_salary").cast("integer"))

# Standardize text fields (e.g., lowercase, remove special characters)
postings_df = postings_df.withColumn("work_type", regexp_replace(col("work_type"), "_", " ").alias("work_type"))


# COMMAND ----------

# Merge postings with salaries based on job_id
merged_df = postings_df.join(salaries_df, "job_id", "left").select(
    postings_df["*"],
    salaries_df["min_salary"].alias("salaries_min_salary"),
    salaries_df["max_salary"].alias("salaries_max_salary"),
    salaries_df["med_salary"].alias("salaries_med_salary"),
    salaries_df["pay_period"].alias("salaries_pay_period")
)


# COMMAND ----------

# Join postings with job industries and industry names
merged_df = merged_df.join(job_industries_df, "job_id", "left") \
                     .join(industries_df, job_industries_df["industry_id"] == industries_df["industry_id"], "left") \
                     .drop(job_industries_df["industry_id"]) \
                     .withColumnRenamed("industry_name", "job_industry_name")


# COMMAND ----------

from pyspark.sql.functions import collect_list

# Join job skills and skill names, then group by job_id to collect all skills
skills_joined = job_skills_df.join(skills_df, "skill_abr", "left").select("job_id", "skill_name")

# Group skills by job_id
skills_grouped = skills_joined.groupBy("job_id").agg(collect_list("skill_name").alias("skills"))

# Join the grouped skills to the main dataframe
merged_df = merged_df.join(skills_grouped, "job_id", "left")


# COMMAND ----------

# Group benefits by job_id
benefits_grouped = benefits_df.groupBy("job_id").agg(collect_list("type").alias("benefits"))

# Join the grouped benefits to the main dataframe
merged_df = merged_df.join(benefits_grouped, "job_id", "left")


# COMMAND ----------

# Join postings with company information
merged_df = merged_df.join(companies_df, "company_id", "left") \
                     .withColumnRenamed("name", "company_name") \
                     .withColumnRenamed("description", "company_description") \
                     .withColumnRenamed("company_size", "company_size") \
                     .withColumnRenamed("state", "company_state") \
                     .withColumnRenamed("country", "company_country") \
                     .withColumnRenamed("city", "company_city")


# COMMAND ----------

# Group industries and specialties by company_id
company_industries_grouped = company_industries_df.groupBy("company_id").agg(collect_list("industry").alias("company_industries"))
company_specialities_grouped = company_specialities_df.groupBy("company_id").agg(collect_list("speciality").alias("company_specialties"))

# Join the grouped data to the main dataframe
merged_df = merged_df.join(company_industries_grouped, "company_id", "left") \
                     .join(company_specialities_grouped, "company_id", "left")


# COMMAND ----------

# Join employee counts data
merged_df = merged_df.join(employee_counts_df, "company_id", "left") \
                     .withColumnRenamed("employee_count", "company_employee_count") \
                     .withColumnRenamed("follower_count", "company_follower_count")


# COMMAND ----------

from pyspark.sql.functions import lit, array, when

# Fill non-array columns with default values
merged_df = merged_df.fillna({
    "salaries_min_salary": 0,
    "salaries_max_salary": 0,
    "salaries_med_salary": 0,
    "pay_period": "Unknown",
    "job_industry_name": "Unknown",
    "company_employee_count": 0,
    "company_follower_count": 0
})

# For array columns, replace nulls with empty arrays
merged_df = merged_df.withColumn("skills", when(col("skills").isNull(), array().cast("array<string>")).otherwise(col("skills"))) \
                     .withColumn("benefits", when(col("benefits").isNull(), array().cast("array<string>")).otherwise(col("benefits"))) \
                     .withColumn("company_industries", when(col("company_industries").isNull(), array().cast("array<string>")).otherwise(col("company_industries"))) \
                     .withColumn("company_specialties", when(col("company_specialties").isNull(), array().cast("array<string>")).otherwise(col("company_specialties")))

# Show the merged dataset
merged_df.show(truncate=False)



# COMMAND ----------

# Show the schema to identify duplicate columns
merged_df.printSchema()




# COMMAND ----------

# Drop unnecessary duplicate columns
merged_df = merged_df.drop("company_name", "company_description", "zip_code")


# COMMAND ----------

# Check schema after dropping duplicates
merged_df.printSchema()


# COMMAND ----------


# Summary statistics for numerical columns
merged_df.describe().show()


# COMMAND ----------

# Select specific numerical columns to get summary statistics
selected_columns = ["min_salary", "med_salary", "max_salary", "salaries_min_salary", "salaries_max_salary", "company_employee_count", "company_follower_count"]
merged_df.select(selected_columns).describe().show()


# COMMAND ----------

# Get specific summary statistics for selected columns
merged_df.select(selected_columns).summary("count", "mean", "stddev", "min", "max").show()


# COMMAND ----------

from pyspark.sql.functions import mean, stddev, min, max

# Calculate mean, standard deviation, min, and max for each relevant column
stats_df = merged_df.select(
    mean("min_salary").alias("mean_min_salary"),
    stddev("min_salary").alias("stddev_min_salary"),
    min("min_salary").alias("min_min_salary"),
    max("min_salary").alias("max_min_salary"),
    
    mean("med_salary").alias("mean_med_salary"),
    stddev("med_salary").alias("stddev_med_salary"),
    min("med_salary").alias("min_med_salary"),
    max("med_salary").alias("max_med_salary"),
    
    mean("max_salary").alias("mean_max_salary"),
    stddev("max_salary").alias("stddev_max_salary"),
    min("max_salary").alias("min_max_salary"),
    max("max_salary").alias("max_max_salary")
)

stats_df.show()


# COMMAND ----------

# Custom aggregation for summary statistics
merged_df.agg(
    mean("min_salary").alias("mean_min_salary"),
    max("max_salary").alias("max_max_salary"),
    mean("company_employee_count").alias("mean_employee_count"),
    max("company_follower_count").alias("max_follower_count")
).show()


# COMMAND ----------

# Limit to a sample of rows for quick testing
sample_df = merged_df.limit(1000)
sample_df.describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Machine Learning Model

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, collect_list
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Job Postings Data Analysis") \
    .getOrCreate()

# File paths dictionary
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

# Load datasets
postings_df = spark.read.csv(file_paths["postings"], header=True, inferSchema=True)
industries_df = spark.read.csv(file_paths["industries"], header=True, inferSchema=True)
skills_df = spark.read.csv(file_paths["skills"], header=True, inferSchema=True)
benefits_df = spark.read.csv(file_paths["benefits"], header=True, inferSchema=True)
job_industries_df = spark.read.csv(file_paths["job_industries"], header=True, inferSchema=True)
job_skills_df = spark.read.csv(file_paths["job_skills"], header=True, inferSchema=True)
salaries_df = spark.read.csv(file_paths["salaries"], header=True, inferSchema=True)
companies_df = spark.read.csv(file_paths["companies"], header=True, inferSchema=True)
company_industries_df = spark.read.csv(file_paths["company_industries"], header=True, inferSchema=True)
company_specialities_df = spark.read.csv(file_paths["company_specialities"], header=True, inferSchema=True)
employee_counts_df = spark.read.csv(file_paths["employee_counts"], header=True, inferSchema=True)

# Data Cleaning

# Drop duplicates
postings_df = postings_df.dropDuplicates()

# Fill missing values
postings_df = postings_df.fillna({
    "min_salary": 0,
    "max_salary": 0,
    "med_salary": 0,
    "location": "Unknown",
    "currency": "USD"
})

# Convert salary columns to integer type
postings_df = postings_df.withColumn("min_salary", col("min_salary").cast(IntegerType()))
postings_df = postings_df.withColumn("max_salary", col("max_salary").cast(IntegerType()))
postings_df = postings_df.withColumn("med_salary", col("med_salary").cast(IntegerType()))

# Standardize pay period
postings_df = postings_df.withColumn("pay_period", when(col("pay_period") == "HOURLY", "hourly")
                                     .when(col("pay_period") == "YEARLY", "yearly").otherwise(col("pay_period")))

# Merge DataFrames

# Merge postings with salaries
salaries_df = salaries_df.withColumnRenamed("min_salary", "salaries_min_salary") \
                         .withColumnRenamed("max_salary", "salaries_max_salary") \
                         .withColumnRenamed("med_salary", "salaries_med_salary") \
                         .withColumnRenamed("pay_period", "salaries_pay_period")

merged_df = postings_df.join(salaries_df, "job_id", "left")

# Merge with employee counts
merged_df = merged_df.join(employee_counts_df, "company_id", "left") \
    .withColumnRenamed("employee_count", "company_employee_count") \
    .withColumnRenamed("follower_count", "company_follower_count")

# Merge with job industries and industry names
merged_df = merged_df.join(job_industries_df, "job_id", "left")
merged_df = merged_df.join(industries_df, "industry_id", "left") \
    .withColumnRenamed("industry_name", "job_industry_name")

# Merge with job skills
skills_grouped = job_skills_df.join(skills_df, "skill_abr", "left") \
    .groupBy("job_id").agg(collect_list("skill_name").alias("skills"))
merged_df = merged_df.join(skills_grouped, "job_id", "left")

# Merge with benefits
benefits_grouped = benefits_df.groupBy("job_id").agg(collect_list("type").alias("benefits"))
merged_df = merged_df.join(benefits_grouped, "job_id", "left")

# Merge with company data
merged_df = merged_df.join(companies_df, "company_id", "left") \
    .withColumnRenamed("name", "company_name") \
    .withColumnRenamed("description", "company_description") \
    .withColumnRenamed("company_size", "company_size") \
    .withColumnRenamed("state", "company_state") \
    .withColumnRenamed("country", "company_country") \
    .withColumnRenamed("city", "company_city")

# Fill missing values for numeric fields
merged_df = merged_df.fillna({
    "salaries_min_salary": 0,
    "salaries_max_salary": 0,
    "salaries_med_salary": 0,
    "pay_period": "Unknown",
    "job_industry_name": "Unknown",
    "company_employee_count": 0,
    "company_follower_count": 0
})

# Selecting relevant columns
required_columns = ["salaries_med_salary", "location", "job_industry_name", "work_type", "remote_allowed",
                    "company_employee_count", "company_follower_count"]
data = merged_df.select(required_columns)

# Feature Engineering

# COMMAND ----------

postings_df = postings_df.dropDuplicates()

# Fill missing values
postings_df = postings_df.fillna({
    "min_salary": 0,
    "max_salary": 0,
    "med_salary": 0,
    "location": "Unknown",
    "currency": "USD"
})

# Convert salary columns to integer type
postings_df = postings_df.withColumn("min_salary", col("min_salary").cast(IntegerType()))
postings_df = postings_df.withColumn("max_salary", col("max_salary").cast(IntegerType()))
postings_df = postings_df.withColumn("med_salary", col("med_salary").cast(IntegerType()))

# Standardize pay period
postings_df = postings_df.withColumn("pay_period", when(col("pay_period") == "HOURLY", "hourly")
                                     .when(col("pay_period") == "YEARLY", "yearly").otherwise(col("pay_period")))

# Merge DataFrames

# Merge postings with salaries
salaries_df = salaries_df.withColumnRenamed("min_salary", "salaries_min_salary") \
                         .withColumnRenamed("max_salary", "salaries_max_salary") \
                         .withColumnRenamed("med_salary", "salaries_med_salary") \
                         .withColumnRenamed("pay_period", "salaries_pay_period")

merged_df = postings_df.join(salaries_df, "job_id", "left")

# Merge with employee counts
merged_df = merged_df.join(employee_counts_df, "company_id", "left") \
    .withColumnRenamed("employee_count", "company_employee_count") \
    .withColumnRenamed("follower_count", "company_follower_count")

# Merge with job industries and industry names
merged_df = merged_df.join(job_industries_df, "job_id", "left")
merged_df = merged_df.join(industries_df, "industry_id", "left") \
    .withColumnRenamed("industry_name", "job_industry_name")

# Merge with job skills
skills_grouped = job_skills_df.join(skills_df, "skill_abr", "left") \
    .groupBy("job_id").agg(collect_list("skill_name").alias("skills"))
merged_df = merged_df.join(skills_grouped, "job_id", "left")

# Merge with benefits
benefits_grouped = benefits_df.groupBy("job_id").agg(collect_list("type").alias("benefits"))
merged_df = merged_df.join(benefits_grouped, "job_id", "left")

# Merge with company data
merged_df = merged_df.join(companies_df, "company_id", "left") \
    .withColumnRenamed("name", "company_name") \
    .withColumnRenamed("description", "company_description") \
    .withColumnRenamed("company_size", "company_size") \
    .withColumnRenamed("state", "company_state") \
    .withColumnRenamed("country", "company_country") \
    .withColumnRenamed("city", "company_city")

# Fill missing values for numeric fields
merged_df = merged_df.fillna({
    "salaries_min_salary": 0,
    "salaries_max_salary": 0,
    "salaries_med_salary": 0,
    "pay_period": "Unknown",
    "job_industry_name": "Unknown",
    "company_employee_count": 0,
    "company_follower_count": 0
})

# Selecting relevant columns
required_columns = ["salaries_med_salary", "location", "job_industry_name", "work_type", "remote_allowed",
                    "company_employee_count", "company_follower_count"]
data = merged_df.select(required_columns)

# Feature Engineering

# Replace nulls in categorical columns with a placeholder
data = data.fillna({"location": "Unknown", "job_industry_name": "Unknown", "work_type": "Unknown", "remote_allowed": "Unknown"})

# Index categorical columns
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_indexed", handleInvalid="keep")
    for col in ["location", "job_industry_name", "work_type", "remote_allowed"]
]
for indexer in indexers:
    data = indexer.fit(data).transform(data)



# COMMAND ----------


# Assemble features
assembler = VectorAssembler(inputCols=["location_indexed", "job_industry_name_indexed", "work_type_indexed",
                                       "remote_allowed_indexed", "company_employee_count", "company_follower_count"],
                            outputCol="features")
data = assembler.transform(data)

# Scale features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
data = scaler.fit(data).transform(data)

# Train-Test Split
train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)

# Build and train the regression model
rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="salaries_med_salary", numTrees=100, seed=1234)
model = rf.fit(train_data)

# Make predictions
predictions = model.transform(test_data)

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="salaries_med_salary", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"R-squared (R2): {r2}")

# COMMAND ----------

postings_df = postings_df.dropDuplicates()

# Fill missing values
postings_df = postings_df.fillna({
    "min_salary": 0,
    "max_salary": 0,
    "med_salary": 0,
    "location": "Unknown",
    "currency": "USD"
})

# Convert salary columns to integer type
postings_df = postings_df.withColumn("min_salary", col("min_salary").cast(IntegerType()))
postings_df = postings_df.withColumn("max_salary", col("max_salary").cast(IntegerType()))
postings_df = postings_df.withColumn("med_salary", col("med_salary").cast(IntegerType()))

# Standardize pay period
postings_df = postings_df.withColumn("pay_period", when(col("pay_period") == "HOURLY", "hourly")
                                     .when(col("pay_period") == "YEARLY", "yearly").otherwise(col("pay_period")))

# Merge DataFrames

# Merge postings with salaries
salaries_df = salaries_df.withColumnRenamed("min_salary", "salaries_min_salary") \
                         .withColumnRenamed("max_salary", "salaries_max_salary") \
                         .withColumnRenamed("med_salary", "salaries_med_salary") \
                         .withColumnRenamed("pay_period", "salaries_pay_period")

merged_df = postings_df.join(salaries_df, "job_id", "left")

# Merge with employee counts
merged_df = merged_df.join(employee_counts_df, "company_id", "left") \
    .withColumnRenamed("employee_count", "company_employee_count") \
    .withColumnRenamed("follower_count", "company_follower_count")

# Merge with job industries and industry names
merged_df = merged_df.join(job_industries_df, "job_id", "left")
merged_df = merged_df.join(industries_df, "industry_id", "left") \
    .withColumnRenamed("industry_name", "job_industry_name")

# Merge with job skills
skills_grouped = job_skills_df.join(skills_df, "skill_abr", "left") \
    .groupBy("job_id").agg(collect_list("skill_name").alias("skills"))
merged_df = merged_df.join(skills_grouped, "job_id", "left")

# Merge with benefits
benefits_grouped = benefits_df.groupBy("job_id").agg(collect_list("type").alias("benefits"))
merged_df = merged_df.join(benefits_grouped, "job_id", "left")

# Merge with company data
merged_df = merged_df.join(companies_df, "company_id", "left") \
    .withColumnRenamed("name", "company_name") \
    .withColumnRenamed("description", "company_description") \
    .withColumnRenamed("company_size", "company_size") \
    .withColumnRenamed("state", "company_state") \
    .withColumnRenamed("country", "company_country") \
    .withColumnRenamed("city", "company_city")

# Fill missing values for numeric fields
merged_df = merged_df.fillna({
    "salaries_min_salary": 0,
    "salaries_max_salary": 0,
    "salaries_med_salary": 0,
    "pay_period": "Unknown",
    "job_industry_name": "Unknown",
    "company_employee_count": 0,
    "company_follower_count": 0
})

# Selecting relevant columns
required_columns = ["salaries_med_salary", "location", "job_industry_name", "work_type", "remote_allowed",
                    "company_employee_count", "company_follower_count"]
data = merged_df.select(required_columns)

# Feature Engineering

# Replace nulls in categorical columns with a placeholder
data = data.fillna({"location": "Unknown", "job_industry_name": "Unknown", "work_type": "Unknown", "remote_allowed": "Unknown"})

# Index categorical columns
indexers = [
    StringIndexer(inputCol=col, outputCol=f"{col}_indexed", handleInvalid="keep")
    for col in ["location", "job_industry_name", "work_type", "remote_allowed"]
]
for indexer in indexers:
    data = indexer.fit(data).transform(data)



# COMMAND ----------


# Assemble features
assembler = VectorAssembler(inputCols=["location_indexed", "job_industry_name_indexed", "work_type_indexed",
                                       "remote_allowed_indexed", "company_employee_count", "company_follower_count"],
                            outputCol="features")
data = assembler.transform(data)

# Scale features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
data = scaler.fit(data).transform(data)

sample_fraction = 0.15  # Set between 0.1 (10%) and 0.2 (20%)
sampled_data = data.sample(withReplacement=False, fraction=sample_fraction, seed=1234)

# Train-Test Split
train_data, test_data = sampled_data.randomSplit([0.8, 0.2], seed=1234)

# Build and train the regression model
rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="salaries_med_salary", numTrees=100, seed=1234)
model = rf.fit(train_data)

# Make predictions
predictions = model.transform(test_data)

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="salaries_med_salary", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"R-squared (R2): {r2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use of streaming algorithms:
# MAGIC ### Use of Reservoir Sampling, Bloom Filter, Flajolet-Martin, DGIM, Computing Moments, algorithms for graph streams, etc

# COMMAND ----------

import random
from pyspark.sql import Row

def reservoir_sampling_rdd(df, k):
    """
    Implements Reservoir Sampling to sample 'k' rows from a PySpark DataFrame using RDDs.
    
    :param df: PySpark DataFrame to sample from
    :param k: Number of samples to select
    :return: DataFrame with 'k' randomly selected rows
    """
    # Convert the DataFrame to an RDD
    rdd = df.rdd
    
    def process_partition(iterator):
        # Local reservoir for this partition
        local_reservoir = []
        
        for i, row in enumerate(iterator):
            # Initialize the reservoir with the first k elements
            if len(local_reservoir) < k:
                local_reservoir.append(row)
            else:
                # Replace a random element in the reservoir with a decreasing probability
                j = random.randint(0, i)
                if j < k:
                    local_reservoir[j] = row
        return local_reservoir

    # Collect samples from all partitions
    sampled_rdd = rdd.mapPartitions(process_partition)
    
    # Flatten and collect the sampled RDD to the driver
    sampled_rows = sampled_rdd.collect()
    
    # Perform global reservoir sampling to combine all local samples
    global_reservoir = []
    for i, row in enumerate(sampled_rows):
        if len(global_reservoir) < k:
            global_reservoir.append(row)
        else:
            j = random.randint(0, i)
            if j < k:
                global_reservoir[j] = row
    
    # Convert the global reservoir back to a DataFrame
    sampled_df = spark.createDataFrame(global_reservoir, schema=df.schema)
    
    return sampled_df

# Define the sample size
k = 5  # Select 5 random rows

# Apply the reservoir sampling function to the DataFrame
sampled_df = reservoir_sampling_rdd(merged_df, k)

# Show the sampled DataFrame
sampled_df.show()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Use of Locality Sensitive Hashing 

# COMMAND ----------

from pyspark.ml.linalg import VectorUDT

# COMMAND ----------

from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import col, udf

# Create a vocabulary for skills (e.g., assigning an index to each skill)
all_skills = skills_df.select("skill_name").rdd.flatMap(lambda x: x).distinct().collect()
skill_to_index = {skill: idx for idx, skill in enumerate(all_skills)}

# UDF to convert skills to sparse vectors
def skills_to_sparse_vector(skills):
    if skills is None or len(skills) == 0:
        return Vectors.sparse(len(skill_to_index), [])
    indices = sorted([skill_to_index[skill] for skill in skills if skill in skill_to_index])
    values = [1.0] * len(indices)
    return Vectors.sparse(len(skill_to_index), indices, values)

# Register UDF to return Vector objects
skills_to_sparse_udf = udf(skills_to_sparse_vector, returnType=VectorUDT())

# Apply UDF to create a "skills_vector" column
skills_grouped = skills_grouped.withColumn("skills_vector", skills_to_sparse_udf(col("skills")))

# Apply MinHashLSH
mh = MinHashLSH(inputCol="skills_vector", outputCol="hashes", numHashTables=3)
model = mh.fit(skills_grouped)

# Transform the dataset to include hashes
hashed_df = model.transform(skills_grouped)

# Find similar jobs based on skills
query_skills = Vectors.sparse(len(skill_to_index), {0: 1.0, 1: 1.0})  # Example query vector
similar_jobs = model.approxNearestNeighbors(hashed_df, query_skills, numNearestNeighbors=5)

# Show similar jobs
similar_jobs.select("job_id", "skills", "distCol").show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Advantages of LSH in Workflow**
# MAGIC **Scalability**:  
# MAGIC Efficiently handles large datasets by organizing similar items into the same hash buckets.
# MAGIC
# MAGIC **Approximate Nearest Neighbors**:  
# MAGIC Enables rapid identification of similar job descriptions, skill sets, or industries without having to compare each pair individually.
# MAGIC
# MAGIC **Customizability**:  
# MAGIC Works with any column that can be vectorized, providing flexibility for diverse applications.

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Use of Privacy techniques K-Anonymity, L-Diversity, Differential Privacy, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. K-Anonymity
# MAGIC The goal is to prevent any individual in the dataset from being uniquely identified by ensuring that at least *k* individuals have the same combination of quasi-identifiers.
# MAGIC
# MAGIC For example, this can be done using PySpark to anonymize data such as location and job industry name.

# COMMAND ----------

from pyspark.sql.functions import col, count

# Group by quasi-identifiers to measure anonymity
k_anonymous_df = data.groupBy("location", "job_industry_name").agg(count("*").alias("count"))

# Filter groups that do not meet the k-anonymity threshold
k = 5
anonymized_df = k_anonymous_df.filter(col("count") >= k)

anonymized_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC This guarantees that no combination of location and job industry can be linked to fewer than *k* individuals.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. L-Diversity
# MAGIC To extend K-Anonymity, it is essential to ensure that not only are at least *k* individuals grouped together based on their quasi-identifiers, but also that sensitive attributes (such as *salaries_med_salary*) exhibit diversity within each equivalence class. This helps prevent sensitive information from being revealed.

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct

# Measure diversity in sensitive attribute
l_diverse_df = data.groupBy("location", "job_industry_name").agg(
    approx_count_distinct("salaries_med_salary").alias("diversity")
)

# Filter groups with sufficient diversity
l = 3
l_diverse_filtered = l_diverse_df.filter(col("diversity") >= l)

l_diverse_filtered.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Ensures that for each location and job industry, there are at least *l* different salary values for *salaries_med_salary*.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Differential Privacy
# MAGIC The objective is to add noise to the dataset to safeguard individual identities while still preserving overall trends and aggregate insights.
# MAGIC
# MAGIC For instance, Laplace noise can be used to perturb salary averages, ensuring that the data remains anonymous while still providing useful statistical information.

# COMMAND ----------

import numpy as np
from pyspark.sql.functions import mean

# Calculate the mean salary and add Laplace noise
epsilon = 1.0  # Privacy budget
salary_mean = data.agg(mean("salaries_med_salary").alias("mean_salary")).collect()[0]["mean_salary"]
noisy_mean_salary = salary_mean + np.random.laplace(scale=1/epsilon)

print(f"True Mean Salary: {salary_mean}")
print(f"Noisy Mean Salary: {noisy_mean_salary}")


# COMMAND ----------

# MAGIC %md
# MAGIC Noise is introduced according to the epsilon parameter, striking a balance between preserving privacy and maintaining accuracy.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Any other tools and techniques covered in the course not included in the other criteria. Think Machine Unlearning, Explainable AI (if covered in the class); Fairness; Federated Learning; Data Poisoning, Responsible AI, etc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Fairness
# MAGIC Assess and ensure fairness in predictions, such as making sure that salary predictions do not disproportionately favor certain locations. This can be done by applying Disparate Impact Analysis to evaluate fairness.

# COMMAND ----------

from pyspark.sql.functions import mean, col

# Calculate mean salary predictions by location
fairness_df = predictions.groupBy("location").agg(mean("prediction").alias("avg_prediction"))
fairness_df.show()

# Define privileged and unprivileged locations
privileged_location = "CityA"
unprivileged_location = "CityB"

# Filter the average predictions for privileged and unprivileged locations
privileged_row = fairness_df.filter(col("location") == privileged_location).select("avg_prediction").first()
unprivileged_row = fairness_df.filter(col("location") == unprivileged_location).select("avg_prediction").first()

# Check if rows are not None
if privileged_row is not None and unprivileged_row is not None:
    privileged_avg = privileged_row[0]
    unprivileged_avg = unprivileged_row[0]

    # Calculate disparate impact
    disparate_impact = unprivileged_avg / privileged_avg
    print(f"Disparate Impact: {disparate_impact}")
else:
    print("One or both of the specified locations do not exist in the dataset.")


# COMMAND ----------

# MAGIC %md
# MAGIC Ensure that salary predictions are fair and not biased toward or against particular locations or industries.
