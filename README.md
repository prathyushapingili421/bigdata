## Real-Time Analysis and Predictive Insights of the Job Market Using Big Data Technologies

### **Project Overview**
Analyze over 124,000 LinkedIn job postings (2023-2024) using real-time streaming, batch processing, and predictive analytics. The system delivers insights into salaries, skill demands, remote work trends, and industry growth using a robust big data pipeline.

## **Workflow:**
![WhatsApp Image 2024-11-26 at 01 24 50_82264b6b](https://github.com/user-attachments/assets/55a9e9f1-bdf2-4ba2-853b-6269905f908e)

#### **Data Ingestion:**
Apache Kafka streams job postings and associated data in real-time.

#### **Data Cleaning:**
Azure Databricks performs cleaning (e.g., handling missing values, normalizing salaries).

#### **Streaming Algorithms:**
Reservoir Sampling: Efficient sampling from data streams.
Bloom Filter: Removes duplicate records.
DGIM Algorithm: Tracks trends over sliding windows.

## **Machine Learning:**
Predict salary trends and identify skill clusters using linear regression models.
Data Storage:
AWS S3 for scalable storage.
AWS Redshift for querying aggregated insights.

## **Visualization:**
Interactive Tableau dashboards for exploring trends like:
Salary ranges across industries and regions.
Skill demand and correlation with salaries.
Company-level hiring trends.

## **Key Features**
Real-Time Processing: Kafka and Spark Streaming ensure low latency and dynamic updates.
Scalability: Handles datasets of over 500,000 records with efficient resource utilization.
Predictive Modeling: Linear regression predicts salary trends based on job details.
Interactive Dashboards: Tableau visualizations offer actionable insights for job seekers, recruiters, and policymakers.

## **Visual Insights**
## **Story 1:** Industry & Salary Trends
![WhatsApp Image 2024-11-26 at 00 32 09_84a0791b](https://github.com/user-attachments/assets/874220b6-53cb-49d2-acd5-10a951398b1f)

Salary trends across industries, roles, and locations.
Heatmap of average maximum salaries across regions.

## **Story 2:** Skills & Company Insights
![WhatsApp Image 2024-11-26 at 00 29 47_5aaefdea](https://github.com/user-attachments/assets/7474ea74-67b4-4194-97f1-6a8f29717daa)

Skills driving salary trends.
Job types by company (e.g., remote, full-time, contract).
Correlation between company size, followers, and employee stats.

## **Technology Stack**
Streaming: Apache Kafka (on AWS EC2)

Data Processing: Apache Spark (via Azure Databricks)

Data Storage: AWS S3

Data Warehousing: AWS Redshift

Machine Learning: Spark MLlib (Linear Regression, Clustering)

Visualization: Tableau Dashboards
