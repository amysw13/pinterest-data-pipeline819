# Pinterest Data Pipeline

# Table of contents



---

## Description

AWS-hosted end-to-end data pipeline inspired by Pinterest's experiment processing pipeline.
Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud.

The pipeline is developed using a Lambda architecture. The batch data is ingested using AWS API Gateway and AWS MSK  and then stored in an AWS S3 bucket.

The batch data is then read from the S3 bucket into Databricks where it is processed using Apache Spark.

The streaming data is read near real-time from AWS Kinesis using Spark Structured Streaming in Databricks and stored in Databricks Delta Tables for long term storage.

![Amazon AWS Badge](https://img.shields.io/badge/Amazon%20AWS-232F3E?logo=amazonaws&logoColor=fff&style=for-the-badge)

## Aim

Build an end-to-end data pipeline using AWS-hosted cloud technologies. 

## Learnt Objectives

![Amazon EC2 Badge](https://img.shields.io/badge/Amazon%20EC2-F90?logo=amazonec2&logoColor=fff&style=for-the-badge)
![Apache Kafka Badge](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=fff&style=for-the-badge)

- Configuring AWS EC2 instances
- Installation and configuration of Kafka on EC2 client machine
- Creating topics with Kafka on EC2 instance

---

![Amazon S3 Badge](https://img.shields.io/badge/Amazon%20S3-569A31?logo=amazons3&logoColor=fff&style=for-the-badge)
![Amazon Identity Access Management Badge](https://img.shields.io/badge/Amazon%20Identity%20Access%20Management-DD344C?logo=amazoniam&logoColor=fff&style=for-the-badge)

- Configuring S3 bucket and MSK Connect
- Confluent to connect to S3 bucket and Kafka topics
- Creation of customised plugins and configuring MSK connector with IAM roles
  
---

![Amazon API Gateway Badge](https://img.shields.io/badge/Amazon%20API%20Gateway-FF4F8B?logo=amazonapigateway&logoColor=fff&style=for-the-badge)

- Creating resource for proxy integration for REST API gateway
- Deployment of API
- Configuration of Kafka REST proxy API on EC2 client machine
- Installation of Confluent package
- Configuration of kafka-rest.properties to perform IAM authentication
- Starting REST proxy on EC2 client machine
- Sending streaming data using API invoke URL to S3 bucket
  - Formatting data to JSON message formatting for API processing
  - Data from 3 pinterest tables to corresponding Kafka topics

---

![Databricks Badge](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=fff&style=for-the-badge)

- Creating Databricks workspace
- Creating access key and secret access key for in AWS for full S3 access
- Loading in credential file to Databricks
- Mounting S3 bucket in Databricks
- Reading in .json format data into Databricks dataframes
  
![Apache Spark Badge](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=fff&style=for-the-badge)

- Cleaning data using PySparks
- Dataframe joins
- Querying and aggregations of data
  - group by
  - classifying values into groups
  - alias
  - sorting data

![Apache Airflow Badge](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=fff&style=for-the-badge)

- Creating DAG to trigger Databricks notebook to run on schedule
- Uploading DAG to S3 bucket
- Using AWS MWAA environment to access Airflow UI
- Triggering DAG successfully
- Ensuring Databricks notebook is compatible with DAG and Airflow workflow
  
---

- Troubleshooting through all configurations and set up of AWS services and users on AWS CLI
  - Including checking through all IAM permissions, MSK connect plugin and connector configuration, EC2 instances connection issues, API gateway configurations.
- Troubleshooting connection issues in Databricks, credentials configuration and Delta data formatting issues.

# Installation and Usage Instructions ⚙

## Example Use / Demo

# File Structure 📂

- 📂 __pinterest\-data\-pipeline819__
   - 📄 [124714cdee67\_dag.py](124714cdee67_dag.py)
   - 📄 [README.md](README.md)
   - 📄 [Reading, cleaning and querying Pinterest Data from mounted S3 bucket using Sparks.ipynb](Reading%2C%20cleaning%20and%20querying%20Pinterest%20Data%20from%20mounted%20S3%20bucket%20using%20Sparks.ipynb)

# License information 🗒

# Technologies used in this project

![Jupyter](https://img.shields.io/badge/Jupyter-F37626.svg?&style=for-the-badge&logo=Jupyter&logoColor=white)
![VsCode](https://img.shields.io/badge/VSCode-0078D4?style=for-the-badge&logo=visual%20studio%20code&logoColor=white)
![github](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)
![conda](https://img.shields.io/badge/conda-342B029.svg?&style=for-the-badge&logo=anaconda&logoColor=white)

![Databricks Badge](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=fff&style=for-the-badge)
![Apache Spark Badge](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=fff&style=for-the-badge)

## AWS Cloud

![Amazon AWS Badge](https://img.shields.io/badge/Amazon%20AWS-232F3E?logo=amazonaws&logoColor=fff&style=for-the-badge)
![Amazon EC2 Badge](https://img.shields.io/badge/Amazon%20EC2-F90?logo=amazonec2&logoColor=fff&style=for-the-badge)
![Apache Kafka Badge](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=fff&style=for-the-badge)
![Amazon MSK Connect](https://img.shields.io/badge/Amazon%20MSK%20Connect-8a42f5?style=for-the-badge&logo={LOGO-NAME}&logoColor=white)
![Amazon Identity Access Management Badge](https://img.shields.io/badge/Amazon%20Identity%20Access%20Management-DD344C?logo=amazoniam&logoColor=fff&style=for-the-badge)
![Amazon S3 Badge](https://img.shields.io/badge/Amazon%20S3-569A31?logo=amazons3&logoColor=fff&style=for-the-badge)
![Amazon API Gateway Badge](https://img.shields.io/badge/Amazon%20API%20Gateway-FF4F8B?logo=amazonapigateway&logoColor=fff&style=for-the-badge)
![Apache Airflow Badge](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=fff&style=for-the-badge)


●Technologies used: Kafka, AWS MSK, MSK Connect, AWS API Gateway, AWS S3, Spark, Spark Structured Streaming, Databricks, Airflow, AWS MWAA, AWS Kinesis.
