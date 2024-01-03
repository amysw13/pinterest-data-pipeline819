# Pinterest Data Pipeline

# Table of contents


#TODO

--- 
## Description
Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud. 


![Amazon AWS Badge](https://img.shields.io/badge/Amazon%20AWS-232F3E?logo=amazonaws&logoColor=fff&style=for-the-badge)

## Aim

#TODO

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
- Creating access key and secret access key for in AWS for full s3 access
- Loading in credential file to Databricks
- Mounting s3 bucket in Databricks
- Reading in .json format data into Databricks dataframes

---

- Troubleshooting through all configurations and set up of AWS services and users on AWS CLI
  - Including checking through all IAM permissions, MSK connect plugin and connector configuration, EC2 instances connection issues, API gateway configurations.
- Troubleshooting connection issues in Databricks, credentials configuration and Delta data formatting issues. 

# Installation and Usage Instructions ⚙


## Example Use / Demo

# File Structure 📂


# License information 🗒


# Open source packages used in this project

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
![Amazon Identity Access Management Badge](https://img.shields.io/badge/Amazon%20Identity%20Access%20Management-DD344C?logo=amazoniam&logoColor=fff&style=for-the-badge)
![Amazon S3 Badge](https://img.shields.io/badge/Amazon%20S3-569A31?logo=amazons3&logoColor=fff&style=for-the-badge)
![Amazon API Gateway Badge](https://img.shields.io/badge/Amazon%20API%20Gateway-FF4F8B?logo=amazonapigateway&logoColor=fff&style=for-the-badge)