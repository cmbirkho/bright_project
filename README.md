## Overview
For this project we are using the following AWS services: S3, Lambda, Athena, Glue, and Eventbridge. We will keep these assets as code and deploy to the respective AWS account using AWS's CDK and CloudFormation. The overall design of this solution is impacted by the requirements and time constraints provided. The goal is to keep the infrastructure lightweight with minimal opportunities for integration hurdles and give sufficient time to work on the data model. 

Initially dbt was deployed as a lambda layer and the dbt project files stored in S3, available to be referenced and run via `dbt run` from the lambda. The intention was to demonstrate use and experience with dbt in a lightweight solution and to take advantage of dbt's ref function to create dependencies. However, on the first deployment errors related to the lambda environment being able to reference the dbt module within the lambda layer occured. Due to these challenges I switched to plan B and created a lambda function capable of running the required queries in the required order to create the tables.

## Deployment Instructions
1. Setup environment variable. In app.py replace `os.getenv('MY_AWS_ACCOUNT_ID')` with you AWS account number or setup the environment variable. 
2. Install AWS CLI and input your credentials by running `aws configure`
3. Install Node.js `nvm install 20`
4. Install AWS CDK CLI `npm install -g aws-cdk`
5. Clone the repo `git clone <repository-url>`
6. Change to repo directory `cd <repository-directory>`
7. Install dependencies `npm install`
9. Deploy the stack `cdk deploy`
* CDK Material
    * https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html
    * https://docs.aws.amazon.com/cdk/v2/guide/hello_world.html



## Infrastructure
Our infrastructure was designed with the goal of limiting integration errors to maximize time to design the data model, and take advantage of server-less technologies to limit cost while meeting the project requirements to: 1) Files will be loaded at various times 2) File schemas are subject to change at any given time 3) New data will need to be available early next day
* S3
    * The pbright-raw-data bucket is where the raw sample files will land. We are assuming in production the files are able to be retrieved or sorted and stored in respective folders representign the sources (salesforce-leads/, source_1/, source_2/, and source_3/).
    * The pbright-athena-query-results-bucket is where all Athena query results will be stored. The Athena work group pbrightWorkgroup is configured to use this bucket.
* Glue
    * Taking advantage of schema on read we will crawl the pbright-raw-data S3 bucket and catalog the data into the Glue Data Catalog under the pbright database. The crawler will prefix these cataloged tables with raw_. The crawler is set on a schedule, but can be configured based on a trigger such as a file landing in S3.
* Lambda
    * The lambda function runs the required queries via Athena. The function references queries stored within a .py file and runs the statements 1 by 1 in the correct order to create the required tables to be referenced and queried through the Glue Data Catalog via Athena. Lambda's max runtime of 15 minutes was a major consideration, but given the data size and complexity of the required queries for this project this lightweight approach to execute the queries is sufficient at this time.  
* EventBridge
    * EventBridge is used to trigger the lambda function on the required schedule.
* Athena
    * Athena is the query engine used to create the required tables and complete any additional analysis.

## Data Model
* Leads
    * raw_salesforce_leads
        * Raw sample data provided
    * int_salesforce_leads
        * Intermediary transform containing cleaned data
    * fact_leads
        * Time series table containing metrics
* Operations
    * raw_source_3
        * Raw sample data provided
    * int_operations
        * Intermediary transform containing cleaned data
* Facilities
    * raw_source_2
        * Raw sample data provided
    * int_facility_details
        * Intermediary transform containing cleaned data
* Credentials
    * raw_source_1
        * Raw sample data provided
    * int_credentials
        * Intermediary transform containing cleaned data

## Longterm Considerations
In the longerterm I would recommend a Lakehouse architecture to support these data feeds in addition to various analytics capabilities required by a growing business. A Lakehouse has several advantages X. 

Specifically for this pipeline the following 

### Architecture *(not an exhaustive list)*
* AWS CDK
    * To deploy and maintain the infrastructure as code. Enabling deployments of pipelines to dev and prod environments in a CI/CD process.
* AWS S3
    * Data lake to store structured and unstructured data, config files, etc.
* AWS Redshift & Redshift Spectrum
    * Redshift is the datawarehouse and main query engine and contains all data relevant to the busienss
    * Redshift Spectrum is used to reference and run SQL against data stored in S3. An example of data we might only store within S3 could be unstructured data such as text, emails, documents or historical data that is no longer relevant for the business and has been archived from Redshift into S3
* AWS Lambda
    * Call 3P APIs and load the data into S3 landing buckets and ultimately into Redsfhit. As an example, we implement a scheduled a lambda to pull data from a 3P API and push to a landing bucket. The bucket has an SNS topic and feeds messages to an SQS queue which then triggers a lambda to load the data in to Redshift. In the event of failures messages are sent to a dead letter queue which triggers an alarm or notification (e.g. slack).
* AWS Glue
    * To catalog data stored within S3 and load data into Redshift
* AWS Managed Workflows Apache Airflow
    * To orchestrate and schedule dbt pipelines
* dbt
    * To write transforms against data stored on Amazon Redshift — creating data models to support the business