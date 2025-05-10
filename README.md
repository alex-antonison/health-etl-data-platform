# Welcome to HealthETL's Data Platform

At HealthETL, we are focused around efficiently and securely ingesting client health monitoring data to help our patient population achieve their health goals.

## Overview

### Project Setup

To simulate data, I first spin up a postgres database via docker in the [app-database](./app-database/) directory. With docker running, all you need to do is run:

```bash
docker-compose up -d
```

Once the database is running, you can then go into the [data-producer](./data-producer/) and run the [wearable_data_producer.py](./data-producer/wearable_data_producer.py) script. This will run periodically and both add and update some data using the Faker python package.

Once that is up and going, you can then go into the [health_etl](./health_etl/) directory and run the [incremental_load.py](./health_etl/incremental_load.py) to incrementally load data from the postgres database into DuckDB.

From there, you can go into the [dbt](./health_etl/dbt/) directory to run commands like `dbt build` to run and test the models. First you will need to run `dbt deps` to install the `dbt_utils` package.

When done with the project, you can spin the postgres database with:

```bash
docker-compose down
```

### Source Data

While the following is our current application model, we are committed to meeting our patients where they are at so as our patient's needs evolve, so will our application!

```text
# Base class
class AppResult:
    created_time: (datetime)
    modified_time: (datetime)
    content_slug: (string)
    patient: (pk)

# Concrete subclass
class DateTimeAppResult(AppResult):
    value: (datetime)

# Concrete subclass
class IntegerAppResult(AppResult):
    value: (integer)

# Concrete subclass
class RangeAppResult(AppResult):
    from: (integer)
    to: (integer)
```

### Data Platform Needs

* **Velocity** The Data Platform needs to be updated daily.
* **Variety** The Data Platform needs to be able to handle changes to the application database schema (aka schema evolution).
* **Volume** The Data Platform needs to be able to handle processing large volumes of data.
* **Veracity** The Data Platform needs to test source data to ensure only high quality results are served via data products.
* **Value** The Data Platform architecture needs to be cost effective in order to ensure the cost does not outweigh the ROI of the downstream data products.
* **Governance** The Data Platform needs to securely ingest and manage sensitive patient data.

### Source Control, Continuous Integration/Continuous Deployment, and code quality

* GitHub will be used for source control.
* GitHub Actions for Continuous Integration/Continuous Deployment.
* pre-commit will be used to perform tasks such as linting and formatting on each commit.
* ruff and sqlfluff will be used for linting and formatting python and sql
* PyTest will be used for unit testing Python code.
* dbt source and model tests will be used for ensuring source data and transformed data is correct.

### Documentation Strategy

#### Data Platform

The HealthETL Data Engineering team will utilize the [Documentation System](https://docs.divio.com/documentation-system/) as an approach for communicating across the HealthETL organization how the data platform operates and how to interact with it.

#### Data Model and Asset Documentation

For data model documentation, dbt schema.yml files will be created for all source tables as well as created models to ensure clarity.

As source tables and models evolve over time, the data contracts between the HealthETL Software Engineering team and the Data Engineering team will be updated to ensure technical and business alignment.

### Cloud Platform

[aws](aws.amazon.com) is the Cloud Platform of choice for HealthETL as our team has deep experience with building and deploying scalable applications on AWS.

### Data Transformation Tools

* [dbt](https://www.getdbt.com/) is the leading data transformation tool in the Data Engineering world. The ability to bring Software Development best practices to the Extract-Load-Transform pattern and allowing for teams to utilize the existing compute power of their Cloud Data Warehouse makes it a clear choice. Couple with it the ability to define data tests on both source and created data models, unit tests for sql transformations, and allowing co-locating data asset documentation with the SQL only makes it a more clear winner.
  * [sqlmesh](https://sqlmesh.com/) is another popular data transformation tool and includes with it some appealing features such that make it less dependent on external orchestration tools like Dagster. However, it is still an evolving tool and has less market adoption than dbt making it more challenging to hire people with experience for it.

### Orchestration Tool

* [Dagster](https://dagster.io) is a popular Orchestration tool in the Data Engineering world as it was the first orchestration tool to take a different approach to orchestrating data pipelines where it is "data-asset-centric." Combine with that the close integration with dbt, it is a great Orchestration option. It supports fully managed, hybrid, and self-hosting options. The fully-managed approach will not work for HealthETL based on the sensitivity of our data, however the hybrid option is viable as no data would leave the HealthETL environment. Lastly, the open source option is still viable but there is a developer cost to standing it up and running it internally.
  * [airflow](https://airflow.apache.org) is the dominant Orchestrator tool across the industry. And while even in the recent version it adopted the concept of assets and can be extended to support dbt, dagster is built around the concept of data assets, natively supports dbt, and provides a better local development experience to Airflow which are all key deciding factors for the HealthETL team.

### Cloud Data Warehouse

The HealthETL Data Platform needs to be able to transform and aggregate massive amounts of data. As such, traditional OLTP databases that are optimized for application databases will not suffice. As such, OLAP databases such as ClickHouse, Snowflake, or Redshift are a more appropriate choice.

First, to identify features that all databases support:

1. Role Based Access Control.
2. Can read files from AWS S3.
3. Can scale up and down based on workload and user needs.
4. There is either a first or third party adapter for dbt.

#### Database Comparison

1. **Redshift** is the AWS supported Cloud Data Warehouse with close integrations with a wide variety of AWS services such as Sagemaker, Lakeformation, etc. However, while Redshift in recent years came out with a Serverless mode of operation, it generally struggles with "spikey" query patterns leading to require provisioning a larger than necessary cluster most of the time. Additionally, it can be challenging to appropriately tune Redshift databases and being that it is not a popular data warehouse, it can be challenging to find Data Engineers with the background to efficiently support it. That all being said, Redshift is not the best fit.
2. **Snowflake** has become a dominant Cloud Data Warehouse in the Data Engineering space for some time. Between it adopting the concept of separating compute and storage, features like time-travel (well before Open Table Formats), database cloning, and strong integrations across all major cloud platforms (AWS, GCP, and Azure); it is the clear winner.
3. **ClickHouse** is an up-and-coming open source Cloud Data Warehouse that has both self-hosting and managed hosting options. While it does not have the same level of integrations as Snowflake, it has a more transparent cost model and can be operated at a lower cost than Snowflake. That being said, it is not as fully featured as Snowflake and not many modern data tools integrate well with it.

#### Cloud Data Warehouse Conclusion

Snowflake will be the Cloud Data Warehouse that HealthETL uses for transforming and serving up data. This was largely decided by the deep integrations Snowflake has with the general modern data stack ecosystem but in particular with AWS, Dagster, and dbt.

****Note*** In the spirit of keeping this project fully local, I will be using DuckDB instead. But for the narrative, Snowflake is the Cloud Data Warehouse of choice.

### Extract and Load Data

The HealthETL Data Platform needs to have the ability to extract new or updated data. There are a handful of solutions that could be used.

#### Managed Services

1. **AWS Database Migration Service** This is a fully managed AWS service that allows for migrating data from a source database to a target location, whether that is a database or S3. Being that the timeliness requirement is only once every 24 hours, this solution would not make sense. However, if down the road there is a desire to ingest data more frequently, this could be an option.
2. **Fivetran** Fivetran is the most fully featured Extract-Load vendor in the space providing a streamlined user experience moving data between source-to-target. However, it is also the most expensive. Being that the given use case is from an internal application and not complicated, Fivetran is perhaps not the best option

#### Open Source

1. **Airbyte** is a great open source option for Extract-Load as it supports a wide variety of sources and destinations. However, it is not python-centric and requires hosting it as a separate service.
2. **data-load-tool (dltHub)** is another great open source option for Extract-Load and while it is less featured than Airbyte, being that it is a python package it is easier to integrate into existing python data pipelines.

#### Extract Load Data Conclusion

With the use case to only ingest data once per day and supporting schema evolution, dltHub is the winner as it is the simplest solution to implement and satisfies the current Data Platform needs.

### Tech Stack

* **Transformation** [dbt](https://www.getdbt.com/)
* **Orchestration** [Dagster](https://dagster.io)
* **Cloud Data Warehouse** [Snowflake](https://www.snowflake.com/) (but really [DuckDB](https://duckdb.org/))
* **Extract Load** [dltHub](https://dlthub.com/)

### Data Governance

To ensure only the appropriate people have access to the data to help our patients, we have defined the following roles and detailed what roles have access to what data.

|Role|Access Description|PHI Access|
|-|-|-|
|Software Developers|Access to development environment data only.|False|
|Data Engineers|Full access to data pipelines and infrastructure.|Limited for debugging|
|Data Analysts|Read-only access to de-identified data for analysis.|False|
|Data Scientists|Read-only access to de-identified data for for model development.|False|
|Machine Learning Engineers|Read-only access to de-identified data for for model development.|False|
|3rd parties responsible for extracting data from hospital EHRs|Data API Access will be provided managed via OAuth Keys. Row Level Security via Snowflake Row Access Policies will be implemented to ensure only applicable data is made available via external facing datasets.|True|
