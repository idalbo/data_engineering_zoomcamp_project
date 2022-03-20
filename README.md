# Final project for Data Engineering Zoomcamp course

## Dataset and Problem

In this project, the COVID19 data from the Italian Health Ministry repo will be used (source https://github.com/pcm-dpc/COVID-19). Different files will be loaded into a Google Cloud Storage bucket and then transformed and loaded into Google BigQuery, specifically:

<<<<<<< HEAD
=======
* regional trend at a daily level
>>>>>>> 37d227b4ad8831380bd9fff872c76d23d5f7f374
* provincial trend at a daily level
* snapshot of population information at a regional level

the goal of this project is to have aggregated data into fact tables inside a DWH, such that easy connections with reporting tools can be established and basic exploratory data analyses can be carried out on the various datasets about COVID19 in Italy. Data will be aggregated at a weekly level and compared against the population baseline for each region.

## Procedure

This is the an overview of the data circle:

* data will be downloaded locally from github
* data will be dumped into a data lake
* from the data lake, data will be loaded into a data warehouse and raw tables will be generated
* here, tranformation will happen where fact and dimension tables will be created
* these latter tables will be loaded into a reporting tool for dashboard generation

The following technologies will be used throughout the project:

* airflow: orchestration of tasks throughout the process
* GCP: cloud platform hosting the data lake and DWH
* terraform: infrastructure as a code
* spark: transformation of raw data into final tables
* GDS: reporting tool 

## Running the project

### terraform

This step generates resources inside your Google Cloud Platform account

* cd inside the terraform folder
* if on windows, execute `gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS` (where tue GOOGLE_APPLICATION_CREDENTIALS have been added to the environmental variables of your User, referecing the json with the information)
* `teraform init`
* `terraform plan` (give you your GCP project ID)
* `terraform apply` (give you your GCP project ID)

### airflow

The following steps will guide you through the initiation and deployment of a local airflow image that will allow you to run the entire orchestration, with the condition that you have an active GCP account.

#### docker setup

The docker image should run wihtout any issues, but you first need to change some variable before building and composing the file. Open the `.env` file and change the following variables:

* `GCP_PROJECT_ID`: use your GCP project ID
* `GCP_GCS_BUCKET`: change this to the bucket you want your data to be dumped and taken from

#### structure of the airflow folder

Within the airflow folder, you will find all the necessary file to create and start the docker image of airflow. If you browse to the `dag` folder, you will find the main DAG used in the project, as well as helper files holding functions or queries used within the DAG, specifically:

* `dag_covid_italy`: the main DAG orchestrating the single tasks
* `dag_utils`: a set of python functions called in specific tasks (mostly `PythonOperator`) 
* `queries`: parameterized queries used within `BigQuery` operators

#### running airflow

open the terminal and cd into the `airflow` folder, then simply run `docker-compose up` and wait for the container to start. Note that it might take some minutes as `pyspark` needs to be installed as well. You can check the status of the container and its components by running `docker ps` in a new terminal window. When everything is up and running, open the browser of your choice and go to `http://localhost:8080/` to interact with the airflow UI.




1. First way
* save data to parquet
* load data
* move to DWH
* create tables raw
* partition and cluster
* use spark to transform
* dashboard
