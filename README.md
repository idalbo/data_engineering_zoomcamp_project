# Final project for Data Engineering Zoomcamp course

## Dataset and Problem

COVID19 and the consequences of the spreading virus at a pandemic level have been shaping our lives for the last two and a half year. In this project, I would like to focus on the solidification and integration of data with different granularity in one place (inside a DWH), using daily updates from the Italian Health Ministry ([source](https://github.com/pcm-dpc/COVID-19)). Different files will be loaded into a Google Cloud Storage bucket and then transformed and loaded into Google BigQuery, specifically:

* regional trend at a daily level
* provincial trend at a daily level
* snapshot of population information at a regional level

The final goal would be to bring the created tables into a reporting tool to shed light into regional and provincial trends all in one tool, where filters will play an important role for the consumers to be able to gain insights about their questions. Data will be aggregated at a weekly level and information about the population will be given at a very high-level (note that due to the short length of this process, the generated dashboard is only a simple example of how the generated tables could be used).

## Procedure

This is the overview of the ETL for this project:

* data will be downloaded locally from a github repository as raw csv files
* data will be dumped into a data lake into Google Cloud Storage (GCS)
* from the data lake, data will be loaded into a data warehouse and raw tables will be generated into BigQuery
* here, transformation will happen where fact and dimension tables will be created into a different schema (production)
* the production tables will be linked to Google Data Studio and visualized

The following technologies will be used throughout the project:

* airflow: orchestration of tasks throughout the process
* GCP: cloud platform hosting the data lake and DWH
* terraform: infrastructure as a code
* basic queries: transformation of tables (note that spark has issue with writing tables into BigQuery due to a bug into the hadoop jar, therefore this step could not be completed using this technology, although the code for it was readily written)
* GDS: reporting tool 

## Running the project

### prerequisites

You should have a Google Cloud Platform subscription (If you don't have a GCP account and you would like to follow along completelly, please follow [this link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/2_gcp_overview.md), you will get 300$ for three months usage) and create a `service account` with the following permissions:

* BigQuery Admin
* BigQuery Data Editor
* BigQuery Job User
* BigQuery User

On top of that, you should download the json credential file for this service account and store it in the folder `HOME\.google\credentials` in your local computer (the `HOME` folder is usually your user folder). This file should be named `google_credentials`.

### terraform

This step generates resources inside your Google Cloud Platform account

* cd inside the terraform folder
* if on windows, execute `gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS` (where the GOOGLE_APPLICATION_CREDENTIALS have been added to the environmental variables of your User, referencing the json with the credentials)
* `terraform init`
* `terraform plan` (give you your GCP project ID)
* `terraform apply` (give you your GCP project ID)

### airflow

The following steps will guide you through the initiation and deployment of a local airflow image that will allow you to run the entire orchestration, with the condition that you have an active GCP account. 

#### docker setup

Browse into the `airflow` folder, here you will find the `Dockerfile` and the `docker-compose.yml` files, which are used to start your docker image. This should run without any issues, but you first need to change some variables before composing it. Open the `.env` file and change the following variables:

* `GCP_PROJECT_ID`: use your GCP project ID
* `GCP_GCS_BUCKET`: change this to the bucket you want your data to be dumped and taken from

#### structure of the airflow folder

Within the airflow folder, you will find all the necessary file to start the docker image of airflow. If you browse to the `dag` folder, you will see several `.py` files used in the project, specifically:

* `dag_covid_italy`: the main DAG orchestrating the single tasks, this is what you will see when opening the airflow UI
* `dag_utils`: a set of python functions called in specific tasks (mostly `PythonOperator` or `BranchOperator`) 
* `queries`: parameterized queries used within `BigQuery` operators

#### running airflow

Open the terminal and cd into the `airflow` folder, then simply run `docker-compose up` and wait for the container to start. Note that it might take some minutes as `pyspark` needs to be installed as well. You can check the status of the container and its components by running `docker ps` in a new terminal window. When everything is up and running (it takes about 5 minutes, you can check when all services are `healthy` through the previously mentioned terminal command), open the browser of your choice and go to `http://localhost:8080/` to interact with the airflow UI.

#### DAG structure

![image](https://user-images.githubusercontent.com/49947038/160274602-d55ff8df-0e9d-400b-989e-85971d747c29.png)

As you can see from the image above, there are three different graphs going from the dummy start task to the dummy end task. As the upper one is a rather static branch, the middle and bottom are very similar, with only difference being which data the deal with. To go more into details:

* the graph starting with `branch_population_task` looks if the static file with the snapshot of the Italian population already exists in the destination bucket. If yes, then this branch reaches the end, if no, then the file is locally downloaded, uploaded into the bucker under the `population` folder, and then in-turn an external table, a staging table, and a fact production table are generated
* the other two branches deal, with the same logic, with regional and provincial daily data for COVID in Italy. First, the file with the daily data is saved locally and then uploaded to the selected bucket, either in the `province` or `region` folder. Then, an external table with all the files for each folder is generated in BiqQuery (`external_table_tasks`). From this point, a `BranchOperator` is used to determine if the staging table for each of the two dataset already exists in BigQuery: if not, the table is created with partition and cluster anew and loaded with data, if yes, then data are loaded into the staging table upon verification that no duplicates are present. The next step is again a `BranchOperator`, where the day of the week is fed to this python function: if the day of the `logical_date` of Airflow is Sunday (i.e., in this case it will run on a Monday), then the graph follows the `branch_append_or_create_prod_task`, otherwise both `province` and `region` branches reach the end. In case the run is on a Monday, another `BranchOperator` checks if the destination table already exists in production: if yes, then data are loaded into a partition and cluster table upon checking for duplicates, if no, then the table is created from scratch and loaded with data

The DAG is set to run every day at 5 UTC

### GCP generated files and tables

If you run the DAG, even just for one day (or better, for one week), you should see in your Google Cloud Platform account the following new resources:

* in your defined Google Data Storage bucket, you will see three subfolders: `population`, `province`, and `region`, where the raw data in `.parquet` format are stored
* within your Google BigQuery data warehouse, you will see two new schemas, namely:
  * `dtc_de_project_dev`: here you will find external tables generated by using the data in the bucket (with the suffix `_raw`), and partitioned&clustered tables obtained from the raw data
  * `dtc_de_project_prod`: here you will find aggregated fact tables of regional and provincial covid numbers in Italy at a weekly level (note that by mistake they are called `dim_` here, but they should be `fact_`), plus a dimension table containing a snapshot of the Italian population numbers (again, by mistake here I switched the prefix and this is called `fact_`)

note that tables within the `dtc_de_project_prod` will only appear once you run the DAG for at least one week.

### Report in GDS

After the production table were ready, a Google Data Studio simple report was generated, you can access it through the [following link](https://datastudio.google.com/reporting/038356f3-3fb1-411a-9c80-c962f5d2f583). For the sake of simplicity, only few simple visuals of regional data, as well as two KPI cards with population information were displayed. 

![image](https://user-images.githubusercontent.com/49947038/160274413-1be98a1f-13a3-4088-8e5c-6fcb05bef4ec.png)

The time series in the upper part of the dashboard shows the development of new positive cases in Italy through the pandemic, where also the delta of the positive (difference between current and previous week) and the average deceased people due to COVID are displayed.

The bottom bar chart shows the distribution of COVID cases in Italy per region, in descending order. Additionally to the two population specific KPI cards, two additional cards show the total detected cases of COVID, as well as the average deceased population. 

Two filters, one for limiting the time window and one for selecting only specific regions, are available. 

With the generated tables in the DWH, additional charts could be generated to get more insights, for example at the provincial level, as well as a better link with the population static data. 

