# Final project for Data Engineering Zoomcamp course

## Dataset and Problem

In this project, one of the datasets available in the website of the city of Toronto will be be used.
Specifically, bike sharing information from 2019 on will be processed and analyzed. Three different types of data will be used:

* trip data from 2019 on, where data is stored in different file formats and needs to be processed specifically during extraction (source https://open.toronto.ca/dataset/bike-share-toronto-ridership-data/)
* bikeways information in the city of Toronto (source https://open.toronto.ca/dataset/bikeways/)
* bicycle parking around the city (source https://open.toronto.ca/dataset/bicycle-parking-high-capacity-outdoor/)

the goal of this project is to finally understand the trend in bike usage in the city in the past three years, as well as if the expansion of infrastructure for bikings and parking spot locations could be a contributing factor in increasing number of bike sharing rides. Of course, many other external factors contribute to changes in this behavior. As such, this project is not meant to imply any causality between these factors.

## Procedure

This is the an overview of the data circle:

* data will be downloaded locally from the website
* after transformation, data will be dumped into a data lake
* from the data lake, data will be loaded into a data warehouse
* here, tranformation will happen where fact and dimension tables will be created
* these latter tables will be loaded into a reporting tool for dashboard generation

The following technologies will be used throughout the project_

* airflow: orchestration of tasks throughout the process
* GCP: cloud platform hosting the data lake and DWH
* terraform: infrastructure as a code
* spark: transformation of raw data into final tables
* GDS: reporting tool 
