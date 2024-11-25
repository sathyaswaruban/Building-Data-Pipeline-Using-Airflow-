# Airflow Task Group Pipeline

This project demonstrates how to build an ETL pipeline in Apache Airflow using **Task Groups**. Task Groups allow for organizing tasks into logical groups within a Directed Acyclic Graph (DAG), improving readability and maintainability of complex workflows.

## Project Overview

In this project, we will:

1. Create an Airflow pipeline (DAG).
2. Use Task Groups to organize related tasks (e.g., data extraction, transformation, loading).
3. Utilize Airflow's features such as scheduling, logging, to make the pipeline more robust.

## Pre-requisites

Before running this project, make sure you have the following installed:

- Python 3.x
- Apache Airflow (preferably the latest stable version)
- Virtualenv or any Python environment manager

Additionally, ensure you have the necessary Python dependencies.
  -- pandas
  
## Directory Structure
  ├── dags/
│   ├── Creating_pipeline_with_branching.py.   # Main DAG definition using Task Groups and script to load , transform and store dataset

├──datasets/
    |insurance_airflow_dataset.csv  #Dataset used in this project
 
## DAG: Creating_pipeline_with_branching.py  # Main DAG definition using Task Groups and script to load , transform and store dataset
  Open this python file  in the respository to view code.

## EXPLANATION
  In this Project 
        -- created a DAG having Task group
        -- DAG Configuration 
           dag_id='python_pipeline_taskgroups_Prj',
           description='Running a python branching pipeline with TaskGroups',
           default_args=default_args,
           start_date = days_ago(1),
           schedule_interval= '@once',
           tags=['branching_pipeline','csv_dataset','transform','task_groups'], 
          
       -- Dataset Load/Extract --- The read_csv function access data from the data path given 
       -- Cleaninig Dataset --- remove_null function removes null values and pushed it to Xcom   
                                remove_duplicate function removes the duplicate values from the dataset pulled from remove_null function and push the dataset to xcom
       -- Branching ---- determine_brach function used to determine which brach function(filtering , grouping ) to be executed by accesing the key value from the Variable in the airflow 
       -- Dataset Tranformation ----- Dataset are transformed based on the brach Task_id got from determine_brach function
                                                  # Filtering Branch 
                                                       --filtering.filter_by_northwest 
                                                       --filtering.filter_by_northeast 
                                                       --filtering.filter_by_southwest 
                                                       --filtering.filter_by_southeast 
                                                  # grouping Branch
                                                       --grouping.groupby_smoker
                                                       --grouping.groupby_regi  

      -- Load / Saving Dataset ---- The transformed dataset are saved in the directory given in the code
                                          Saved Dataset -- groupby_smoker.csv
                                                        -- northwest.csv
                                                       
