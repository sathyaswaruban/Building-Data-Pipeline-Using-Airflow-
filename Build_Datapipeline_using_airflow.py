import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable 
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

from random import choice
from datetime import datetime, timedelta

default_args ={
    'ownername':'root'
}
# give your dataset path 

DATASET_PATH = '' 

# give your dataset path to save proceesed dataset
OUTPUT_PATH =''

def read_csv_file(ti):
    df = pd.read_csv(DATASET_PATH)
    print(df.head(20))

    ti.xcom_push(key='OG_Dataset' , value=df.to_json())

def remove_null_values(ti):

    json_data= ti.xcom_pull(key='OG_Dataset')

    df = pd.read_json(json_data)
    df=df.dropna()
    print(df.head(10))

    ti.xcom_push(key='Dataset_without_null', value=df.to_json())

def remove_duplicates(ti):
    print('enter')
    df = ti.xcom_pull(key ='Dataset_without_null')
    df_cleaned = pd.read_json(df)
    print(df_cleaned.head(10))
    df_cleaned = df_cleaned.drop_duplicates(subset=['age','sex','bmi'])
    print("Duplicate Data Removed Successfully")
    ti.xcom_push(key='Cleaned_Dataset', value= df_cleaned.to_json())

def determine_branch():
    transform_action = Variable.get("transform_action",default_var = None)
    print(transform_action)
    if transform_action.startswith('filter'):
        return "filtering.{0}".format(transform_action)
    elif transform_action.startswith('groupby'):
        return "grouping.{0}".format(transform_action)

def filter_by_southwest(ti):
    json_data = ti.xcom_pull(key='Cleaned_Dataset')
    df = pd.read_json(json_data)
    region_df = df[df['region']=='southwest']
    print("Data filtered Successfully")

    region_df.to_csv(OUTPUT_PATH.format('southwest'),index=False)

def filter_by_southeast(ti):
    json_data = ti.xcom_pull(key='Cleaned_Dataset')
    df = pd.read_json(json_data)
    region_df = df[df['region']=='southeast']
    print("Data filtered Successfully")
   
    region_df.to_csv(OUTPUT_PATH.format('southeast'),index=False)

def filter_by_northeast(ti):
    json_data = ti.xcom_pull(key='Cleaned_Dataset')
    df = pd.read_json(json_data)
    region_df = df[df['region']=='northeast']
    print("Data filtered Successfully")
   
    region_df.to_csv(OUTPUT_PATH.format('northeast'),index=False)

def filter_by_northwest(ti):
    json_data = ti.xcom_pull(key='Cleaned_Dataset')
    df = pd.read_json(json_data)
    region_df = df[df['region']=='northwest']
    print("Data filtered Successfully")
   
    region_df.to_csv(OUTPUT_PATH.format('northwest'),index=False)

def groupby_smoker(ti):
    json_data=ti.xcom_pull(key='Cleaned_Dataset')
    df = pd.read_json(json_data)

    smoker_df = df.groupby('smoker').agg({
        'age':'mean',
        'bmi':'mean',
        'expenses':'mean',
    }).reset_index()

    smoker_df.to_csv(OUTPUT_PATH.format('grouped_by_smokers'),index=False)

def groupby_region(ti):
    json_data=ti.xcom_pull(key='Cleaned_Dataset')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age':'mean',
        'bmi':'mean',
        'expenses':'mean'
     }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('grouped_by_region'),index=False)

with DAG(
    dag_id='python_pipeline_taskgroups_Prj',
    description='Running a python branching pipeline with TaskGroups',
    default_args=default_args,
    start_date = days_ago(1),
    schedule_interval= '@once',
    tags=['branching_pipeline','csv_dataset','transform','task_groups']
) as dag :
    with TaskGroup('reading_and_preprocessing') as reading_and_preprocessing:
        read_csv_files = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
        )
        remove_null_value = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
        )

        remove_duplicates = PythonOperator(
        task_id='remove_duplicates',
        python_callable=remove_duplicates
        )
        read_csv_files >> remove_null_value >> remove_duplicates


    determine_branch = BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable = determine_branch
    )
    with TaskGroup('filtering') as filtering:
        filter_by_northeast = PythonOperator(
        task_id='filter_by_northeast',
        python_callable=filter_by_northeast
        )

        filter_by_northwest = PythonOperator(
        task_id='filter_by_northwest',
        python_callable=filter_by_northwest
        )

        filter_by_southeast = PythonOperator(
        task_id='filter_by_southeast',
        python_callable=filter_by_southeast
        )

        filter_by_southwest = PythonOperator(
        task_id='filter_by_southwest',
        python_callable=filter_by_southwest
        )

    with TaskGroup('grouping') as grouping:   
        groupby_smoker = PythonOperator(
        task_id='groupby_smoker',
        python_callable=groupby_smoker
        )
        groupby_region = PythonOperator(
        task_id='groupby_region',
        python_callable=groupby_region
        )

reading_and_preprocessing >>Label('Processed_Data') >>determine_branch >> Label("branch on condition")>>[filtering,grouping]
