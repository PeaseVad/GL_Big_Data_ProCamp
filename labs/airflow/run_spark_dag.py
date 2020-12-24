import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStorageObjectSensor

# Output file for Cloud Dataproc job.

# Path to Hadoop wordcount example available on every Dataproc cluster.
SPRARK_JAR = (
    'gs://globallogic-procamp-bigdata-datasets/airoport-destination-df_2.12-1.0.jar'
)

bucket =models.Variable.get('gcs_bucket')

file_prefix="flights/"
file_suffix="/_SUCCESS"

full_path = file_prefix+datetime.datetime.now().strftime('%Y/%m/%d/%H')+file_suffix


# Arguments to pass to Cloud Dataproc job.

spark_args = ['com.procamp.MainRun']

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'composer_spark_job',
        # Continue to run DAG once per day
        schedule_interval='0 *,* * * *',
        default_args=default_dag_args,
        catchup=False) as dag:

  
    gcs_file_sensor = GoogleCloudStorageObjectSensor(
        task_id='gcs_file_sensor_task',
        bucket=bucket,
        object=full_path,
        timeout=3600)

  # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/code.html#default-variables
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        num_workers=2,
        zone=models.Variable.get('gce_zone'),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')


    run_dataproc_spark = dataproc_operator.DataProcSparkOperator(
        task_id='run_dataproc_spark',
        main_jar=SPRARK_JAR,
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        arguments=spark_args)

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
gcs_file_sensor>>create_dataproc_cluster >>run_dataproc_spark >> delete_dataproc_cluster