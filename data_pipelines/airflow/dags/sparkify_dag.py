from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import logging
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
    'catchup': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          # max_active_runs=1
)


def ensure_redshift_role():
    try:
        redshift_s3_read_role_arn = Variable.get("redshift_s3_read_role_arn")
    except KeyError:
        logging.error("Airflow var `redshift_s3_read_role_arn` not set.")
        raise Exception("Airflow var `redshift_s3_read_role_arn` not set.")

def additional_dq_checks():
    checks = {
        "songplays": {
            "playid_check": SqlQueries.column_null_check_template.format(
                                table="songplays",
                                column="playid"
                            )

        },
        "artists": {
            "artistid_check": SqlQueries.column_null_check_template.format(
                                  table="songplays",
                                  column="playid"
                              ),
            "name_check": SqlQueries.column_null_check_template.format(
                              table="artists",
                              column="name"
                          )
        },
        "songs": {
            "songid_check": SqlQueries.column_null_check_template.format(
                              table="songs",
                              column="songid"
                            )
        },
        "time": {
            "start_time_check": SqlQueries.column_null_check_template.format(
                                    table="songs",
                                    column="songid"
                                )
        }
    }
    return checks


start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

check_variables = PythonOperator(
    task_id="check_airflow_vars",
    dag=dag,
    python_callable=ensure_redshift_role
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    create_table_sql=SqlQueries.create_events_stage,
    stage_table_sql=SqlQueries.staging_table_copy_template,
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    table="events_stage",
    json_path="s3://udacity-dend/log_json_path.json",
    timeformat="epochmillisecs",
    s3_region="us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    create_table_sql=SqlQueries.create_songs_stage,
    stage_table_sql=SqlQueries.staging_table_copy_template,
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    table="songs_stage",
    s3_region="us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    create_table_sql=SqlQueries.create_songplays,
    insert_table_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    create_table_sql=SqlQueries.create_users,
    insert_table_sql=SqlQueries.user_table_insert,
    delete_existing_records=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    create_table_sql=SqlQueries.create_songs,
    insert_table_sql=SqlQueries.song_table_insert,
    delete_existing_records=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    create_table_sql=SqlQueries.create_artists,
    insert_table_sql=SqlQueries.artist_table_insert,
    delete_existing_records=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    create_table_sql=SqlQueries.create_table_time,
    insert_table_sql=SqlQueries.time_table_insert,
    delete_existing_records=False
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "songs", "users", "artists", "time"],
    count_sql_template=SqlQueries.count_rows_template,
    additional_checks=additional_dq_checks()
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

start_operator >> check_variables
check_variables >> stage_events_to_redshift
check_variables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
