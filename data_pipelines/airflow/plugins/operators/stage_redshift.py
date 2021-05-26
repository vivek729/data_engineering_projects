from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 create_table_sql="",
                 stage_table_sql="",
                 redshift_conn_id="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="",
                 table="",
                 json_path="auto",
                 timeformat="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.create_table_sql = create_table_sql
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.stage_table_sql = stage_table_sql
        self.table = table
        self.json_path = json_path
        self.timeformat = timeformat

    def execute(self, context):
        self.log.info(f"Creating table {self.table}")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(self.create_table_sql)
        self.log.info(f"DONE - {self.table} created.")

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        redshift_s3_read_role_arn = Variable.get("redshift_s3_read_role_arn")
        self.log.info(f"Copying {self.table} from {s3_path}...")
        stage_table_sql_final = self.stage_table_sql.format(
            table=self.table,
            s3_path=s3_path,
            redshift_s3_read_role_arn=redshift_s3_read_role_arn,
            json_path=self.json_path,
            s3_region=self.s3_region,
            timeformat=self.timeformat
        )
        redshift_hook.run(stage_table_sql_final)
        self.log.info(f"DONE - Copied {self.table} from {s3_path}.")
