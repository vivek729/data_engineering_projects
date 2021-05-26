from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_table_sql="",
                 insert_table_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.insert_table_sql = insert_table_sql

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info(f"Creating table {self.table}...")
        redshift_hook.run(self.create_table_sql)
        self.log.info(f"Done - Table {self.table} created.")

        self.log.info(f"Loading data to table {self.table}...")
        redshift_hook.run(self.insert_table_sql)
        self.log.info(f"Done - Loaded data to table {self.table}.")
