from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=[],
                 count_sql_template="",
                 redshift_conn_id="",
                 additional_checks={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.tables = tables
        self.count_sql_template = count_sql_template
        self.redshift_conn_id = redshift_conn_id
        self.additional_checks = additional_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            count_sql = self.count_sql_template.format(table=table)
            records = redshift_hook.get_records(count_sql)
            fail_msg = "Data quality check failed."
            pass_msg = "Data quality check passed on table {tble}"
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"{fail_msg} {table} returned no results.")

            num_records = int(records[0][0])
            if num_records < 1:
                raise ValueError(f"{fail_msg} {table} contained 0 rows.")

            pass_msg = pass_msg.format(tble=table)
            logging.info(f"{pass_msg} with {num_records} records.")

        for table, checks in self.additional_checks.items():
            for check_name, check_query in checks.items():
                null_record_count = int(redshift_hook.get_records(check_query)[0][0])
                if null_record_count > 0:
                    raise ValueError(f"{check_name} check failed: table {table}")

        logging.info("All Data quality checks passed.")
