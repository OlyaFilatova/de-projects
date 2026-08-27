from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime

from config.s3 import BUCKET_NAME, BUCKET_REGION, AWS_CONN_ID


@dag(
  dag_id="07_setup_s3",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=["module_follow", "infrastructure"],
)
def setup_s3():

  @task
  def ensure_bucket():
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

    if not s3.check_for_bucket(BUCKET_NAME):
      s3.create_bucket(
        bucket_name=BUCKET_NAME,
        region_name=BUCKET_REGION,
      )

  ensure_bucket()

setup_s3()
