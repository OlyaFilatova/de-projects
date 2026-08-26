from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload(filename, local_path, aws_conn_id, bucket_name):
  try:
    s3 = S3Hook(
      aws_conn_id=aws_conn_id,
    )

    s3.load_file(
      filename=str(local_path),
      key=filename,
      bucket_name=bucket_name,
      replace=True,
    )
  finally:
    local_path.unlink(missing_ok=True)
