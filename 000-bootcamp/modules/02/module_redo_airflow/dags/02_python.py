from airflow.sdk import dag, task


@dag(
  dag_id="02_python",
  description="Collect Docker Hub download statistics.",
)
def python_dag():
  @task.virtualenv(
    system_site_packages=False,
    requirements=["requests"],
  )
  def collect_stats_task():
    from lib.collect_stats import collect_stats

    image_name = "kestra/kestra"
    stats = collect_stats(image_name)
    print(stats)

  collect_stats_task()

python_dag()
