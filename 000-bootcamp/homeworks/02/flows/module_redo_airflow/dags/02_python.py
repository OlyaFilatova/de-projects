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
  def collect_stats():
    import requests

    image_name = "kestra/kestra"
    url = f"https://hub.docker.com/v2/repositories/{image_name}/"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    downloads = data.get("pull_count", "Not available")

    return {
      "downloads": downloads,
    }

  collect_stats()

python_dag()
