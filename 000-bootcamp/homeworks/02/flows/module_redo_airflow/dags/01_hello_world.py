from datetime import datetime

from airflow.sdk import dag, get_current_context, task, Param

@dag(
  dag_id="01_hello_world",
  schedule="0 10 * * *",
  catchup=False,
  max_active_runs=2,
  tags=["module_follow"],
  params={
    "name": Param(
      default="Olha",
      type="string",
      description="Name to greet",
    )
  },
)
def hello_world():
    @task
    def hello_message(name: str):
      """
      #### Say hi.
      """
      context = get_current_context()
      name = context["params"]["name"]

      output = f"Hello, {name}!"
      print(output)
      return output

    name = "{{ params.name }}"

    hello_message(name)

hello_world()
