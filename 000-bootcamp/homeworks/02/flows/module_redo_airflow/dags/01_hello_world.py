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
    def hello_message():
      """
      #### Say hi.
      """
      context = get_current_context()
      name = context["params"]["name"]
      output = f"Hello, {name}!"
      print(output)
      return output

    @task
    def generate_output():
        return "I was generated during this Hello World workflow"

    @task
    def sleep():
        import time
        time.sleep(15)

    @task
    def log_output(output: str):
        print(f"This is an output: {output}")

    @task
    def goodbye_message():
      context = get_current_context()
      name = context["params"]["name"]
      print(f"Goodbye, {name}!")

    hello = hello_message()
    output = generate_output()
    wait = sleep()
    logged_output = log_output(output)
    goodbye = goodbye_message()

    hello >> output >> wait >> logged_output >> goodbye # type: ignore

hello_world()
