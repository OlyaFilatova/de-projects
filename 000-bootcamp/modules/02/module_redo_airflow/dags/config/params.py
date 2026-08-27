from airflow.sdk import Param

taxi = Param(
  "yellow",
  type="string",
  enum=["yellow", "green"],
  title="Taxi type",
)

year = Param(
  "2019",
  type="string",
  enum=["2019", "2020"],
  title="Year",
)

month = Param(
  "01",
  type="string",
  enum=[
    "01", "02", "03", "04",
    "05", "06", "07", "08",
    "09", "10", "11", "12",
  ],
  title="Month",
)
