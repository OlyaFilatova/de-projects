import gzip
import shutil
import tempfile
from pathlib import Path

import requests


def download_taxi_data(
  data_url: str,
  taxi: str,
  filename: str,
) -> Path:

  temp_dir = Path(
    tempfile.mkdtemp(prefix="nyc_taxi_")
  )

  compressed_file = temp_dir / f"{filename}.gz"
  csv_file = temp_dir / filename

  url = data_url.format(
    taxi=taxi,
    file=filename,
  )

  response = requests.get(
    url,
    stream=True,
    timeout=300,
  )
  response.raise_for_status()

  with compressed_file.open("wb") as output:
    shutil.copyfileobj(response.raw, output)

  with gzip.open(compressed_file, "rb") as source:
    with csv_file.open("wb") as target:
      shutil.copyfileobj(source, target)

  compressed_file.unlink()

  return csv_file
