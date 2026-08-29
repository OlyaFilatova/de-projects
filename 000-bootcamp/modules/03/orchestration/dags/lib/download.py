import shutil
import tempfile
from pathlib import Path

import requests

def download_file(
  url: str,
  folder_prefix: str,
  filename: str,
) -> Path:

  temp_dir = Path(
    tempfile.mkdtemp(prefix=folder_prefix)
  )

  file = temp_dir / filename


  response = requests.get(
    url,
    stream=True,
    timeout=300,
  )
  response.raise_for_status()

  with file.open("wb") as output:
    shutil.copyfileobj(response.raw, output)

  return file
