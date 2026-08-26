import gzip
import requests

def download_and_unzip(url, local_path):
  response = requests.get(
    url,
    stream=True,
    timeout=120,
  )
  response.raise_for_status()

  with gzip.GzipFile(fileobj=response.raw) as gz:
    with local_path.open("wb") as output:
      while chunk := gz.read(1024 * 1024):
        output.write(chunk)
