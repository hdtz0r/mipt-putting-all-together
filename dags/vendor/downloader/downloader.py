import requests

def download(url: str, output_file: str):
    response = requests.get(url, stream = True)
    if response.status_code == 200:
        with open(output_file, "wb") as output:
            for chunk in response.iter_content(chunk_size = 209715200):
                if chunk:
                    output.write(chunk)