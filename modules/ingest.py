import io
import os
from zipfile import ZipFile

import requests

RAW_DIR = "data/raw"

DATA_URLS = {
    "yellow_tripdata": "https://d37ci6vzurychx.cloudfront.net/trip-data/\
yellow_tripdata_2024-01.parquet",
    "zone_lookup": "https://d37ci6vzurychx.cloudfront.net/misc/\
taxi_zone_lookup.csv"
}


def extract_zip(response, extract_to):
    """
    Extrai um arquivo específico de um arquivo ZIP.

    Args:
        response: Objeto de resposta HTTP contendo o arquivo ZIP.
        extract_to: Caminho para a pasta onde o arquivo será extraído.
        target_file: Nome do arquivo a ser extraído.
    """
    with io.BytesIO(response.content) as buffer:
        with ZipFile(buffer, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            print(
                f"Arquivos extraídos para {extract_to}.")


def download_file(url, output_path):
    """
        Extrai os dados da fonte.
    Args:
        url: caminho url para extração do conteudo
        output_path: Caminho para a pasta onde o arquivo será extraído.
    """

    if not os.path.exists(output_path):
        print(f"\nBaixando - {output_path}\n{url}...\n")
        response = requests.get(url)
        if '.zip' in url:
            extract_zip(response, output_path)
        else:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Arquivo salvo em {output_path}")
    else:
        print(f"Arquivo já existe: {output_path}")


def ingest_data():
    """
        Responsavel por ingerir os dados especificados em DATA_URLS.
    """

    os.makedirs(RAW_DIR, exist_ok=True)
    download_file(DATA_URLS["yellow_tripdata"], os.path.join(
        RAW_DIR, "yellow_tripdata.parquet"))

    download_file(DATA_URLS["zone_lookup"], os.path.join(
        RAW_DIR, "taxi_zone_lookup.csv"))
