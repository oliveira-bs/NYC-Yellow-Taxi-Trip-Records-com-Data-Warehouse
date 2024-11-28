import json

import config.database_config as db_config
from modules.database import create_tables, persist_data, start_connection
from modules.ingest import ingest_data
from modules.process import process_yellow_tripdata, process_zone_lookup
from modules.transform import (create_location_dim, create_payment_type_dim,
                               create_rate_code_dim, create_time_dim,
                               create_trips_fact, create_vendor_dim)

DB_NAME = db_config.DB_NAME

# Caminhos para os arquivos Parquet
PARQUET_FILES = {
    "vendor_dim": "data/warehouse/vendor_dim.parquet",
    "time_dim": "data/warehouse/time_dim.parquet",
    "location_dim": "data/warehouse/location_dim.parquet",
    "rate_code_dim": "data/warehouse/rate_code_dim.parquet",
    "payment_type_dim": "data/warehouse/payment_type_dim.parquet",
    "trips_fact": "data/warehouse/trips_fact.parquet",
}


def get_schema():
    # Carregar o schema de um arquivo JSON
    with open("config/schema.json", "r") as file:
        schema = json.load(file)
        return schema


print("Iniciando o pipeline...")

# Ingestão de Dados
print("\n\nEtapa 1: Ingestão dos Dados")
ingest_data()


# Processar os DataFrames
print("\n\nEtapa 2: Processamento dos Dados")
# zone_shapefile = process_zone_shapefile()
zone_lookup = process_zone_lookup()
yellow_dataframes = process_yellow_tripdata()


# Processar os DataFrames
print("\n\nEtapa 3: Modelagem Dimensional")

table_time_dim = create_time_dim(yellow_dataframes, get_schema())
table_location_dim = create_location_dim(zone_lookup, get_schema())
table_vendor_dim = create_vendor_dim(get_schema())
table_rate_code_dim = create_rate_code_dim(get_schema())
table_payment_type_dim = create_payment_type_dim(get_schema())
table_trips_fact = create_trips_fact(
    yellow_dataframes, table_time_dim, table_location_dim, table_vendor_dim,
    table_rate_code_dim, table_payment_type_dim, get_schema())

# Persistir os dados no banco
print("\n\nEtapa 4: Persistência dos Dados\n")

# Criação do banco de dados
start_connection(DB_NAME)

# Criar as tabelas no banco de dados
create_tables()
persist_data(PARQUET_FILES)
