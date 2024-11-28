import json
from datetime import datetime

import pandas as pd
import pendulum
from dateutil.relativedelta import relativedelta

DATETIME = pendulum.now("America/Sao_Paulo")
RAW_DIR = "data/raw"
STAGING_DIR = "data/staging"


def get_schema():
    """
    # Carregar o schema expresso no arquivo JSON no diretorio config

    Retorna:
        json: Retorna o para a construção dos arquivos parquet em \
        data/warehouse
    """

    with open("config/schema.json", "r") as file:
        schema = json.load(file)
        return schema


def date_range(df: pd.DataFrame, columns_list: list):
    """
    Retorna o intervalo de datas para o dataframe especificado no dataframe. \
    Baseado na data mais aaaa/mm mais frequente, são estabelecidos o ultimo \
    dia do mes anterior (last_day_previous_month) e o primeiro dia do mes \
    seguinte (first_day_next_month).

    Retorna:
        date: Retorna o ultimo dia do mes anterior (last_day_previous_month) \
        e o primeiro dia do mes seguinte (first_day_next_month).
    """
    # Aplicando o filt
    date_column_list = []

    for col in columns_list:
        df[col] = pd.to_datetime(df[col])
        date_column_list.append(df[col])

    timestamps = pd.concat(date_column_list).reset_index(drop=True)
    timestamps = timestamps.sort_values(ascending=True)

    # Calculando o período mais frequente
    frequent_period = timestamps.mode()[0].to_period('M')

    # Extraindo o ano e o mês/ano/trimestre do período mais frequente
    if isinstance(frequent_period, pd.Period):
        if frequent_period.freq == 'ME':  # Para ano e mês (yyyy-mm)
            frequent_year = frequent_period.year
            frequent_month = frequent_period.month
        else:
            raise ValueError("Formato de período não suportado.")

    reference_date = datetime(frequent_year, frequent_month, 1)

    # Último dia do mês anterior
    last_day_previous_month = reference_date - relativedelta(days=1)

    # Primeiro dia do mês seguinte
    first_day_next_month = (
        reference_date + relativedelta(months=1)).replace(day=1)

    return last_day_previous_month, first_day_next_month


def apply_filter(df, last_day_previous_month, first_day_next_month):
    """
    Filtra o dataframe de acordo com o intervalo especificado em:
        last_day_previous_month, first_day_next_month.

    Retorna:
        pd.Dataframe: DataFrames filtrado para as datas selecionadas.
    """
    # Aplicando o filtro de data
    date_filter = (
        (df['pickup_datetime'] >= last_day_previous_month) &
        (df['pickup_datetime'] < first_day_next_month) &
        (df['dropoff_datetime'] > last_day_previous_month + relativedelta(
            days=1)) &
        (df['dropoff_datetime'] <= first_day_next_month)
    )

    filtered_df = df[date_filter]

    return filtered_df


def process_yellow_tripdata():
    """
    Processa os dados do arquivo yellow_tripdata.parquet e cria DataFrames
    para tabelas relacionadas: Trips, Vendors, RateCodes, e Fares.

    Retorna:
        dict: DataFrames correspondentes às tabelas do banco de dados.
    """

    file_path = f"{RAW_DIR}/yellow_tripdata.parquet"
    df = pd.read_parquet(file_path)

    # Ajustando os nomes das colunas para compatibilidade com o schema
    df = df.rename(columns=lambda x: x.lower())

    df = df.rename(columns={
        "vendorid": "vendor_id",
        "ratecodeid": "rate_code_id",
        "pulocationid": "pu_location_id",
        "dolocationid": "do_location_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "payment_type": "payment_type_id",
        "tpep_dropoff_datetime": "dropoff_datetime"
    })

    # Obtendo o último dia do mês anterior e o primeiro dia do mês seguinte
    last_day_previous_month, first_day_next_month = date_range(
        df=df, columns_list=['pickup_datetime', 'dropoff_datetime'])

    # Aplicando o filtro no DataFrame
    df = df.query("fare_amount > 0")
    df = df.query("passenger_count > 0")
    df = apply_filter(df, last_day_previous_month, first_day_next_month)

    # Substiuir os valores invalidos pelos valores dominantes nos dados
    df['rate_code_id'] = df['rate_code_id'].fillna(1)
    df['rate_code_id'] = df['rate_code_id'].replace(99, 1)
    df['passenger_count'] = df['passenger_count'].fillna(1)
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna('N')
    df['vendor_id'] = df['vendor_id'].replace(6, 2)
    df['payment_type_id'] = df['payment_type_id'].replace(0, 5)
    df['load_datetime'] = DATETIME.format('DD/MM/YYYY HH:mm:ss')

    df.to_parquet(f"{STAGING_DIR}/yellow_tripdata.parquet", index=False)
    print(f"Arquivo processado em staging: \
{STAGING_DIR}/yellow_tripdata.parquet")

    return df


def process_zone_lookup():
    """
    Processa os dados do arquivo taxi_zone_lookup.csv e cria o DataFrame \
Locations.

    Retorna:
        pd.DataFrame: DataFrame correspondente à tabela Locations.
    """

    file_path = f"{RAW_DIR}/taxi_zone_lookup.csv"

    def fill_nans(row):
        """Função para preencher valores nulos com base no valor da mesma \
            linha"""
        if row['borough'] == 'Unknown':
            row = row.fillna('Unknown')
        elif row['zone'] == "Outside of NYC":
            row = row.fillna('Outside of NYC')
        return row

    df = pd.read_csv(file_path)

    df = df.rename(columns=lambda x: x.lower())
    df = df.rename(columns={
        "locationid": "location_id"
    })
    df = df.apply(fill_nans, axis=1)

    df.to_parquet(f"{STAGING_DIR}/zone_lookup.parquet", index=False)
    print(
        f"Arquivo processado em staging: {STAGING_DIR}/zone_lookup.parquet")

    return df
