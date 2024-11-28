import pandas as pd

STAGING_DIR = "data/staging"
WAREHOUSE_DIR = "data/warehouse"


def create_time_dim(df, schema):
    """
        Recebe dataframe do staging para criar arquivo parquet base para \
        persistir a tabela time_dim no data warehouse.

    Args:
        df: Dataframe correspondente a tabela time_dim
        schema: arquivo json com os schemas dos arquivos parquet
    """

    # Combinar e obter timestamps únicos
    timestamps = pd.concat([df['pickup_datetime'],
                            df['dropoff_datetime']]
                           ).drop_duplicates().reset_index(drop=True)
    timestamps = timestamps.sort_values(ascending=True)

    # Criar DataFrame de tempo
    time_dim = pd.DataFrame({
        'date': timestamps.dt.date,
        'year': timestamps.dt.year,
        'month': timestamps.dt.month,
        'day': timestamps.dt.day,
        'day_of_week': timestamps.dt.strftime('%A'),
        'hour': timestamps.dt.hour,
        'minute': timestamps.dt.minute,
        'second': timestamps.dt.second,
        'part_of_day': timestamps.dt.hour.map(
            lambda h: 'Morning' if 5 <= h <= 11 else
                      'Afternoon' if 12 <= h <= 17 else
                      'Evening' if 18 <= h <= 21 else
                      'Night'
        )
    })

    # Adicionar ID incremental (simulando SERIAL)
    time_dim.reset_index(inplace=True)
    time_dim.rename(columns={'index': 'time_id'}, inplace=True)

    # Configurar schema do dataframe
    time_dim = time_dim.astype(schema.get('time_dim'))

    time_dim.to_parquet(f"{WAREHOUSE_DIR}/time_dim.parquet", index=False)

    print(f"Tabela time_dim processada em warehouse: \
{WAREHOUSE_DIR}/time_dim.parquet")

    return time_dim


def create_vendor_dim(schema):
    """
        Criar arquivo parquet base para \
        persistir a tabela vendor_dim no data warehouse.

    Args:
        schema: arquivo json com os schemas dos arquivos parquet
    """

    vendor_dim = [
        (1, "Creative Mobile Technologies, LLC"),
        (2, "VeriFone Inc")
    ]

    vendor_dim = pd.DataFrame(
        vendor_dim, columns=['vendor_id', 'vendor_name'])

    # Configurar schema do dataframe
    vendor_dim = vendor_dim.astype(schema.get('vendor_dim'))

    vendor_dim.to_parquet(f"{WAREHOUSE_DIR}/vendor_dim.parquet", index=False)

    print(f"Tabela vendor_dim processada em warehouse: \
{WAREHOUSE_DIR}/vendor_dim.parquet")

    return vendor_dim


def create_location_dim(df, schema):
    """
        Recebe dataframe do staging para criar arquivo parquet base para \
        persistir a tabela location_dim no data warehouse.

    Args:
        df: Dataframe correspondente a tabela location_dim
        schema: arquivo json com os schemas dos arquivos parquet
    """

    # Combinar e obter timestamps únicos
    location_dim = df.drop_duplicates().reset_index(drop=True)

    # Configurar schema do dataframe
    location_dim = location_dim.astype(schema.get('location_dim'))

    location_dim.to_parquet(
        f"{WAREHOUSE_DIR}/location_dim.parquet", index=False)

    print(f"Tabela location_dim processada em warehouse: \
{WAREHOUSE_DIR}/location_dim.parquet")

    return location_dim


def create_rate_code_dim(schema):
    """
        Criar arquivo parquet base para \
        persistir a tabela rate_code_dim no data warehouse.

    Args:
        schema: arquivo json com os schemas dos arquivos parquet
    """

    rate_code_dim = [
        (1, "Standard rate"),
        (2, "JFK"),
        (3, "Newark"),
        (4, "Nassau or Westchester"),
        (5, "Negotiated fare"),
        (6, "Group ride")
    ]

    rate_code_dim = pd.DataFrame(
        rate_code_dim, columns=['rate_code_id', 'rate_code_description'])

    # Configurar schema do dataframe
    rate_code_dim = rate_code_dim.astype(schema.get('rate_code_dim'))

    rate_code_dim.to_parquet(
        f"{WAREHOUSE_DIR}/rate_code_dim.parquet", index=False)

    print(f"Tabela rate_code_dim processada em warehouse: \
{WAREHOUSE_DIR}/rate_code_dim.parquet")

    return rate_code_dim


def create_payment_type_dim(schema):
    """
        Criar arquivo parquet base para \
        persistir a tabela payment_type_dim no data warehouse.

    Args:
        schema: arquivo json com os schemas dos arquivos parquet
    """

    payment_type_dim = [
        (1, "Credit Card"),
        (2, "Cash"),
        (3, "No Charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided Trip")
    ]

    payment_type_dim = pd.DataFrame(
        payment_type_dim, columns=['payment_type_id', 'payment_description'])

    # Configurar schema do dataframe
    payment_type_dim = payment_type_dim.astype(schema.get('payment_type_dim'))

    payment_type_dim.to_parquet(
        f"{WAREHOUSE_DIR}/payment_type_dim.parquet", index=False)

    print(f"Tabela payment_type_dim processada em warehouse: \
{WAREHOUSE_DIR}/payment_type_dim.parquet")

    return payment_type_dim


def create_trips_fact(df_process_tripdata, time_dim, location_dim, vendor_dim,
                      rate_code_dim, payment_type_dim, schema):
    """
        Recebe dataframe do staging para criar arquivo parquet base para \
        persistir a tabela trips_fact no data warehouse.

    Args:
        df: Dataframe correspondente a tabela trips_fact
        schema: arquivo json com os schemas dos arquivos parquet
    """

    print("Ajustando indexs para tabela trips_fact")

    # Merge para pickup_time_id e dropoff_time_id
    print("\tPopulando indexs para pickup_time_id e dropoff_time_id ...")

    # Converte para datetime.date (um tipo de objeto Python)
    time_dim['date'] = time_dim['date'].dt.date

    df_process_tripdata = df_process_tripdata.merge(
        time_dim[['time_id', 'date', 'hour', 'minute', 'second']],
        how='left',
        left_on=[
            df_process_tripdata['pickup_datetime'].dt.date,
            df_process_tripdata['pickup_datetime'].dt.hour,
            df_process_tripdata['pickup_datetime'].dt.minute,
            df_process_tripdata['pickup_datetime'].dt.second
        ],
        right_on=['date', 'hour', 'minute', 'second']
    ).rename(columns={'time_id': 'pickup_time_id'})

    df_process_tripdata = df_process_tripdata.merge(
        time_dim[['time_id', 'date', 'hour', 'minute', 'second']],
        how='left',
        left_on=[
            df_process_tripdata['dropoff_datetime'].dt.date,
            df_process_tripdata['dropoff_datetime'].dt.hour,
            df_process_tripdata['dropoff_datetime'].dt.minute,
            df_process_tripdata['dropoff_datetime'].dt.second
        ],
        right_on=['date', 'hour', 'minute', 'second']
    ).rename(columns={'time_id': 'dropoff_time_id'})

    # Converte para objeto Python (um datetime.date)
    time_dim['date'] = pd.to_datetime(time_dim['date'], format='%Y-%m-%d')

    # Merge para pickup_location_id e dropoff_location_id
    print("\tPopulando indexs para pickup_location_id e \
dropoff_location_id ...")
    df_process_tripdata = df_process_tripdata.merge(
        location_dim[['location_id']],
        how='left',
        left_on='pu_location_id',
        right_on='location_id'
    ).rename(columns={'location_id': 'pickup_location_id'})

    df_process_tripdata = df_process_tripdata.merge(
        location_dim[['location_id']],
        how='left',
        left_on='do_location_id',
        right_on='location_id'
    ).rename(columns={'location_id': 'dropoff_location_id'})

    # Merge para rate_code_id e payment_type_id
    print("\tPopulando indexs para rate_code_id e payment_type_id ...")
    df_process_tripdata = df_process_tripdata.merge(
        rate_code_dim[['rate_code_id']],
        how='left',
        left_on='rate_code_id',
        right_on='rate_code_id'
    ).rename(columns={'rate_code_id': 'rate_code_id'})

    df_process_tripdata = df_process_tripdata.merge(
        payment_type_dim[['payment_type_id']],
        how='left',
        left_on='payment_type_id',
        right_on='payment_type_id'
    ).rename(columns={'payment_type_id': 'payment_type_id'})

    # Merge para vendor_id
    print("\tPopulando indexs para vendor_id ...")
    df_process_tripdata = df_process_tripdata.merge(
        vendor_dim[['vendor_id']],
        how='left',
        left_on='vendor_id',
        right_on='vendor_id'
    ).rename(columns={'vendor_id': 'vendor_id'})

    # Calcular duração da viagem
    print("\tCalculando duração da viagem em segundos ...")
    df_process_tripdata['calc_trip_duration_seconds'] = (
        df_process_tripdata['dropoff_datetime'] -
        df_process_tripdata['pickup_datetime']
    ).dt.total_seconds()

    # Adicionar ID incremental (simulando SERIAL)
    df_process_tripdata.reset_index(inplace=True)
    df_process_tripdata.rename(columns={'index': 'trip_id'}, inplace=True)

    # Criar o DataFrame da tabela fato
    trips_fact = df_process_tripdata[[
        'trip_id', 'vendor_id', 'pickup_time_id', 'dropoff_time_id',
        'pickup_location_id', 'dropoff_location_id', 'rate_code_id',
        'passenger_count', 'trip_distance', 'payment_type_id', 'fare_amount',
        'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount',
        'calc_trip_duration_seconds', 'load_datetime'
    ]].copy()

    trips_fact = trips_fact.astype(schema.get('trips_fact'))

    trips_fact.to_parquet(f"{WAREHOUSE_DIR}/trips_fact.parquet", index=False)

    print(
        f"Tabela trips_fact processada em warehouse: \
{WAREHOUSE_DIR}/trips_fact.parquet")

    return trips_fact
