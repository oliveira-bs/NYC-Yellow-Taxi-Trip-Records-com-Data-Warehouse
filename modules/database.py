import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

import config.database_config as db_config


def start_connection(db_name):
    """Estabelece conexão inicial ao banco padrão para criar o banco especifico
do projeto desejado."""
    conn = psycopg2.connect(dbname='postgres', user=db_config.DB_USER,
                            password=db_config.DB_PASSWORD,
                            host=db_config.DB_HOST, port=db_config.DB_PORT)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        # Remove o banco existente, se houver
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
        print(f"Banco de dados '{db_name}' removido com sucesso.")

        # Cria o banco de dados
        cursor.execute(f"CREATE DATABASE {db_name};")
        print(f"Banco de dados '{db_name}' criado com sucesso.")

    finally:
        cursor.close()
        conn.close()


def get_connection():
    """
    Estabelece a conexão com o banco de dados PostgreSQL.
    Retorna:
        connection: Objeto de conexão com o banco de dados.
    """

    try:
        connection = psycopg2.connect(
            host=db_config.DB_HOST,
            database=db_config.DB_NAME,
            user=db_config.DB_USER,
            password=db_config.DB_PASSWORD,
            port=db_config.DB_PORT
        )
        return connection
    except Exception as e:
        print(f"\n\nErro ao conectar ao banco de dados: {e}")
        raise


def create_tables(schema_file="sql/schema.sql"):
    """
    Lê e executa o arquivo de esquema SQL para criar tabelas no banco de dados.

    Args:
        schema_file (str): Caminho para o arquivo de esquema SQL.
    """
    connection = get_connection()
    try:
        cursor = connection.cursor()

        # Ler o arquivo schema.sql
        with open(schema_file, "r") as f:
            schema_sql = f.read()

        # Executar os comandos SQL
        cursor.execute(schema_sql)
        connection.commit()
        print("\nSchema das tabelas criados com sucesso.")
    except Exception as e:
        connection.rollback()
        print(f"\nErro ao criar tabelas: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


def insert_data(df, table_name, connection):
    """
    Insere os dados de um DataFrame em uma tabela do banco de dados.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
        table_name (str): Nome da tabela no banco de dados.
        connection: Objeto de conexão com o banco de dados.
    """

    # Obter as colunas e os valores
    columns = ",".join(df.columns)
    values = [tuple(row[1:]) for row in df.itertuples()]

    # values = [tuple(row) for row in df.values]
    insert_query = f"""INSERT INTO {
db_config.DB_SCHEMA}.{table_name}({columns}) VALUES %s"""

    with connection.cursor() as cursor:
        execute_values(cursor, insert_query, values)
    connection.commit()

    print(f"Dados inseridos na tabela {table_name}")


def persist_data(parquet_files):
    """
    Persiste múltiplos DataFrames em suas respectivas tabelas no banco de \
dados.

    Args:
        parquet_files (dict): Dicionário contendo os caminhos para os arquivos\
 parquet em finaliados em warehouse e seus nomes de tabela correspondentes.
    """

    # Conexão com o banco de dados
    connection = get_connection()

    # Iterar sobre cada tabela e popular com dados do Parquet
    try:
        for table_name, parquet_path in parquet_files.items():
            print(f"\nLendo dados de {parquet_path} para \
popular {table_name}...")
            df = pd.read_parquet(parquet_path)
            insert_data(df, table_name, connection)

    except psycopg2.Error as e:
        print("\n\nErro ao conectar ou inserir dados no PostgreSQL:", e)

    finally:
        if connection:
            connection.close()
