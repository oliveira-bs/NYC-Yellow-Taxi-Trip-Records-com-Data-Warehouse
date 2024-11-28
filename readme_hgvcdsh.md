# NYC Yellow Taxi Trip Records

## Objetivo do Projeto

Ingestão de Dados: Carregar dados de 3 fontes:
- Yellow Taxi Trip Records.
- Taxi Zone Lookup Table.
- Taxi Zone Shapefile.

Processamento e Persistência: Organizar e armazenar os dados processados em um banco de dados externo (PostgreSQL).

Geração de Insights: Executar consultas SQL para análise e geração de KPIs.

## Estrutura do Projeto
    
```
NYC_Yellow_Taxi_Trip_Records/
├── data/
│   ├── raw/                # Dados brutos baixados
│   ├── staging/            # Dados processados
│   └── warehouse/          # Dados finais organizados para consulta
├── modules/               # Módulos para ingestão, processamento e persistência
│   ├── ingest.py           # Funções para ingestão de dados
│   ├── process.py          # Funções para processar dados
│   ├── transform.py        # Funções para transformação e carga no Data Warehouse
│   └── database.py         # Funções para conexão e carga no banco
├── notebooks/             # Notebooks para análises exploratórias
├── sql/                   # Consultas SQL
│   └── schema.sql          # Esquema do banco de dados          
├── main.py                # Ponto de entrada do projeto
├── readme.md              # Documentação do projeto
└── requirements.txt       # Dependências do projeto
```

## Estruturar o Modelo Relacional

Aqui está uma possível estrutura de tabelas com suas chaves primárias (PK) e estrangeiras (FK):

a) TABELA: Trips

Descrição: Registra informações principais de cada viagem.

Colunas:

    trip_id (PK) - Identificador único da viagem.
    VendorID (FK) - Relaciona-se à tabela de fornecedores.
    RatecodeID (FK) - Tipo de tarifa.
    PULocationID (FK) - Local de partida (chave para a tabela de localizações).
    DOLocationID (FK) - Local de destino (chave para a tabela de localizações).
    pickup_datetime - Data/hora da coleta.
    dropoff_datetime - Data/hora da entrega.
    trip_distance - Distância percorrida.
    passenger_count - Número de passageiros.
    store_and_fwd_flag - Indicador de armazenamento e envio.

b) TABELA: Fares

    Descrição: Registra detalhes financeiros de cada viagem.
    Colunas:
        trip_id (PK, FK) - Identificador da viagem (relaciona-se à tabela Trips).
        fare_amount - Valor da tarifa.
        extra - Valores extras (ex.: taxa de pico).
        mta_tax - Taxa do MTA (Metropolitan Transportation Authority).
        tip_amount - Gorjeta.
        tolls_amount - Pedágios.
        total_amount - Valor total da corrida.
        payment_type - Tipo de pagamento (ex.: cartão, dinheiro).

c) TABELA: Vendors

    Descrição: Lista fornecedores de serviços de transporte.
    Colunas:
        VendorID (PK) - Identificador do fornecedor.
        vendor_name - Nome do fornecedor.

d) TABELA: Locations

    Descrição: Registra os locais de coleta e entrega com base em IDs.
    Colunas:
        LocationID (PK) - Identificador único do local.
        borough - Bairro da localização.
        zone - Nome da zona.
        service_zone - Zona de serviço.

e) TABELA: RateCodes

    Descrição: Define os tipos de tarifa.
    Colunas:
        RatecodeID (PK) - Identificador da tarifa.
        rate_description - Descrição da tarifa (ex.: tarifa padrão, tarifa de aeroporto).

f) TABELA: PaymentTypes

    Descrição: Define os tipos de pagamento.
    Colunas:
        payment_type (PK) - Identificador do tipo de pagamento.
        payment_description - Descrição do tipo (ex.: cartão de crédito, dinheiro).

## Relacionamentos entre as Tabelas

Trips:
    Relaciona-se com Vendors, Locations, e RateCodes.
    Usa trip_id como chave primária e referência para a tabela Fares.

Fares:
    Relaciona-se com Trips através de trip_id.

Locations:
    É referenciada por PULocationID e DOLocationID na tabela Trips.



## Execução

Para instalar os requisitos:

```
pip install -r requirements.txt
```

Rodar projeto:

```
python main.py
```