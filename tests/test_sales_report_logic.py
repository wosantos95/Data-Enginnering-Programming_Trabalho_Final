# tests/test_sales_report_logic.py

import pytest
import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, TimestampType
import logging
import sys
import os

# Adiciona o pacote raiz para que o teste encontre src/ e config/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importações do projeto
from src.business_logic.sales_report_logic import SalesReportLogic
from src.io.data_io import DataIO
from src.config.spark_config import AppConfig

# Evita logs do PySpark poluindo a saída durante os testes
logging.basicConfig(level=logging.ERROR)

# -------------------------------
# Fixture para SparkSession
# -------------------------------
@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder
        .appName("UnitTestSparkSession")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()

# -------------------------------
# Fixture para SalesReportLogic
# -------------------------------
@pytest.fixture
def sales_report_logic(spark_session):
    io_manager = DataIO(spark_session)
    config = AppConfig()
    return SalesReportLogic(io_manager, config)

# -------------------------------
# Dados de teste
# -------------------------------

# Pedidos (orders)
ORDERS_DATA = [
    ("P001", datetime.datetime(2025,1,15,10,0,0), "SP", 500.0, 1),
    ("P002", datetime.datetime(2024,1,1,10,0,0), "RJ", 200.0, 1),
    ("P003", datetime.datetime(2025,2,20,10,0,0), "MG", 100.0, 2),
    ("P004", datetime.datetime(2025,5,10,10,0,0), "SP", 300.0, 1),
]

ORDERS_SCHEMA = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("data_criacao", TimestampType(), True),
    StructField("uf", StringType(), True),
    StructField("valor_unitario", DoubleType(), True),
    StructField("quantidade", LongType(), True)
])

# Pagamentos (payments) com StructType aninhado
PAYMENTS_DATA = [
    Row(id_pedido="P001", forma_pagamento="Pix", valor_pagamento=500.0, status=False,
        avaliacao_fraude=Row(fraude=False, score=0.1)),
    Row(id_pedido="P002", forma_pagamento="Boleto", valor_pagamento=200.0, status=True,
        avaliacao_fraude=Row(fraude=False, score=0.2)),
    Row(id_pedido="P003", forma_pagamento="Boleto", valor_pagamento=200.0, status=False,
        avaliacao_fraude=Row(fraude=True, score=0.9)),
    Row(id_pedido="P004", forma_pagamento="Cartao", valor_pagamento=300.0, status=False,
        avaliacao_fraude=Row(fraude=False, score=0.05)),
]

PAYMENTS_SCHEMA = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", DoubleType(), True),
    StructField("status", BooleanType(), True),
    StructField("avaliacao_fraude", StructType([
        StructField("fraude", BooleanType(), True),
        StructField("score", DoubleType(), True)
    ]))
])

# -------------------------------
# Teste principal
# -------------------------------
def test_build_report_from_dataframes(spark_session, sales_report_logic):
    # Cria DataFrames de pedidos e pagamentos
    pedidos_df = spark_session.createDataFrame(ORDERS_DATA, ORDERS_SCHEMA)
    pagamentos_df = spark_session.createDataFrame(PAYMENTS_DATA, PAYMENTS_SCHEMA)

    # Executa método interno que processa os DataFrames
    result_df = sales_report_logic._process_dataframes(pedidos_df, pagamentos_df)

    # Verifica as colunas finais
    expected_columns = ["id_pedido", "estado_uf", "forma_pagamento", "valor_total_pedido", "data_pedido"]
    assert result_df.columns == expected_columns, f"Colunas diferentes: {result_df.columns}"

    # Verifica o número de linhas filtradas corretamente
    # Apenas P001 e P004 devem passar:
    # P002 -> ano errado
    # P003 -> fraude=True
    result_count = result_df.count()
    assert result_count == 2, f"Esperava 2 linhas, encontrou {result_count}"

    # Verifica valores finais da primeira linha (ordenada por estado, forma, data)
    first_row = result_df.collect()[0]
    assert first_row["id_pedido"] == "P004" or first_row["id_pedido"] == "P001"