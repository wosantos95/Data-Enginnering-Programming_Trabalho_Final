from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, LongType
)

class DataIO:
    def __init__(self, spark):
        self.spark = spark

    def read_pagamentos(self, path):
        schema = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("forma_pagamento", StringType(), True),
            StructField("valor_pagamento", DoubleType(), True),
            StructField("status", BooleanType(), True),
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score", DoubleType(), True)
            ])),
            StructField("data_processamento", TimestampType(), True)
        ])
        return self.spark.read.schema(schema).json(path)

    def read_pedidos(self, path):
        schema = StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", DoubleType(), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True),
        ])
        return self.spark.read.option("header", "true").option("sep", ";").schema(schema).csv(path)

    def write_parquet(self, df, path):
        df.write.mode("overwrite").parquet(path)
