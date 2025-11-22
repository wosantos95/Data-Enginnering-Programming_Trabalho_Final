import logging
from pyspark.sql.functions import col, year

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SalesReportLogic:
    def __init__(self, io_manager, config):
        self.io = io_manager
        self.config = config

    def build_report(self):
        """Método principal que lê os dados do IO e retorna o relatório"""
        try:
            logger.info("Lendo pagamentos...")
            pagamentos = self.io.read_pagamentos(self.config.pagamentos_path)

            logger.info("Lendo pedidos...")
            pedidos = self.io.read_pedidos(self.config.pedidos_path)

            return self._process_dataframes(pedidos, pagamentos)

        except Exception as e:
            logger.exception("Erro ao processar relatório")
            raise

    def _process_dataframes(self, pedidos_df, pagamentos_df):
        """Processa DataFrames recebidos e retorna o relatório"""
        pagamentos_filtrados = pagamentos_df.filter(
            (col("status") == False) &
            (col("avaliacao_fraude.fraude") == False)
        )

        pedidos_df = pedidos_df.withColumn(
            "valor_total_pedido",
            col("valor_unitario") * col("quantidade")
        )

        joined = pedidos_df.join(pagamentos_filtrados, "id_pedido", "inner")
        joined = joined.filter(year(col("data_criacao")) == self.config.ano_relatorio)

        result = joined.select(
            col("id_pedido"),
            col("uf").alias("estado_uf"),
            col("forma_pagamento"),
            col("valor_total_pedido"),
            col("data_criacao").alias("data_pedido")
        ).orderBy("estado_uf", "forma_pagamento", "data_pedido")

        return result
