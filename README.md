# =========================================================
# 1. SETUP INICIAL E CRIAÇÃO DA ESTRUTURA DE DIRETÓRIOS
# =========================================================
echo "🛠️ Criando estrutura de diretórios..."
mkdir -p ~/environment/data-engineering-pyspark
cd ~/environment/data-engineering-pyspark
mkdir -p data/input data/output
mkdir -p src/business_logic src/io src/orchestration src/spark_manager src/config
mkdir -p tests
echo "Estrutura criada em $(pwd)"

# =========================================================
# 2. CRIAÇÃO DO SCRIPT DE DOWNLOAD
# =========================================================
echo "⬇️ Criando data/download_data.sh..."

cat > data/download_data.sh << 'EOF_DOWNLOAD'
#!/usr/bin/env bash
set -e

ROOT="$PWD"
INPUT_DIR="$ROOT/data/input"

echo "🧽 Limpando diretórios..."
rm -rf "$ROOT/data/tmp-pagamentos" "$ROOT/data/tmp-pedidos"
mkdir -p "$INPUT_DIR"
rm -rf "$INPUT_DIR"/*

echo ""
echo "⬇ Baixando TODOS os arquivos de PAGAMENTOS (via API GitHub)..."

curl -s https://api.github.com/repos/infobarbosa/dataset-json-pagamentos/contents/data/pagamentos \
| grep "download_url" \
| cut -d '"' -f 4 \
| while read url; do
      echo "Baixando: $(basename $url)"
      curl -L "$url" -o "$INPUT_DIR/$(basename $url)"
  done

echo ""
echo "⬇ Baixando TODOS os arquivos de PEDIDOS (pasta data/pedidos)..."

curl -s https://api.github.com/repos/infobarbosa/datasets-csv-pedidos/contents/data/pedidos \
| grep "download_url" \
| cut -d '"' -f 4 \
| while read url; do
      echo "Baixando: $(basename $url)"
      curl -L "$url" -o "$INPUT_DIR/$(basename $url)"
  done

echo ""
echo "📂 Arquivos baixados:"
ls -lh "$INPUT_DIR"

echo ""
echo "✅ Processo concluído com sucesso!"
EOF_DOWNLOAD

chmod +x data/download_data.sh
echo "Script de download criado e permissão de execução concedida."

# =========================================================
# 3. CRIAÇÃO DOS ARQUIVOS PYTHON (SRC)
# =========================================================

# src/config/spark_config.py
echo "📝 Criando src/config/spark_config.py..."
cat > src/config/spark_config.py << 'EOF_CONFIG'
from dataclasses import dataclass

@dataclass
class AppConfig:
    pagamentos_path: str = "data/input/pagamentos*.json.gz"
    pedidos_path: str = "data/input/pedidos*.csv.gz"
    output_path: str = "data/output/relatorio_parquet"
    ano_relatorio: int = 2025
    app_name: str = "FIAP_Trabalho_Final"
    master: str = "local[*]"
EOF_CONFIG

# src/business_logic/sales_report_logic.py
echo "📝 Criando src/business_logic/sales_report_logic.py..."
cat > src/business_logic/sales_report_logic.py << 'EOF_LOGIC'
import logging
from pyspark.sql.functions import col, year

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class SalesReportLogic:
    def __init__(self, io_manager, config):
        self.io = io_manager
        self.config = config

    def build_report(self):
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
EOF_LOGIC

# src/io/data_io.py
echo "📝 Criando src/io/data_io.py..."
cat > src/io/data_io.py << 'EOF_IO'
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, LongType

class DataIO:
    def __init__(self, spark):
        self.spark = spark

    def read_pagamentos(self,
