# Projeto PySpark -- Pipeline de Processamento de Vendas

Este documento descreve a arquitetura, estrutura de pastas,
dependências, execução e testes do projeto de engenharia de dados
desenvolvido em PySpark.

## 1. Visão Geral

O projeto implementa um pipeline completo de processamento de dados
usando PySpark, incluindo: - Leitura de dados brutos - Transformações e
validações - Geração de relatório agregado - Escrita em Parquet - Testes
unitários com Pytest - Orquestração modularizada

------------------------------------------------------------------------

## 2. Estrutura do Projeto

    data-engineering-pyspark/
    │
    ├── main.py
    ├── requirements.txt
    │
    ├── src/
    │   ├── business_logic/
    │   │   └── sales_report_logic.py
    │   │
    │   ├── config/
    │   │   └── spark_config.py
    │   │
    │   ├── io/
    │   │   └── data_io.py
    │   │
    │   ├── orchestration/
    │   │   └── pipeline_orchestrator.py
    │   │
    │   └── spark_manager/
    │       └── session_manager.py
    │
    └── tests/
        └── test_sales_report_logic.py

------------------------------------------------------------------------

## 3. Dependências

Instale as dependências:

``` bash
pip install -r requirements.txt
```

------------------------------------------------------------------------

## 4. Componentes do Projeto

### 4.1 Spark Session Manager (`session_manager.py`)

Responsável por criar e configurar a SparkSession.

### 4.2 Configurações (`spark_config.py`)

Carrega e centraliza parâmetros usados no Spark.

### 4.3 IO de Dados (`data_io.py`)

Funções para leitura e escrita: - leitura de CSV - escrita em Parquet -
criação de pastas caso necessário

### 4.4 Lógica de Negócio (`sales_report_logic.py`)

Implementa: - limpeza de colunas - agregações - cálculos da receita -
geração do relatório final

### 4.5 Orquestrador (`pipeline_orchestrator.py`)

Controla todo o fluxo: 1. cria sessão Spark\
2. lê dados\
3. processa\
4. salva saída

### 4.6 Aplicação Principal (`main.py`)

Ponto de entrada do pipeline:

``` bash
python main.py
```

------------------------------------------------------------------------

## 5. Execução do Pipeline

### 5.1 Rodar o pipeline completo

``` bash
python main.py
```

A saída será salva automaticamente em:

    ./data/output/report

------------------------------------------------------------------------

## 6. Estrutura dos Dados

### Entrada

Arquivo CSV com colunas como: - id - product - price - quantity -
timestamp

### Saída

Parquet otimizado, com colunas agregadas, como: - total_sales -
total_revenue - avg_price

------------------------------------------------------------------------

## 7. Testes Unitários

O teste principal valida a lógica do relatório:

``` bash
pytest -q
```

Arquivo:

    tests/test_sales_report_logic.py

------------------------------------------------------------------------

## 8. Fluxo Geral do Pipeline

1.  Criar sessão Spark\
2.  Carregar arquivo CSV\
3.  Tratar e validar dados\
4.  Gerar métricas e relatório\
5.  Salvar Parquet\
6.  Encerrar sessão

------------------------------------------------------------------------

## 9. Como Usar este Projeto no Cloud9

Crie o ambiente:

``` bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Rodar:

``` bash
python main.py
```

Rodar testes:

``` bash
pytest
```

------------------------------------------------------------------------

## 10. Autor

Projeto desenvolvido para prática de Engenharia de Dados com PySpark.
