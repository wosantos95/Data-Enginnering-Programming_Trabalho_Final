# FIAP - Data Engineering Programming – Trabalho Final

## Descrição do Projeto
Este projeto em **PySpark** gera um relatório de pedidos de venda filtrando apenas:
- Pagamentos recusados (`status=false`)
- Pagamentos legítimos (`fraude=false`)
- Pedidos do ano de 2025

O relatório inclui:
1. Identificador do pedido (`id_pedido`)
2. Estado (`uf`)
3. Forma de pagamento
4. Valor total do pedido
5. Data do pedido  

O relatório é salvo em **formato Parquet**, ordenado por UF, forma de pagamento e data do pedido.

---

## Estrutura do Projeto

