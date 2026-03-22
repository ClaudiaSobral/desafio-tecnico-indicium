17/03 - Overview do desafio técnico
- Ler e compreender os dados ✅
- Abrir json (esqueci como fazer) ✅
- Ver shape (não é big data)

- Pensar em como fazer a modelagem: SQL funcionaria pro cliente?
- Limpar dados
    kkkkk eletrunicos
    - Produtos - price é string (object)
- Faz sentido limpar em pandas?

- Produtos ✅
    - name, # Vale a pena criar uma categoria de produtos que reúna por subcategoria? Possível que sim
    - price, ✅
    - code, ✅ # Não precisou limpar
    - actual_category ✅

    - Alterou-se nomes de colunas por questão de legibilidade

- Vendas ✅
    - id, ✅
    - id_client, ✅
    - id_product, ✅
    - qtd, ✅
    - total, ✅
    - sale_date ✅ ❗ ---> Gostaria de mudar o formato datetime para %d %m %Y
- Clientes
    - full_name, ✅
    - location, 
    - code, ✅
    - email ✅
- Custos importação
    - product_id, ✅
    - product_name, ✅
    - category, ✅
    - historic_data ❗ ---> Gostaria de mudar o formato datetime para %d %m %Y

18/03

-  Entendendo sobre o DuckDB (ele proporciona a integração com datalakes)
- Entendendo __init__.py
- Transformando ETL em scripts
- Ponto de aprendizado: return como early return, funciona como um else
- Terminei script
- Implementei try/except no main.py
- Implementei docstrings
- Ponto de interesse: sugerir Databricks e projeto de Data Quality na alimentação do banco de dados

19/03
- Padronizar aspas duplas e simples
- Não tem clientes de Florianópolis na tabela de clientes. Suponho que seja apenas do e-commerce
- Há só 2 anos no dataset. Não há justificativa para granularizar as vendas por ano
- Um ajuste para o futuro é cruzar start_date da compra com a cotação do dia x pro produto
- Não há quantidade em estoque? Apenas quantidade de vendas. O valor pago é do lote?
- Não há produto que dê prejuízo, o que é estranho

20/03
- Percebi que a cotação do dólar não é feita de acordo com a cotação histórica da data. Isso pode causar discrepâncias de
valor na análise de vendas. Implementei o dólar atualizado de acordo com a cotação, mas isso gera uma demora bem grande no
script
- O valor em dólar é a unidade!!!!! Agora TUDO faz sentido.
- Hoje foi um dia confuso
- Ponto de interesse: Todos os 10 produtos mais lucrativos são motores e os 10 que mais dão prejuízo são cabos

21/03
- Agora eu preciso documentar o projeto
- Preciso fazer alguns ajustes nos gráficos (top 5 em que constam apenas 3)
- Amanhã vou fazer o
    - dicionário de dados ✅,
    - rever a implementação de ML ✅,
    - criar um mapa mental do fluxo de dados, ✅
    - fazer o PDF ✅
    - apresentar
    - Revisar os comentários do código ✅

1. EDA ✅ (17/03)
2. Tratamento e limpeza ✅ (17/03)
3. Levar para SQL como script (⏳18/03) ✅
4. Análise de vendas (⏳20/03) ✅
5. Análise de clientes (⏳20/03)✅
7. Gráficos das análises (21/03)
6. Previsão de demandas (⏳21/03)
7. Sistema de recomendações (⏳21/03)
9. Apresentação (⏳22/03)   