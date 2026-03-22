
"""
Centraliza todas as queries SQL da LH Nauticals
"""


EXTRAIR_CLIENTES = """
    -- Extrai lucro líquido e tíquete médio por cliente
    
    SELECT 
        c.nome_cliente,
        c.estado,
        ROUND(SUM(v.total), 2) AS total_compras,
        ROUND(SUM(i.brl_price * v.qtd), 2) AS custo_total_cliente,
        ROUND((total_compras - custo_total_cliente),2) AS lucro_liq_cliente,
        ROUND(((total_compras - custo_total_cliente) / v.qtd),2) as tiquete_medio_cliente
    FROM clientes AS c
    JOIN vendas AS v ON c.id_cliente = v.id_cliente
    JOIN custo_importacoes AS i ON v.id_produto = i.id_produto
    GROUP BY
        c.nome_cliente,
        c.estado,
        v.qtd
    ORDER BY
        lucro_liq_cliente DESC
"""

EXTRAIR_FATURAMENTO_POR_CATEGORIA = """
    -- Exibe as categorias que mais faturaram, do maior faturamento para o menor
    
    SELECT
        i.categoria,
        ROUND(
            SUM(v.total) - SUM(i.brl_price * v.qtd), 2
        ) AS lucro_total
    FROM vendas AS v
    JOIN custo_importacoes AS i
        ON v.id_produto = i.id_produto
    JOIN produtos AS p
        ON p.id_produto = i.id_produto
    GROUP BY i.categoria
    ORDER BY lucro_total DESC
    """

EXTRAIR_FATURAMENTO_POR_SUBCATEGORIA = """
    -- Exibe o faturamento por subcategoria, do maior faturamento ao menor

    SELECT
        p.subcategoria_produto,
        ROUND(
            SUM(v.total) 
            - SUM(i.brl_price * v.qtd),
            2
        ) AS lucro_total
    FROM vendas AS v
    JOIN custo_importacoes AS i
        ON v.id_produto = i.id_produto
    JOIN produtos AS p
        ON p.id_produto = i.id_produto
    GROUP BY p.subcategoria_produto
    ORDER BY lucro_total DESC
    """

EXTRAIR_PRODUTOS_MAIOR_PREJUIZO = """
    -- Exibe os 5 produtos que mais causaram prejuízo

    SELECT
        i.produto,
        i.categoria,
        ROUND(SUM(v.total) - (i.brl_price * SUM(v.qtd)), 2) as resultado_financeiro,
        ROUND(((SUM(v.total) - SUM(i.brl_price * v.qtd)) / SUM(v.total)) * 100, 2) AS margem_percentual
    FROM 
        vendas as v
    JOIN
        custo_importacoes as i
    ON v.id_produto = i.id_produto
    GROUP BY i.produto, i.brl_price, i.categoria, data_venda
    ORDER BY resultado_financeiro asc
    limit 5
    """

EXTRAIR_VENDAS_POR_MES = """
    -- Exibe a sazonalidade das vendas distribuídas por mês.
    -- Colunas: numero_mes, mes_do_ano, total_pedidos, faturamento_historico, ticket_medio_sazonal

    SELECT 
    EXTRACT(MONTH FROM data_venda) AS numero_mes,
    CASE EXTRACT(MONTH FROM data_venda)
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS mes_do_ano,
    COUNT(DISTINCT id_vendas) AS total_pedidos,
    ROUND(SUM(total), 2) AS faturamento_historico,
    ROUND(SUM(total) / COUNT(DISTINCT id_vendas), 2) AS ticket_medio_sazonal
FROM vendas
GROUP BY 
    EXTRACT(MONTH FROM data_venda),
    mes_do_ano
ORDER BY 
    numero_mes
"""

EXTRAIR_TIQUETE_MEDIO_ESTADO = """
    -- Extrai o tíquete médio das vendas por estado, do maior para o menor.
    -- Colunas: estado, faturamento_total, custo_total_estado, lucro_liq_estado, tiquete_medio_estado

    SELECT 
        c.estado,
        ROUND(SUM(v.total), 2) AS faturamento_total,
        ROUND(SUM(i.brl_price * v.qtd), 2) AS custo_total_estado,
        ROUND((faturamento_total - custo_total_estado),2) AS lucro_liq_estado,
        ROUND(SUM(v.total) / COUNT(DISTINCT v.id_vendas), 2) as tiquete_medio_estado
    FROM clientes AS c
    JOIN vendas AS v ON c.id_cliente = v.id_cliente
    JOIN custo_importacoes AS i ON v.id_produto = i.id_produto
    GROUP BY
        c.estado
    ORDER BY
        tiquete_medio_estado DESC
        """

EXTRAIR_PRODUTOS_MAIOR_LUCRO = """
    -- Exibe os 5 produtos que mais trouxeram lucro, do maior para o menor

    SELECT
        i.produto,
        i.categoria,
        ROUND(SUM(v.total) - (i.brl_price * SUM(v.qtd)), 2) as resultado_financeiro,
        ROUND(((SUM(v.total) - SUM(i.brl_price * v.qtd)) / SUM(v.total)) * 100, 2) AS margem_percentual
    FROM 
        vendas as v
    JOIN
        custo_importacoes as i
    ON v.id_produto = i.id_produto
    GROUP BY i.produto, i.brl_price, i.categoria, data_venda
    ORDER BY resultado_financeiro DESC
    limit 5
"""

EXTRAIR_MEDIA_FATURAMENTO_DIA_SEMANA = """
    -- Exibe a média de faturamento por dia da semana, incluindo dias sem vendas.

WITH calendario AS (
    SELECT data
    FROM generate_series(
        (SELECT MIN(CAST(data_venda AS DATE)) FROM vendas),
        (SELECT MAX(CAST(data_venda AS DATE)) FROM vendas),
        INTERVAL '1 day'
    ) AS t(data)
),
vendas_diarias AS (
    SELECT 
        CAST(data_venda AS DATE) AS data_exata,
        SUM(total) AS faturamento_total_dia,
        SUM(qtd) AS total_itens_dia,
        COUNT(id_vendas) AS total_pedidos_dia 
    FROM vendas
    GROUP BY CAST(data_venda AS DATE)
)
SELECT 
    CASE EXTRACT(DOW FROM c.data)
        WHEN 0 THEN 'Domingo'
        WHEN 1 THEN 'Segunda'
        WHEN 2 THEN 'Terça'
        WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta'
        WHEN 5 THEN 'Sexta'
        WHEN 6 THEN 'Sábado'
    END AS dia_semana,
    ROUND(AVG(COALESCE(v.faturamento_total_dia, 0)), 2) AS media_faturamento_dia,   
    ROUND(AVG(COALESCE(v.total_itens_dia, 0)), 2) AS media_itens_vendidos_dia,
    ROUND(AVG(COALESCE(v.total_pedidos_dia, 0)), 2) AS media_pedidos_dia
FROM calendario AS c
LEFT JOIN vendas_diarias AS v ON c.data = v.data_exata
GROUP BY 
    EXTRACT(DOW FROM c.data),
    CASE EXTRACT(DOW FROM c.data)
        WHEN 0 THEN 'Domingo'
        WHEN 1 THEN 'Segunda'
        WHEN 2 THEN 'Terça'
        WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta'
        WHEN 5 THEN 'Sexta'
        WHEN 6 THEN 'Sábado'
    END
ORDER BY 
    media_faturamento_dia DESC
"""