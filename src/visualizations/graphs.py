import os
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

# Importando a query do seu arquivo de queries (ajuste o caminho se necessário)
from src.sql_db.queries import (
    EXTRAIR_VENDAS_POR_MES,
    EXTRAIR_FATURAMENTO_POR_CATEGORIA,
    EXTRAIR_PRODUTOS_MAIOR_PREJUIZO,
    EXTRAIR_PRODUTOS_MAIOR_LUCRO,
    EXTRAIR_MEDIA_FATURAMENTO_DIA_SEMANA,
    EXTRAIR_TIQUETE_MEDIO_ESTADO,
    EXTRAIR_CLIENTES,
    EXTRAIR_FATURAMENTO_POR_SUBCATEGORIA
)

def configurar_estilo():
    """Define o padrão visual das imagens."""
    sns.set_theme(style="whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)
    plt.rcParams['font.size'] = 12

def gerar_grafico_faturamento_categoria(db_path: str, pasta_destino: str):
    """
    Executa a query de faturamento, gera um gráfico de barras e salva como imagem.
    """
    with duckdb.connect(db_path) as conn:
        df = conn.execute(EXTRAIR_FATURAMENTO_POR_CATEGORIA).df()

    configurar_estilo()
    plt.figure()

    # Desenha o gráfico de barras horizontais (ideal para nomes de categorias)
    sns.barplot(
        data=df, 
        x='lucro_total', 
        y='categoria', 
        palette='RdYlGn_r',
        hue='categoria',
        legend=False
    )

    # Adiciona título e limpa os eixos
    plt.title('Lucro total por categoria', fontsize=16, pad=15)
    plt.xlabel('Lucro (R$)')
    plt.ylabel('') 

    # Salva a imagem
    os.makedirs(pasta_destino, exist_ok=True)
    caminho = os.path.join(pasta_destino, 'faturamento_por_categoria.png')
    plt.savefig(caminho, bbox_inches='tight', dpi=300)
    plt.close()

def gerar_grafico_sazonalidade_mensal(db_path: str, pasta_destino: str):
    """Gera um gráfico de linhas mostrando a evolução do faturamento ao longo dos meses."""
    with duckdb.connect(db_path) as conn:
        df = conn.execute(EXTRAIR_VENDAS_POR_MES).df()

    configurar_estilo()
    plt.figure()

    # Usamos marker='o' para colocar uma "bolinha" em cada mês, facilitando a visualização
    sns.lineplot(
        data=df, 
        x='mes_do_ano', 
        y='faturamento_historico', 
        marker='o', 
        color='#2ecc71', # Um verde agradável
        linewidth=2.5
    )

    plt.title('Sazonalidade: Faturamento histórico por mês', fontsize=16, pad=15)
    plt.xlabel('Mês')
    plt.ylabel('Faturamento (R$)')
    plt.xticks(rotation=45) # Inclina os nomes dos meses para não encavalarem
    
    # Salva a imagem
    os.makedirs(pasta_destino, exist_ok=True)
    caminho = os.path.join(pasta_destino, 'sazonalidade_vendas_mes.png')
    plt.savefig(caminho, bbox_inches='tight', dpi=300)
    plt.close()

def gerar_grafico_top5_produtos_lucro_prejuizo(db_path: str, pasta_destino: str):
    """
    Gera dois gráficos de barras separados: um para os produtos que dão mais lucro 
    e outro para os que dão mais prejuízo.
    """
    with duckdb.connect(db_path) as conn:
        df_lucro = conn.execute(EXTRAIR_PRODUTOS_MAIOR_LUCRO).df()
        df_prejuizo = conn.execute(EXTRAIR_PRODUTOS_MAIOR_PREJUIZO).df()

    configurar_estilo()
    os.makedirs(pasta_destino, exist_ok=True)

    # 1. Gráfico de Lucro (Verde)
    plt.figure()
    sns.barplot(data=df_lucro, x='resultado_financeiro', y='produto', palette='Greens_r')
    plt.title('Top 5 Produtos com Maior Lucro', fontsize=16, pad=15)
    plt.xlabel('Lucro (R$)')
    plt.ylabel('')
    
    caminho_lucro = os.path.join(pasta_destino, 'top5_maior_lucro.png')
    plt.savefig(caminho_lucro, bbox_inches='tight', dpi=300)
    plt.close()

    # 2. Gráfico de Prejuízo (Vermelho)
    plt.figure()
    # No prejuízo, os números costumam ser negativos, o barplot lida bem com isso
    sns.barplot(data=df_prejuizo, x='resultado_financeiro', y='produto', palette='Reds_r')
    plt.title('Top 5 Produtos com Maior Prejuízo', fontsize=16, pad=15)
    plt.xlabel('Prejuízo (R$)')
    plt.ylabel('')
    
    caminho_prejuizo = os.path.join(pasta_destino, 'top5_maior_prejuizo.png')
    plt.savefig(caminho_prejuizo, bbox_inches='tight', dpi=300)
    plt.close()

def gerar_grafico_dia_semana(db_path: str, pasta_destino: str):
    """Gráfico de barras mostrando os melhores dias da semana em faturamento."""
    with duckdb.connect(db_path) as conn:
        df = conn.execute(EXTRAIR_MEDIA_FATURAMENTO_DIA_SEMANA).df()

    configurar_estilo()
    plt.figure()

    df['media_faturamento_dia'] = df['media_faturamento_dia'] / 1000000
    sns.barplot(data=df, x='media_faturamento_dia', y='dia_semana', palette='Greens_r')
    plt.title('Média de faturamento por dia da semana', fontsize=16, pad=15)
    plt.xlabel('Faturamento médio (milhões de R$)')
    plt.ylabel('')
    plt.ticklabel_format(style='plain', axis='x')

    caminho = os.path.join(pasta_destino, 'faturamento_dia_semana.png')
    plt.savefig(caminho, bbox_inches='tight', dpi=300)
    plt.close()

def gerar_grafico_tiquete_estado(db_path: str, pasta_destino: str):
    """Gráfico de barras verticais para comparar o tíquete médio entre UFs."""
    with duckdb.connect(db_path) as conn:
        df = conn.execute(EXTRAIR_TIQUETE_MEDIO_ESTADO).df()

    configurar_estilo()
    plt.figure(figsize=(14, 6)) # Deixamos a imagem um pouco mais larga para caber os estados

    sns.barplot(data=df, x='estado', y='tiquete_medio_estado', palette='magma')
    plt.title('Tíquete Médio por Estado', fontsize=16, pad=15)
    plt.xlabel('Estado')
    plt.ylabel('Tíquete Médio (R$)')
    
    caminho = os.path.join(pasta_destino, 'tiquete_medio_estado.png')
    plt.savefig(caminho, bbox_inches='tight', dpi=300)
    plt.close()

def gerar_grafico_top5_clientes_rentaveis(db_path: str, pasta_destino: str):
    """Gera um gráfico com os 5 clientes que trouxeram o maior lucro líquido."""
    with duckdb.connect(db_path) as conn:
        df = conn.execute(EXTRAIR_CLIENTES).df()

    # Como a query traz todos os clientes ordenados, pegamos apenas os 5 primeiros
    df_top5 = df.head(5)

    configurar_estilo()
    plt.figure()

    sns.barplot(data=df_top5, x='lucro_liq_cliente', y='nome_cliente', palette='Blues_r')
    
    plt.title('Top 5 Clientes Mais Rentáveis (Lucro Líquido)', fontsize=16, pad=15)
    plt.xlabel('Lucro Líquido (R$)')
    plt.ylabel('')
    
    # Desliga a notação científica no eixo X (evita o "1e6")
    plt.ticklabel_format(style='plain', axis='x')
    
    caminho = os.path.join(pasta_destino, 'top5_clientes_rentaveis.png')
    plt.savefig(caminho, bbox_inches='tight', dpi=300)
    plt.close()


def gerar_grafico_faturamento_subcategoria(db_path: str, pasta_destino: str):
    """Gera um gráfico mostrando o lucro total por subcategoria de produto."""
    with duckdb.connect(db_path) as conn:
        df = conn.execute(EXTRAIR_FATURAMENTO_POR_SUBCATEGORIA).df()

    configurar_estilo()
    plt.figure(figsize=(12, 8)) # Aumentamos um pouco a altura caso existam muitas subcategorias

    sns.barplot(data=df, x='lucro_total', y='subcategoria_produto', palette='viridis')
    
    plt.title('Faturamento (Lucro) por Subcategoria', fontsize=16, pad=15)
    plt.xlabel('Lucro Total (R$)')
    plt.ylabel('')
    
    # Desliga a notação científica no eixo X
    plt.ticklabel_format(style='plain', axis='x')
    
    caminho = os.path.join(pasta_destino, 'faturamento_por_subcategoria.png')
    plt.savefig(caminho, bbox_inches='tight', dpi=300)
    plt.close()