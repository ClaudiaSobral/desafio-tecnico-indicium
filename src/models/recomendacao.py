import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

def criar_matriz_recomendacao(df_compras: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma o histórico de compras em uma matriz matemática onde:
    - Linhas = Clientes
    - Colunas = Produtos
    - Valores = Quantidade comprada (ou apenas 1 para "comprou" e 0 para "não comprou")
    """
    # Cria uma tabela cruzada (pivot table)
    matriz_usuario_item = df_compras.pivot_table(
        index='id_cliente', 
        columns='id_produto', 
        values='qtd', 
        fill_value=0 # Se não comprou, preenchemos com 0
    )
    return matriz_usuario_item

def recomendar_produtos_similares(produto_alvo: str, matriz_usuario_item: pd.DataFrame, top_n: int = 3) -> pd.Series:
    """
    Calcula quais produtos têm o padrão de compra mais parecido com o produto_alvo.
    """
    # 1. Transpõe a matriz para que os Produtos fiquem nas linhas
    matriz_item_usuario = matriz_usuario_item.T
    
    # 2. Calcula a "distância" (similaridade) entre todos os produtos
    similaridade = cosine_similarity(matriz_item_usuario)
    
    # 3. Transforma o resultado em um DataFrame legível
    df_similaridade = pd.DataFrame(
        similaridade, 
        index=matriz_item_usuario.index, 
        columns=matriz_item_usuario.index
    )
    
    # 4. Pega as pontuações do produto escolhido, remove ele mesmo da lista e ordena os maiores
    pontuacoes = df_similaridade[produto_alvo].drop(produto_alvo)
    recomendacoes = pontuacoes.sort_values(ascending=False).head(top_n)
    
    return recomendacoes

# ==========================================
# Exemplo de Uso (Simulando dados da LH Nauticals)
# ==========================================
if __name__ == "__main__":
    # Histórico de compras fictício
    compras_mock = pd.DataFrame({
        'id_cliente': [1, 1, 1, 2, 2, 3, 3, 4],
        'nome_produto': [
            'Colete Salva-Vidas', 'Bússola', 'Sinalizador', 
            'Colete Salva-Vidas', 'Sinalizador', 
            'Bússola', 'Corda Náutica', 
            'Colete Salva-Vidas'
        ],
        'quantidade': [1, 1, 2, 1, 1, 1, 5, 2]
    })

    # Aplicando as funções
    matriz = criar_matriz_recomendacao(compras_mock)
    
    produto_consulta = 'Colete Salva-Vidas'
    produtos_recomendados = recomendar_produtos_similares(produto_consulta, matriz)
    
    print(f"Porque o cliente comprou '{produto_consulta}', recomendamos:")
    print(produtos_recomendados)