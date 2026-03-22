import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

def preparar_dados_temporais(df: pd.DataFrame, coluna_data: str) -> pd.DataFrame:
    """
    Extrai informações úteis de uma coluna de datas para ajudar o modelo a entender sazonalidades.
    Modelos de ML não entendem datas puras, mas entendem números (mês 1, mês 2, etc).
    """
    df = df.copy()
    df[coluna_data] = pd.to_datetime(df[coluna_data])
    
    # Criando novas colunas (Feature Engineering) baseadas na data
    df['mes'] = df[coluna_data].dt.month
    df['ano'] = df[coluna_data].dt.year
    df['dia_da_semana'] = df[coluna_data].dt.dayofweek
    
    return df

def treinar_modelo_previsao(df: pd.DataFrame, coluna_alvo: str, colunas_features: list) -> pd.DataFrame:
    """
    Treina o modelo e retorna um DataFrame com as previsões.
    """
    X = df[colunas_features]
    y = df[coluna_alvo]
    
    # Treinamento do modelo
    from sklearn.ensemble import RandomForestRegressor
    modelo = RandomForestRegressor(n_estimators=100, random_state=42)
    modelo.fit(X, y)
    
    # CORREÇÃO: Em vez de retornar o modelo, vamos gerar as previsões
    # e criar um DataFrame (tabela) para o DuckDB conseguir salvar
    
    df_resultados = df.copy() # Copiamos os dados originais
    
    # Criamos uma nova coluna com a previsão que a IA fez para cada linha
    df_resultados['previsao_qtd'] = modelo.predict(X)
    
    # Opcional: arredondar as previsões, já que não vendemos "meio" produto
    df_resultados['previsao_qtd'] = df_resultados['previsao_qtd'].round(0).astype(int)
    
    # Retornamos apenas as colunas que importam para o banco de dados
    colunas_finais = ['id_vendas', 'id_produto', 'data_venda', 'qtd', 'previsao_qtd']
    
    # Filtra para retornar apenas as colunas se elas existirem no df
    colunas_existentes = [col for col in colunas_finais if col in df_resultados.columns]
    
    return df_resultados[colunas_existentes]

# ==========================================
# Exemplo de Uso (Simulando dados da LH Nauticals)
# ==========================================
if __name__ == "__main__":
    # Criando dados fictícios para demonstração
    dados_mock = pd.DataFrame({
        'data_venda': pd.date_range(start='2023-01-01', periods=100, freq='W'),
        'preco_produto': np.random.uniform(100, 500, 100),
        'quantidade_vendida': np.random.randint(10, 100, 100) # O que queremos prever
    })

    # Aplicando as funções
    df_preparado = preparar_dados_temporais(dados_mock, 'data_venda')
    features = ['mes', 'ano', 'dia_da_semana', 'preco_produto']
    
    modelo_treinado = treinar_modelo_previsao(df_preparado, 'quantidade_vendida', features)