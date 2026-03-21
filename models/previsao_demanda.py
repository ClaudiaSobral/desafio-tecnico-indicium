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

def treinar_modelo_previsao(df: pd.DataFrame, coluna_alvo: str, colunas_features: list):
    """
    Treina um modelo de Random Forest para prever a demanda futura.
    
    Parâmetros:
    - df: DataFrame contendo o histórico de vendas.
    - coluna_alvo: O nome da coluna que queremos prever (ex: 'quantidade_vendida').
    - colunas_features: Lista de colunas usadas para fazer a previsão (ex: ['mes', 'preco']).
    """
    # 1. Separação entre o que usamos para prever (X) e o que queremos prever (y)
    X = df[colunas_features]
    y = df[coluna_alvo]
    
    # 2. Divisão dos dados: 80% para o modelo aprender, 20% para testarmos se ele aprendeu bem
    X_treino, X_teste, y_treino, y_teste = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 3. Criação e treinamento do modelo
    modelo = RandomForestRegressor(n_estimators=100, random_state=42)
    modelo.fit(X_treino, y_treino)
    
    # 4. Teste do modelo com os dados separados
    previsoes = modelo.predict(X_teste)
    
    # 5. Avaliação de erro: Em média, quanto o modelo erra para mais ou para menos?
    erro_medio = mean_absolute_error(y_teste, previsoes)
    print(f"Erro Médio Absoluto (MAE): O modelo erra a previsão em cerca de {erro_medio:.2f} unidades.")
    
    return modelo

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