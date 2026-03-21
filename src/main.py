import pandas as pd
import logging
from src.extract import ExtrairDados
from src.transform import TransformadorDados
from src.load import DuckDBLoader
import pandas as pd

# Configuração do Log
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Lista de UFs do seu código 
ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
       'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
       'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']

def run_pipeline():

    logging.info("🚀 Iniciando o Pipeline de Dados...")
    
    try:
        # 1. Extração
        logging.info("📥 Iniciando etapa de Extração...")

        extrator = ExtrairDados('data/raw')
        df_produtos = extrator.load_csv('produtos_raw.csv')
        df_vendas = extrator.load_csv('vendas_2023_2024.csv')
        df_clientes = extrator.load_json('clientes_crm.json')
        df_custos_importacao = extrator.load_json('custos_importacao.json')

        # 2. Transformação
        logging.info("🛠️ Iniciando etapa de Transformação...")
        transformer = TransformadorDados(ufs)

        datas_aux = df_custos_importacao["historic_data"].explode()
        df_datas_temp = pd.json_normalize(datas_aux)
        datas_unicas = pd.to_datetime(df_datas_temp["start_date"], format='%d/%m/%Y').dt.date.unique()
        dolar_historico = transformer.buscar_dolar_historico(datas_unicas)
        
        df_prod_limpo = transformer.limpar_produtos(df_produtos)
        df_vendas_limpo = transformer.limpar_vendas(df_vendas)
        df_clientes_limpo = transformer.limpar_clientes(df_clientes)
        df_importacao_limpo = transformer.limpar_custos_importacoes(df_custos_importacao, mapa_cambio=dolar_historico)

        # 3. Carga (Load)
        logging.info("💾 Iniciando etapa de Carga no DuckDB...")
        loader = DuckDBLoader('data/processed/lhnauticaldb.duckdb')
        loader.load_dataframe(df_prod_limpo, "produtos")
        loader.load_dataframe(df_vendas_limpo, "vendas")
        loader.load_dataframe(df_clientes_limpo, "clientes")
        loader.load_dataframe(df_importacao_limpo, "custo_importacoes")
        loader.close()

        logging.info("✅ Pipeline executado com sucesso!")
        print("Pipeline finalizado! Verifique o log para detalhes.")

    except FileNotFoundError as e:
        logging.error(f"❌ Erro de arquivo não encontrado: {e}")
        print(f"Erro: Algum arquivo de dados está faltando. Verifique o log.")
        
    except Exception as e:
        logging.error(f"🚨 Ocorreu um erro inesperado: {e}")
        print(f"Ocorreu um erro crítico: {e}")

if __name__ == "__main__":
    run_pipeline()