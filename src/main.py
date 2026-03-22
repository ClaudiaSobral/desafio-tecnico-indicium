import pandas as pd
import logging
import os

# Imports dos seus módulos de ETL
from src.ETL.extract import ExtrairDados
from src.ETL.transform import TransformadorDados
from src.ETL.load import DuckDBLoader

# Imports dos módulos de Machine Learning (IA)
from src.models.previsao_demanda import preparar_dados_temporais, treinar_modelo_previsao
from src.models.recomendacao import criar_matriz_recomendacao

# ==========================================
# Imports do módulo de Visualização
# ==========================================
from src.visualizations.graphs import (
    gerar_grafico_faturamento_categoria,
    gerar_grafico_sazonalidade_mensal,
    gerar_grafico_top5_produtos_lucro_prejuizo,
    gerar_grafico_dia_semana,
    gerar_grafico_tiquete_estado,
    gerar_grafico_top5_clientes_rentaveis,
    gerar_grafico_faturamento_subcategoria      
)

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
    
    # Caminhos padrão do projeto
    db_path = 'data/processed/lhnauticaldb.duckdb'
    viz_path = 'data/visualizations'

    try:
        # ==========================================
        # 1. EXTRAÇÃO
        # ==========================================
        logging.info("📥 Iniciando etapa de Extração...")

        extrator = ExtrairDados('data/raw')
        df_produtos = extrator.load_csv('produtos_raw.csv')
        df_vendas = extrator.load_csv('vendas_2023_2024.csv')
        df_clientes = extrator.load_json('clientes_crm.json')
        df_custos_importacao = extrator.load_json('custos_importacao.json')

       # ==========================================
        # 2. TRANSFORMAÇÃO
        # ==========================================
        logging.info("🛠️ Iniciando etapa de Transformação...")
        transformer = TransformadorDados(ufs)
        
        # Limpezas que são rápidas e sempre devem rodar
        df_prod_limpo = transformer.limpar_produtos(df_produtos)
        df_vendas_limpo = transformer.limpar_vendas(df_vendas)
        df_clientes_limpo = transformer.limpar_clientes(df_clientes)

        # Verifica se o processamento do dólar é necessário
        db_path = 'data/processed/lhnauticaldb.duckdb'
        
        if os.path.exists(db_path):
            logging.info("⚡ Banco detectado! Pulando processamento pesado do dólar e lendo tabela pronta...")
            
            # Como o banco já existe, resgatamos os dados direto dele em vez de recalcular
            import duckdb
            with duckdb.connect(db_path) as conn:
                # Verifica se a tabela já existe dentro do banco para evitar erros
                tabelas = conn.execute("SHOW TABLES").df()
                if 'custo_importacoes' in tabelas['name'].values:
                    df_importacao_limpo = conn.execute("SELECT * FROM custo_importacoes").df()
                else:
                    # Fallback de segurança caso o banco exista, mas a tabela tenha sido apagada
                    df_importacao_limpo = transformer.limpar_custos_importacoes(df_custos_importacao, mapa_cambio={}) 
        else:
            logging.info("🌐 Primeira execução: Buscando cotação histórica do dólar...")
            datas_aux = df_custos_importacao["historic_data"].explode()
            df_datas_temp = pd.json_normalize(datas_aux)
            datas_unicas = pd.to_datetime(df_datas_temp["start_date"], format='%d/%m/%Y').dt.date.unique()
            
            dolar_historico = transformer.buscar_dolar_historico(datas_unicas)
            df_importacao_limpo = transformer.limpar_custos_importacoes(df_custos_importacao, mapa_cambio=dolar_historico)

        # ==========================================
        # 3. MACHINE LEARNING
        # ==========================================
        logging.info("🧠 Iniciando processamento de Machine Learning...")
        
        # A. Previsão de Demandas
        # Preparamos os dados de vendas limpos para o formato que o modelo entende
        df_vendas_temporais = preparar_dados_temporais(df_vendas_limpo, 'data_venda')
        
        # Treinamos/Aplicamos o modelo (assumindo que sua função retorna um DataFrame com previsões)
        # Ajuste 'quantidade' e a lista de features conforme o nome real das suas colunas
        df_previsoes_futuras = treinar_modelo_previsao(
            df_vendas_temporais, 
            coluna_alvo='qtd',
            colunas_features=['mes', 'ano', 'dia_da_semana'] 
        )
        
        # B. Sistema de Recomendações
        # Gera a matriz de cruzamento de produtos baseada no histórico limpo
        df_matriz_recom = criar_matriz_recomendacao(df_vendas_limpo)

        # ==========================================
        # 4. CARGA (Load no DuckDB)
        # ==========================================
        logging.info("💾 Iniciando etapa de Carga no DuckDB...")
        loader = DuckDBLoader('data/processed/lhnauticaldb.duckdb')
        
        # 4.1 Tabelas de Cadastro e Custos: Fazemos Replace (apaga e recria com dados atualizados)
        loader.load_dataframe(df_prod_limpo, "produtos", modo="replace")
        loader.load_dataframe(df_clientes_limpo, "clientes", modo="replace")
        loader.load_dataframe(df_importacao_limpo, "custo_importacoes", modo="replace")
        
        # 4.2 Tabela de Vendas: 
        # ATENÇÃO: Se o seu CSV sempre traz TODO o histórico (2023_2024.csv), use "replace" para não duplicar.
        # Se você passar a ler apenas o CSV do mês atual (ex: vendas_novas.csv), mude para "append".
        loader.load_dataframe(df_vendas_limpo, "vendas", modo="replace") 
        
        # 4.3 Tabelas de Machine Learning: Geralmente fazemos Replace com as previsões mais frescas
        loader.load_dataframe(df_previsoes_futuras, "ia_previsao_demanda", modo="replace")
        loader.load_dataframe(df_matriz_recom, "ia_matriz_recomendacoes", modo="replace")
        
        loader.close()

        logging.info("✅ Pipeline executado com sucesso!")
        print("Pipeline finalizado! Verifique o log para detalhes.")

        # ==========================================
        # 5. VISUALIZAÇÃO
        # ==========================================
        logging.info("📊 Gerando painéis visuais a partir do banco atualizado...")
        
        print("Gerando todas as imagens...")
        gerar_grafico_faturamento_categoria(db_path, viz_path)
        gerar_grafico_sazonalidade_mensal(db_path, viz_path)
        gerar_grafico_top5_produtos_lucro_prejuizo(db_path, viz_path)
        gerar_grafico_dia_semana(db_path, viz_path)
        gerar_grafico_tiquete_estado(db_path, viz_path)
        gerar_grafico_top5_clientes_rentaveis(db_path, viz_path)
        gerar_grafico_faturamento_subcategoria(db_path, viz_path)
        
        print("Todas as imagens foram geradas com sucesso!")

        logging.info(f"🎨 Todas as imagens foram salvas na pasta: {viz_path}")
        logging.info("✅ Pipeline executado com sucesso!")
        print("Pipeline finalizado com sucesso! Gráficos e tabelas atualizados.")

    except FileNotFoundError as e:
        logging.error(f"❌ Erro de arquivo não encontrado: {e}")
        print(f"Erro: Algum arquivo de dados está faltando. Verifique o log.")
        
if __name__ == "__main__":
    run_pipeline()