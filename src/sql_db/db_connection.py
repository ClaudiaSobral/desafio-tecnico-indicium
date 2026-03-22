"""
Módulo: database.py
Objetivo: Gerenciar a conexão e as operações com o banco de dados DuckDB.
"""

import duckdb
import pandas as pd
import logging

class GerenciadorDuckDB:
    """
    Classe para encapsular as operações do banco de dados DuckDB.
    """
    def __init__(self, caminho_banco: str = 'lh_nauticals.duckdb'):
        """
        Inicializa o gerenciador definindo o caminho do banco de dados.
        Se o arquivo não existir, o DuckDB o criará automaticamente.
        """
        self.caminho_banco = caminho_banco
        self.conexao = None

    def conectar(self):
        """Abre a conexão com o banco de dados."""
        try:
            self.conexao = duckdb.connect(database=self.caminho_banco, read_only=False)
            logging.info(f"Conexão com DuckDB aberta com sucesso em: {self.caminho_banco}")
        except Exception as e:
            logging.error(f"Erro ao conectar no DuckDB: {e}")
            raise e

    def desconectar(self):
        """Fecha a conexão com o banco de dados de forma segura."""
        if self.conexao:
            self.conexao.close()
            logging.info("Conexão com DuckDB encerrada.")

    def executar_query_para_df(self, query: str) -> pd.DataFrame:
        """
        Recebe uma string SQL (vinda do queries.py), executa no banco 
        e retorna os resultados diretamente como um DataFrame do Pandas.
        """
        if not self.conexao:
            self.conectar()
            
        try:
            # O DuckDB tem integração nativa com o Pandas através do .df()
            df_resultado = self.conexao.execute(query).df()
            logging.info(f"Query executada com sucesso. Linhas retornadas: {len(df_resultado)}")
            return df_resultado
        except Exception as e:
            logging.error(f"Erro ao executar a query: {e}")
            raise e

    def salvar_df_como_tabela(self, df: pd.DataFrame, nome_tabela: str):
        """
        Pega um DataFrame (ex: os resultados da IA) e salva como uma tabela física no DuckDB.
        """
        if not self.conexao:
            self.conectar()
            
        try:
            # O DuckDB consegue ler DataFrames da memória do Python diretamente
            # Cria-se ou substitui-se a tabela com os dados do DataFrame
            self.conexao.execute(f"CREATE OR REPLACE TABLE {nome_tabela} AS SELECT * FROM df")
            logging.info(f"Tabela '{nome_tabela}' salva/atualizada com sucesso no banco!")
        except Exception as e:
            logging.error(f"Erro ao salvar o DataFrame na tabela {nome_tabela}: {e}")
            raise e