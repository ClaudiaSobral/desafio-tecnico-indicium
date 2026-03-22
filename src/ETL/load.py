import duckdb
import pandas as pd
import logging

class DuckDBLoader:
    def __init__(self, db_path: str):
        # 🟢 O DuckDB conecta ao arquivo existente ou cria um novo se não achar
        self.conn = duckdb.connect(db_path)

    def load_dataframe(self, df: pd.DataFrame, nome_tabela: str, modo: str = 'replace'):
        """
        Carrega um DataFrame no DuckDB.
        
        Parâmetros:
        - modo: 'replace' (apaga e recria) ou 'append' (adiciona ao fim da tabela existente).
        """
        try:
            if modo == 'replace':
                # 🟢 Sobrescreve a tabela inteira (Carga Total)
                self.conn.execute(f"CREATE OR REPLACE TABLE {nome_tabela} AS SELECT * FROM df")
                logging.info(f"Tabela '{nome_tabela}' recriada com sucesso (Replace).")
                
            elif modo == 'append':
                # 🟢 Verifica se a tabela já existe no banco
                tabelas_existentes = self.conn.execute("SHOW TABLES").df()
                
                if nome_tabela in tabelas_existentes['name'].values:
                    # 🟢 e existe, apenas insere os dados novos (Carga Incremental)
                    self.conn.execute(f"INSERT INTO {nome_tabela} SELECT * FROM df")
                    logging.info(f"Novos registros adicionados à tabela '{nome_tabela}' (Append).")
                else:
                    # 🟢 Se é a primeira vez rodando, cria a tabela
                    self.conn.execute(f"CREATE TABLE {nome_tabela} AS SELECT * FROM df")
                    logging.info(f"Tabela '{nome_tabela}' criada pela primeira vez (Append fallback).")
            else:
                logging.warning(f"Modo '{modo}' não reconhecido. Use 'replace' ou 'append'.")

        except Exception as e:
            logging.error(f"Erro ao carregar a tabela {nome_tabela}: {e}")
            raise e

    def close(self):
        self.conn.close()