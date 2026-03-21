import duckdb
import os

class DuckDBLoader:
    """Classe responsável por carregar dados limpos no banco DuckDB.

    Gerencia a conexão com o banco e a criação automática de pastas
    e tabelas a partir de DataFrames.
    """
    
    def __init__(self, db_path):
        """Cria a conexão com o banco e garante a existência da pasta.

        Args:
            db_path (str): Caminho completo para o arquivo .duckdb.
        """
        
        folder = os.path.dirname(db_path)

        if folder and not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Pasta {folder} criada com sucesso!")

        self.con = duckdb.connect(db_path)

    def load_dataframe(self, df, table_name):
        """Salva um DataFrame do Pandas como uma tabela SQL no DuckDB.

        Se a tabela já existir, ela será substituída pelos dados novos.

        Args:
            df (pd.DataFrame): Tabela com os dados para salvar.
            table_name (str): Nome que a tabela terá no banco de dados.
        """
        
        self.con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        print(f"Tabela {table_name} carregada com sucesso!")

    def close(self):
        self.con.close()