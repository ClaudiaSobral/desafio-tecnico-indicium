import pandas as pd

class ExtrairDados:
    """Classe responsável por buscar dados brutos em arquivos locais.

    Esta classe gerencia o carregamento de arquivos CSV e JSON, centralizando o caminho da pasta de origem (raw).
    """    

    def __init__(self, path_raw):
        """Inicializa o extrator com o caminho da pasta de dados.

        Args:
            path_raw (str): Caminho da pasta onde os arquivos originais estão.
        """
        self.path_raw = path_raw
    
    def load_csv(self, file_name):
        """Lê um arquivo CSV e o converte em um DataFrame do Pandas.

        Args:
            file_name (str): Nome do arquivo (ex: 'vendas.csv').

        Returns:
            pd.DataFrame: Tabela contendo os dados do arquivo.
        """
        full_path = f"{self.path_raw}/{file_name}"
        return pd.read_csv(full_path)
    
    def load_json(self, file_name):
        """Lê um arquivo JSON e o converte em um DataFrame do Pandas.

        Args:
            file_name (str): Nome do arquivo (ex: 'vendas.json').

        Returns:
            pd.DataFrame: Tabela contendo os dados do arquivo.
        """
        full_path = f"{self.path_raw}/{file_name}"
        return pd.read_json(full_path)