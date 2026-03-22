import pandas as pd
import requests
import logging
import time

class TransformadorDados():
    """Classe responsável por aplicar as regras de limpeza aos dados brutos.

    Contém a lógica para corrigir erros de digitação, padronizar e-mails
    e extrair informações geográficas de strings complexas.
    """

    def __init__(self, lista_estados):
        """Inicializa o transformador com uma lista de estados válidos.

        Args:
            ufs_list (list): Lista de siglas de estados (ex: ['SP', 'RJ']).
        """
        self.ufs = lista_estados

    def limpar_produtos(self, df):
        """Realiza a limpeza e padronização da base de produtos.

        Remove símbolos monetários, corrige erros de digitação nas categorias
        usando um mapa de substituição e extrai a primeira palavra do nome como subcategoria.

        Args:
            df (pd.DataFrame): DataFrame bruto de produtos.

        Returns:
            pd.DataFrame: DataFrame com preços numéricos, categorias corrigidas e colunas renomeadas.
        """

        # 🟢 Limpeza do preço (retira o R$) e transformação em float
        df["price"] = df["price"].str.replace("R$ ", "", regex=False) 
        df["price"] = pd.to_numeric(df["price"])

        # 🟢 Padronização categorias
        df["actual_category"] = df["actual_category"].str.replace(" ", "").str.lower() #Tira espaços entre as palavras e as minimiza

        mapa_substituicoes = {"eletronicos": "eletrônicos", "eletronicoz": "eletrônicos", "eletrunicos": "eletrônicos", "eletroniscos": "eletrônicos",
                              "propulsao": "propulsão", "propução": "propulsão", "prop": "propulsão", "propulsãoulsão": "propulsão", "propulsãoulssão": "propulsão",
                              "propulsãoulçao": "propulsão", "propulsãoulsam": "propulsão", "propulsãoulção": "propulsão",
                              "ancoraguem": "ancoragem", "encoragem": "ancoragem", "ancorajm": "ancoragem", "ancorajem": "ancoragem", "ancorajen": "ancoragem",
                              "encoragi": "ancoragem", "ancoragen": "ancoragem"}
        for errado, certo in mapa_substituicoes.items():
            df["actual_category"] = df["actual_category"].str.replace(errado, certo) # Faz a substituição de erros de digitação
        
        # 🟢 Cria coluna com subcategoria de produto selecionando a primeira palavra da coluna "produto"
        df["subcategoria_produto"] = df["name"].str.split().str[0]

        # 🟢 Renomeia columas da tabela de produtos
        return df.rename(columns={
            'name': 'produto','price': 'preco','code': 'id_produto', 'actual_category': 'categoria_produto'
        }) 
    
        
    
    def extrair_estado_cidade(self, local):
        """Separa a sigla do estado e o nome da cidade de um texto.

        Busca as siglas definidas no __init__ no início ou fim do texto.

        Args:
            local (str): Texto contendo cidade e estado misturados.

        Returns:
            tuple: (sigla_estado, nome_cidade). Retorna None se não identificar.
        """

        # 🟢 Verifica se o dado é uma string
        if not isinstance(local, str):
            return None, None
        
        # 🟢 Tira espaços em branco ao redor da palavra
        local = local.strip() 
        
        # 🟢 Verifica se há só estado na coluna "localization" 

        if local[:2] in self.ufs and (len(local) == 2):
            return local[:2], "Não informado"
        
        # 🟢 Verifica início
        if local[:2] in self.ufs and (len(local) != 2):
            return local[:2], local[2:].strip("-_ /,")
        
        # 🟢 Verifica final
        if local[-2:] in self.ufs and (len(local) != 2):
            return local[-2:], local[:-2].strip("-_ /,")
        return None, local
    

    def limpar_vendas(self, df):
        """Padroniza a base de vendas, focando na conversão de datas.

        Converte a coluna de data para o formato datetime do Python, lidando com
        formatos mistos de entrada.

        Args:
            df (pd.DataFrame): DataFrame bruto de vendas.

        Returns:
            pd.DataFrame: DataFrame com datas tipadas corretamente e colunas renomeadas.
        """
        # 🟢 Transforma a coluna de datas em formato datetime
        df["sale_date"] = pd.to_datetime(df["sale_date"], format='mixed').dt.date

        return df.rename(columns={'id': 'id_vendas', 'id_client': 'id_cliente', 'sale_date': 'data_venda', 'id_product': 'id_produto'})


    def limpar_clientes(self, df):
        """Limpa a base de clientes e extrai informações geográficas.

        Corrige caracteres especiais em e-mails e utiliza o método da classe
        para separar cidade e estado da coluna de localização.

        Args:
            df (pd.DataFrame): DataFrame bruto de clientes.

        Returns:
            pd.DataFrame: DataFrame com e-mails corrigidos, localização dividida e colunas renomeadas.
        """

        # 🟢 Corrige o erro de digitação em alguns e-mails, trocando # por @
        df["email"] = df["email"].str.replace("#", "@")

        # 🟢 Aplica a função de transformação da coluna "location"
        df[['estado', 'cidade']] = df["location"].apply(
            lambda x: pd.Series(self.extrair_estado_cidade(x))
            )
        
        df = df.drop(columns=["location"])

        return df.rename(columns={
            'full_name': 'nome_cliente', 'code': 'id_cliente', 'email': 'email_client'
            }) 
    

    def buscar_dolar_historico(self, datas_unicas):
            """Busca a cotação do dólar para uma lista de datas específicas.
            
            Args:
                datas_unicas (list): Lista de objetos datetime.date ou strings YYYYMMDD.
                
            Returns:
                dict: Dicionário mapeando {data: cotacao_brl}.
            """
            mapa_cambio = {}
            total = len(datas_unicas)
            print(f"🚀 Iniciando busca de {total} cotações únicas...")            

            for i, data in enumerate(datas_unicas):
                # 🟢 Print de progresso:
                print(f"⏳ Processando data {i+1} de {total}: {data}")

                # 🟢 Data formatada para API (AAAAMMDD)
                data_str = data.strftime('%Y%m%d')
                try:
                    # API de cotação por data específica
                    url = f"https://economia.awesomeapi.com.br/json/daily/USD-BRL/?start_date={data_str}&end_date={data_str}"
                    resposta = requests.get(url, timeout=10)
                    dados = resposta.json()

                    if not dados:
                        # 🟢 Busca a última cotação disponível (até 3 dias antes para cobrir feriadões)
                        url_fallback = f"https://economia.awesomeapi.com.br/json/daily/USD-BRL/1?timestamp={int(time.mktime(data.timetuple()))}"
                        resposta = requests.get(url_fallback, timeout=10)
                        dados = resposta.json()
                    
                    # 🟢 Grava o valor de fechamento (bid)
                    cotacao = float(dados[0]['bid'])
                    mapa_cambio[data] = cotacao
                    print(f"   ✅ R$ {cotacao:.2f}")                
                
                except Exception as e:
                    logging.warning(f"Falha total para {data}: {e}. Usando 5.10")
                    mapa_cambio[data] = 5.10

                time.sleep(0.3)

            return mapa_cambio

    def limpar_custos_importacoes(self, df, mapa_cambio):
        """Processa e expande os dados históricos de custos de importação.

            Esta função 'explode' listas aninhadas em colunas JSON, normaliza os dados
                históricos e garante a tipagem correta de IDs e datas, além de criar uma coluna de custo de importação convertida em reais.
            Args:
                df (pd.DataFrame): DataFrame contendo dados aninhados de importação.
                mapa_cambio (dict): Dicionário {data: valor_dolar} obtido na API.

            Returns:
                pd.DataFrame: DataFrame expandido (long format) com preços e datas de vigência.
                """

        # 🟢 Transformação da coluna "historic_data" em colunas "start_date" e "usd_price" 
        dados_historicos = df["historic_data"]

        df_explodido = dados_historicos.explode("historic_data")
        df_final = pd.json_normalize(df_explodido)
        df_final.index = df_explodido.index
        df = df.drop(columns=['historic_data']).join(df_final)

        # 🟢 Garante que o preço em dólar é numérico
        df["usd_price"] = pd.to_numeric(df["usd_price"])

        # 🟢 Coluna temporária para a taxa daquele dia
        df["taxa_do_dia"] = df["start_date"].map(mapa_cambio)
                
        # Se por acaso alguma data não tiver no mapa, usamos 5.10 como fallback (segurança)
        df["taxa_do_dia"] = df["taxa_do_dia"].fillna(5.10)

        # 🟢 Cria coluna em reais
        df["brl_price"] = round((df["usd_price"] * df["taxa_do_dia"]), 2)        

        # 🟢 Alteração da coluna de id para int
        df["product_id"] = df["product_id"].astype('Int64')

        # 🟢 Mudança do tipo da coluna start_date para datetime
        df["start_date"] = pd.to_datetime(df["start_date"], format='%d/%m/%Y').dt.date
                
        return df.rename(columns={
        'product_id': 'id_produto', 'product_name': 'produto', 'category': 'categoria'
        })


