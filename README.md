# Projeto ETL + Análise + ML para LH Nautical 
Esse projeto cria uma pipeline de dados com ETL de dados brutos, processamento para análise e criação de modelos preditivos utilizando as bases da LH Nauticals utilizando Python e DuckDB. <br>

## Visão geral
O objetivo do projeto é integrar dados brutos fragmentados do cliente **LH Nautical** em uma única base de dados, utilizando uma arquitetura modular Modern Data Stack (MDS) que permite a implementação e atualização contínua de uma pipeline de dados, estabelecendo-se como uma solução ao cenário atual de dispersão dos dados do cliente, garantindo uma "Single Source of Truth" para o cliente. O projeto inclui:<br>
* Armazenamento de Alta Performance: consolidação dos dados em uma base de dados DuckDB (aceita pandas e polars, além de possuir fácil integração com datalakes caso o cliente deseje migrar);
* Pipeline Modular: Estrutura organizada em módulos (src/), favorecendo a manutenção e escalabilidade do projeto.

* Integração com API: conversão monetária em tempo real, garantindo precisão financeira nos dados de vendas.

* Análise Preditiva: Modelos de Machine Learning integrados para previsão de demanda futura e sistema de recomendações.

* Visualização Automatizada: Geração de relatórios gráficos automatizado baseado em queries de SQL e bibliotecas de visualização (Matplotlib/Seaborn).

## Insights de negócio
* Das três categorias vendidas pela LH Nauticals (ancoragem, eletrônicos e propulsão), apenas "propulsão" dá lucro para a empresa. Eletrônicos dá um pouco de prejuízo e ancoragem, mais ainda.
* O Rio de Janeiro, Amapá e Espírito Santo são os estados com maior tíquete médio por compra.
* Setembro é o mês de pior faturamento histórico da LH Nautical. Depois disso, há uma ascendente até atingir o pico máximo em novembro e voltar a cair até janeiro, onde retoma um ritmo mais ou menos constante até o reinício do ciclo.
* Todos os cinco produtos mais lucrativos da empresa são motores.
* Os cinco maiores prejuízos estão na categoria ancoragem.
* Dos tipos de produto (subcategoria), apenas os *motores* dão lucro. Todo o resto causa prejuízo.
* Terça é o melhor dia de faturamento histórico, enquanto segunda é o pior.
* 4 dos 5 clientes mais rentáveis são mulheres.

## Recomendações ao cliente
* Fazer um trabalho de Data Quality nas tabelas brutas, para garantir que a excelência operacional e a consolidação dos dados.
* Adotar uma estrutura em nuvem para integrar melhor os dados é uma estratégia que eu recomendaria avaliar a seguir. Podemos criar um planejamento de migração, avaliando quanto tempo o projeto demoraria para se pagar. Indicaria adotar a *Databricks*, por ser parceira da Indicium AI, que vai poder oferecer a máxima expertise e eficiência na transição para a nuvem.
* Vale avaliar o direcionamento do marketing da LH Nauticals para o público feminino. É necessário uma investigação maior.
* ❗É urgente rever os preços de importação e venda dos produtos. Apenas equipamentos de propulsão, da subcategoria motores, fecham com um saldo positivo no e-commerce.❗


## Arquitetura do projeto
🗂️ lh_nautical_project/ <br>
├── 📂 data/<br>
│   ├── 📂 raw/                 <- Dados brutos (CSV, JSON)<br>
│   ├── 📂 processed/           <- Banco de dados DuckDB consolidado<br>
│   └── 📂 visualizations/      <- Gráficos gerados automaticamente (.png)<br>
├── 📂 notebooks/               <- Notebook de exploração dos datasets brutos<br>
├── 📂 src/<br>
│   ├── 📂 ETL/                 <- Scripts de ETL em pandas <br>
│   ├── 📂 models/              <- Modelos preditivos <br>
│   ├── 📂 sql_db/              <- Conexão com banco e queries SQL puras<br>
│   └── 📂 visualizations/      <- Geração de gráficos (Seaborn/Matplotlib)<br>
├── main.py                      <- Orquestrador principal do pipeline<br>
└── requirements.txt             <- Dependências do projeto<br>

## Problemas conhecidos, desafios e updates futuros
* 🕝 **Tempo de execução**: a chamada do API para ler a cotação do dólar no dia da compra dos produtos é bastante demorada. Seria interessante encontrar mecanimos para reduzir o tempo de execução do script.
* ❌ **Dependência da IA**: houve muita dependência de IA Generativa para a criação de modelos preditivos. Seria interessante um maior estudo da minha parte e adequação para entregar um resultado mais relevante, com maior capacidade de análise, e mais eficiente.<br>
* 🔒 **LGPD**: além disso, em um projeto real, dados sensíveis do cliente precisariam passar por um processo de anonimização antes de serem alimentados a uma IA Generativas para respeito a LGPD e por questões de segredo industrial. Por se tratar de um cliente fictício, esses dados foram alimentados à IA sem tratamento. <br>
* 📊 **Refino visual**: alguns gráficos poderiam ser melhor refinados (existe termos em exibição como "1e6, que decorrem da notação científica das bibliotecas, além de que seria ideal adaptar as apresentações ao manual da marca de um cliente real, com paletas mais adequadas).
* 🔎**Clareza ao rodar**: seria interessante que o log, os prints e previsões de erro fossem mais detalhados para o acompanhamento no terminal e no próprio log.

## Instalação
1. **Clone o repositório**
```
git clone https://github.com/yourusername/bdesafio-tecnico-indicium.git
cd lh_nautical_project
```
2. ***(Optional) Crie um virtual environment***
```python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Instale as dependências do projeto**
```
pip install -r requirements.txt
```

4. **Execute o script main.py**
