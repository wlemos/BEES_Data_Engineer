# BEES_Data_Engineer
<h1 align="center"> BEES_Data_Engineer </h1>

# :hammer: Funcionalidades do projeto

- `Funcionalidade 1`: Script *leituraAPI.py*, leitura da API *https://api.openbrewerydb.org/breweries* e gravação dos dados em JSON na camada Bronze.
- `Funcionalidade 2`: Script *transformacao.py*, Leitura dos dados na Camada Bronze, transformação dos dados em uma tabela colunar e salvando um arquivo parquet particionado pelo *state* na camada Silver.
- `Funcionalidade 3`: Script *tabelaAgregada.py*, Leitura dos dados na Camada Silver, agregando os valores por *Type e State* e salvando um arquivo parquet na camada Gold.