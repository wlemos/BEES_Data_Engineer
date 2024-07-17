import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Caminho para o arquivo Parquet
parquet_file_path = r"C:\\Users\\User\Desktop\BEES_Data_Engineer\Silver\breweries"

# Lendo o arquivo Parquet como um DataFrame do Pandas
table = pq.read_table(parquet_file_path)
breweries_data = table.to_pandas()

# Exibindo as primeiras linhas do DataFrame para entender a estrutura
print(breweries_data.head())

aggregated_data = breweries_data.groupby(['brewery_type', 'state']).size().reset_index(name="contagem_linhas")

# Exibindo o resultado da agregação
print(aggregated_data.head())

# Convertendo o DataFrame do Pandas para uma tabela Arrow
table = pa.Table.from_pandas(aggregated_data)

# Definindo o caminho para salvar o arquivo Parquet
base_path = r"C:\\Users\\User\Desktop\BEES_Data_Engineer\\Gold\breweries_agregada.parquet"

# Salvando a tabela Arrow como arquivo Parquet
pq.write_table(table, base_path)

print(f"Dados salvos em formato Parquet em {base_path}")
