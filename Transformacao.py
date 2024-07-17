import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Definindo o caminho do arquivo JSON
file_path = r"C:\\Users\\User\Desktop\BEES_Data_Engineer\Bronze\breweries.json"

# Lendo o arquivo JSON para um DataFrame do Pandas
breweries_data = pd.read_json(file_path)

# Convertendo o DataFrame do Pandas para uma tabela Arrow
table = pa.Table.from_pandas(breweries_data)

# Definindo o caminho para salvar o arquivo Parquet
parquet_file_path = r"C:\\Users\\User\Desktop\BEES_Data_Engineer\Silver\breweries.parquet"

# Salvando a tabela Arrow como arquivo Parquet
pq.write_table(table, parquet_file_path)

print(f"Dados salvos em formato Parquet em {parquet_file_path}")