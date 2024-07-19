import pandas as pd
import s3fs
import json

# Configurações do MinIO
minio_url = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_leitura = "bronze"
bucket_gravacao = "silver"

# Definindo o caminho do arquivo JSON
path_leitura = f"{bucket_leitura}/breweries.json"
path_gravacao = f"{bucket_gravacao}/breweries"

try:
    # Criando um sistema de arquivos S3 com s3fs
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': minio_url}
    )
    print("Sistema de arquivos S3 configurado")

    # Abrindo e lendo o arquivo JSON do MinIO
    with fs.open(path_leitura, 'r') as file:
        breweries = json.load(file)
    
    # Convertendo o JSON para um DataFrame do Pandas
    breweries_data = pd.DataFrame(breweries)
    
    # Gravar o DataFrame como Parquet particionado
    breweries_data.to_parquet(
        f"s3://{path_gravacao}",
        partition_cols=['state'],
        storage_options={
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {
                "endpoint_url": minio_url
            }
        },
        engine='pyarrow'  # Pode usar 'pyarrow' ou 'fastparquet'
    )
    
    print(f"DataFrame gravado com sucesso em s3://{path_gravacao}")

except Exception as e:
    print(f"Ocorreu um erro: {e}")
