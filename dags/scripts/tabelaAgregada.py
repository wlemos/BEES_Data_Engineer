import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

# Configurações do MinIO
minio_url = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_leitura = "silver"
bucket_gravacao = "gold"

# Definindo o caminho do arquivo JSON
path_leitura = f"{bucket_leitura}/breweries"
path_gravacao = f"{bucket_gravacao}/breweries.parquet"



try:
    # Criando um sistema de arquivos S3 com s3fs
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': minio_url}
    )
    print("Sistema de arquivos S3 configurado")

    # Ler o arquivo Parquet do MinIO usando pandas
    df = pd.read_parquet(
        f"s3://{path_leitura}",
        storage_options={
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {
                "endpoint_url": minio_url
            }
        },
        engine='pyarrow'  # Pode usar 'pyarrow' ou 'fastparquet'
    )

    aggregated_data = df.groupby(['brewery_type', 'state']).size().reset_index(name="contagem_linhas")

    aggregated_data.head()
    # Gravar o DataFrame como Parquet particionado
    aggregated_data.to_parquet(
        f"s3://{path_gravacao}",
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
