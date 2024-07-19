import requests
import json
import s3fs

# Endpoint da API
url = "https://api.openbrewerydb.org/breweries"

# Configurações do MinIO
minio_url = "http://minio:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_name = "bronze"

try:
    # Criando um sistema de arquivos S3 com s3fs
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': minio_url}
    )
    print("Sistema de arquivos S3 configurado")

    # Fazendo a solicitação GET para a API
    response = requests.get(url)
    print("Requisição à API realizada")

    # Verificando se a solicitação foi bem-sucedida
    if response.status_code == 200:
        print("Requisição bem-sucedida")

        # Convertendo a resposta JSON em um dicionário Python
        breweries = response.json()

        # Definindo o caminho do arquivo onde os dados serão salvos
        file_path = f"{bucket_name}/breweries.json"

        # Gravando o DataFrame como um arquivo JSON no MinIO
        with fs.open(file_path, 'w') as file:
            json.dump(breweries, file, indent=4)
        print(f"Dados salvos em {file_path}")
    else:
        print(f"Erro ao acessar a API: {response.status_code}")

except Exception as e:
    print(f"Ocorreu um erro: {e}")
