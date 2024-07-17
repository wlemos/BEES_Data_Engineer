import requests
import json

# Endpoint da API
url = "https://api.openbrewerydb.org/breweries"

# Fazendo a solicitação GET para a API
response = requests.get(url)

# Verificando se a solicitação foi bem-sucedida
if response.status_code == 200:
    # Convertendo a resposta JSON em um dicionário Python
    breweries = response.json()
    # Definindo o caminho do arquivo onde os dados serão salvos
    file_path = "C:\\Users\\User\\Desktop\\BEES_Data_Engineer\\Bronze\\breweries.json"
    # Salvando os dados em um arquivo JSON
    with open(file_path, 'w') as file:
        json.dump(breweries, file, indent=4)
    print(f"Dados salvos em {file_path}")
else:
    print(f"Erro ao acessar a API: {response.status_code}")
