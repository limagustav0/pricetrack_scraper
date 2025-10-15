import requests

ean_key = "7898724572612beleza"
response = requests.delete(f'http://localhost:8000/api/urls/{ean_key}')

if response.status_code == 200:
    print("✅ Deletado com sucesso!")
    print(response.json())
else:
    print(f"❌ Erro: {response.status_code}")
    print(response.json())