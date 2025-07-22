import asyncio
import httpx
import pandas as pd
from pprint import pprint
from amazon_scraper import amazon_scrap
from beleza_scraper import beleza_na_web_scrap
from magalu_scraper import magalu_scrap
from epoca_scraper import epoca_scrap
from decimal import Decimal
import logging
from colorama import init, Fore, Style

# Inicializa colorama para cores no terminal (necessário para Windows)
init()

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ENDPOINT = "http://201.23.64.234:8000/api/urls/"
PRODUCTS_ENDPOINT = "http://201.23.64.234:8000/api/products"

async def get_from_api():
    async with httpx.AsyncClient() as client:
        try:
            pprint(f"{Fore.CYAN}Enviando requisição GET para: {API_ENDPOINT}{Style.RESET_ALL}")
            response = await client.get(API_ENDPOINT, timeout=10)
            pprint(f"{Fore.CYAN}Resposta recebida - Status: {response.status_code}{Style.RESET_ALL}")
            if response.status_code != 200:
                pprint(f"{Fore.RED}Erro na API: {response.text}{Style.RESET_ALL}")
                return None
            response_data = response.json()
            pprint(f"{Fore.CYAN}Dados recebidos:{Style.RESET_ALL}")
            pprint(response_data, indent=2)
            if isinstance(response_data, list):
                return pd.DataFrame(response_data)
            else:
                pprint(f"{Fore.RED}Resposta não é uma lista: {response_data}{Style.RESET_ALL}")
                return None
        except httpx.RequestError as e:
            pprint(f"{Fore.RED}Erro ao conectar com a API: {e}{Style.RESET_ALL}")
            return None
        except ValueError as e:
            pprint(f"{Fore.RED}Erro ao processar JSON: {e}{Style.RESET_ALL}")
            return None

async def post_to_products(products):
    """Envia uma lista de JSONs para o endpoint POST /products."""
    async with httpx.AsyncClient() as client:
        try:
            # Mapeia os campos do JSON para corresponder ao schema ProductsDetailsIn
            payload = []
            for product in products:
                product_data = {
                    "ean": product["ean"],
                    "sku": product.get("sku", "SKU não encontrado"),
                    "loja": product.get("loja", "-"),
                    "preco_final": str(Decimal(str(product.get("preco_final", 0.00)))),
                    "marketplace": product.get("marketplace", "Desconhecido"),
                    "key_loja": product.get("key_loja", "sem_loja"),
                    "key_sku": product.get("key_sku", f"{product['ean']}_{product.get('loja', 'sem_loja')}"),
                    "descricao": product.get("descricao", "Sem descrição"),
                    "review": float(product.get("review", 0.0)),
                    "imagem": product.get("imagem", "https://via.placeholder.com/150"),
                    "status": product.get("status", "ativo"),
                    "preco_pricing": str(Decimal(str(product["preco_pricing"]))) if product.get("preco_pricing") else None,
                    "url": product.get("url","-"),
                    "marca": product.get("marca", "Marca não informada")
                }
                # Lidar com possíveis variações nos nomes dos campos
                if "price" in product and "preco_final" not in product:
                    product_data["preco_final"] = str(Decimal(str(product["price"])))
                if "image" in product and "imagem" not in product:
                    product_data["imagem"] = product["image"]
                payload.append(product_data)
            
            logger.info(f"Enviando {len(payload)} produtos para {PRODUCTS_ENDPOINT}")
            pprint(f"{Fore.CYAN}Enviando {len(payload)} produtos para: {PRODUCTS_ENDPOINT}{Style.RESET_ALL}")
            response = await client.post(PRODUCTS_ENDPOINT, json=payload, timeout=10)
            pprint(f"{Fore.CYAN}Resposta POST - Status: {response.status_code}{Style.RESET_ALL}")
            if response.status_code == 200:
                pprint(f"{Fore.GREEN}Produtos enviados com sucesso: {response.json()}{Style.RESET_ALL}")
                return response.json()
            else:
                logger.error(f"Erro ao enviar produtos: {response.text}")
                pprint(f"{Fore.RED}Erro ao enviar produtos: {response.text}{Style.RESET_ALL}")
                return None
        except httpx.RequestError as e:
            logger.error(f"Erro ao conectar com a API: {e}")
            pprint(f"{Fore.RED}Erro ao conectar com a API: {e}{Style.RESET_ALL}")
            return None
        except ValueError as e:
            logger.error(f"Erro ao processar JSON: {e}")
            pprint(f"{Fore.RED}Erro ao processar JSON: {e}{Style.RESET_ALL}")
            return None

async def main():
    pprint(f"{Fore.CYAN}Iniciando requisição GET para: {API_ENDPOINT}{Style.RESET_ALL}")
    df = await get_from_api()
    pprint(f"{Fore.CYAN}\nResultado do DataFrame:{Style.RESET_ALL}")
    if df is None or df.empty:
        pprint(f"{Fore.RED}Nenhum dado válido retornado ou DataFrame vazio.{Style.RESET_ALL}")
        return None

    pprint(f"{Fore.CYAN}Colunas do DataFrame: {df.columns.tolist()}{Style.RESET_ALL}")
    pprint(df)

    # Validate required columns
    required_columns = ['url', 'ean', 'brand']
    if not all(col in df.columns for col in required_columns):
        pprint(f"{Fore.RED}DataFrame não contém todas as colunas necessárias: {required_columns}{Style.RESET_ALL}")
        return None

    # Process rows and call appropriate scraper based on URL
    for index, row in df.iterrows():
        pprint(f"{Fore.CYAN}Processando EAN: {row['ean']}{Style.RESET_ALL}")
        try:
            result = await epoca_scrap(row['ean'], row['brand'])
            if result:
                pprint(f"{Fore.GREEN}Resultado epoca_scrap: {result}{Style.RESET_ALL}")
                await post_to_products(result)
            else:
                pprint(f"{Fore.RED}Nenhum resultado retornado por epoca_scrap para EAN: {row['ean']}{Style.RESET_ALL}")
        except Exception as e:
            pprint(f"{Fore.RED}Erro em epoca_scrap para EAN {row['ean']}: {e}{Style.RESET_ALL}")

        if "amazon" in row['url']:
            pprint(f"{Fore.CYAN}Executando amazon_scrap para EAN: {row['ean']}{Style.RESET_ALL}")
            try:
                result = await amazon_scrap(row['url'], row['ean'], row['brand'])
                if result:
                    pprint(f"{Fore.GREEN}Resultado amazon_scrap: {result}{Style.RESET_ALL}")
                    await post_to_products(result)
                else:
                    pprint(f"{Fore.RED}Nenhum resultado retornado por amazon_scrap para EAN: {row['ean']}{Style.RESET_ALL}")
            except Exception as e:
                pprint(f"{Fore.RED}Erro em amazon_scrap para EAN {row['ean']}: {e}{Style.RESET_ALL}")
        elif "belezanaweb" in row['url']:
            pprint(f"{Fore.CYAN}Executando beleza_na_web_scrap para EAN: {row['ean']}{Style.RESET_ALL}")
            try:
                result = await beleza_na_web_scrap(row['url'], row['ean'], row['brand'])
                if result:
                    pprint(f"{Fore.GREEN}Resultado beleza_na_web_scrap: {result}{Style.RESET_ALL}")
                    await post_to_products(result)
                else:
                    pprint(f"{Fore.RED}Nenhum resultado retornado por beleza_na_web_scrap para EAN: {row['ean']}{Style.RESET_ALL}")
            except Exception as e:
                pprint(f"{Fore.RED}Erro em beleza_na_web_scrap para EAN {row['ean']}: {e}{Style.RESET_ALL}")
        elif "magazineluiza" in row['url']:
            pprint(f"{Fore.CYAN}Executando magalu_scrap para EAN: {row['ean']}{Style.RESET_ALL}")
            try:
                result = await magalu_scrap(row['url'], row['ean'], row['brand'])
                if result:
                    pprint(f"{Fore.GREEN}Resultado magalu_scrap: {result}{Style.RESET_ALL}")
                    await post_to_products(result)
                else:
                    pprint(f"{Fore.RED}Nenhum resultado retornado por magalu_scrap para EAN: {row['ean']}{Style.RESET_ALL}")
            except Exception as e:
                pprint(f"{Fore.RED}Erro em magalu_scrap para EAN {row['ean']}: {e}{Style.RESET_ALL}")

    return df

if __name__ == "__main__":
    df = asyncio.run(main())
