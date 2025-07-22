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
from datetime import datetime

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ENDPOINT = "http://201.23.64.234:8000/api/urls/"
PRODUCTS_ENDPOINT = "http://201.23.64.234:8000/api/products"

async def get_from_api():
    async with httpx.AsyncClient() as client:
        try:
            logger.info("========== Iniciando requisição GET para: %s ==========", API_ENDPOINT)
            print(f"INFO: Enviando requisição GET para: {API_ENDPOINT}")
            response = await client.get(API_ENDPOINT, timeout=10)
            logger.info("Resposta recebida - Status: %s", response.status_code)
            print(f"INFO: Resposta recebida - Status: {response.status_code}")
            if response.status_code != 200:
                logger.error("Erro na API: %s", response.text)
                print(f"ERROR: Erro na API: {response.text}")
                return None
            response_data = response.json()
            logger.info("Dados recebidos:")
            print("INFO: Dados recebidos:")
            pprint(response_data, indent=2)
            if isinstance(response_data, list):
                return pd.DataFrame(response_data)
            else:
                logger.warning("Resposta não é uma lista: %s", response_data)
                print(f"WARNING: Resposta não é uma lista: {response_data}")
                return None
        except httpx.RequestError as e:
            logger.error("Erro ao conectar com a API: %s", e)
            print(f"ERROR: Erro ao conectar com a API: {e}")
            return None
        except ValueError as e:
            logger.error("Erro ao processar JSON: %s", e)
            print(f"ERROR: Erro ao processar JSON: {e}")
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
                    "url": product.get("url", "-"),
                    "marca": product.get("marca", "Marca não informada")
                }
                # Lidar com possíveis variações nos nomes dos campos
                if "price" in product and "preco_final" not in product:
                    product_data["preco_final"] = str(Decimal(str(product["price"])))
                if "image" in product and "imagem" not in product:
                    product_data["imagem"] = product["image"]
                payload.append(product_data)
            
            logger.info("Enviando %s produtos para %s", len(payload), PRODUCTS_ENDPOINT)
            print(f"INFO: Enviando {len(payload)} produtos para: {PRODUCTS_ENDPOINT}")
            response = await client.post(PRODUCTS_ENDPOINT, json=payload, timeout=10)
            logger.info("Resposta POST - Status: %s", response.status_code)
            print(f"INFO: Resposta POST - Status: {response.status_code}")
            if response.status_code == 200:
                logger.info("Produtos enviados com sucesso: %s", response.json())
                print(f"SUCCESS: Produtos enviados com sucesso: {response.json()}")
                return response.json()
            else:
                logger.error("Erro ao enviar produtos: %s", response.text)
                print(f"ERROR: Erro ao enviar produtos: {response.text}")
                return None
        except httpx.RequestError as e:
            logger.error("Erro ao conectar com a API: %s", e)
            print(f"ERROR: Erro ao conectar com a API: {e}")
            return None
        except ValueError as e:
            logger.error("Erro ao processar JSON: %s", e)
            print(f"ERROR: Erro ao processar JSON: {e}")
            return None

async def main():
    # Contador para rastrear o número de raspagens
    total_raspagens = 0
    raspagens_concluidas = 0
    
    logger.info("========== Início da Execução - %s ==========", datetime.now().strftime("%Y-%m-%d %H:%M:%S %z"))
    print("INFO: Iniciando requisição GET para:", API_ENDPOINT)

    df = await get_from_api()
    logger.info("Resultado do DataFrame:")
    print("\nINFO: Resultado do DataFrame:")
    if df is None or df.empty:
        logger.warning("Nenhum dado válido retornado ou DataFrame vazio.")
        print("WARNING: Nenhum dado válido retornado ou DataFrame vazio.")
        return None

    logger.info("Colunas do DataFrame: %s", df.columns.tolist())
    print("INFO: Colunas do DataFrame:", df.columns.tolist())
    print(df)

    # Validação das colunas necessárias
    required_columns = ['url', 'ean', 'brand']
    if not all(col in df.columns for col in required_columns):
        logger.error("DataFrame não contém todas as colunas necessárias: %s", required_columns)
        print(f"ERROR: DataFrame não contém todas as colunas necessárias: {required_columns}")
        return None

    # Total de raspagens a serem feitas
    total_raspagens = len(df)
    
    # Processa as linhas e chama o scraper apropriado baseado na URL
    for index, row in df.iterrows():
        raspagens_concluidas += 1
        logger.info("========== Processando EAN: %s ==========", row['ean'])
        print(f"INFO: Processando EAN: {row['ean']}")
        try:
            result = await epoca_scrap(row['ean'], row['brand'])
            if result:
                logger.info("Resultado de epoca_scrap: %s", result)
                print(f"SUCCESS: Resultado de epoca_scrap: {result}")
                await post_to_products(result)
            else:
                logger.warning("Nenhum resultado retornado por epoca_scrap para EAN: %s", row['ean'])
                print(f"WARNING: Nenhum resultado retornado por epoca_scrap para EAN: {row['ean']}")
        except Exception as e:
            logger.error("Erro em epoca_scrap para EAN %s: %s", row['ean'], e)
            print(f"ERROR: Erro em epoca_scrap para EAN {row['ean']}: {e}")

        logger.info("Raspagens concluídas: %d de %d", raspagens_concluidas, total_raspagens)
        print(f"INFO: Raspagens concluídas: {raspagens_concluidas} de {total_raspagens}")

        if "amazon" in row['url']:
            logger.info("Executando amazon_scrap para EAN: %s", row['ean'])
            print(f"INFO: Executando amazon_scrap para EAN: {row['ean']}")
            try:
                result = await amazon_scrap(row['url'], row['ean'], row['brand'])
                if result:
                    logger.info("Resultado de amazon_scrap: %s", result)
                    print(f"SUCCESS: Resultado de amazon_scrap: {result}")
                    await post_to_products(result)
                else:
                    logger.warning("Nenhum resultado retornado por amazon_scrap para EAN: %s", row['ean'])
                    print(f"WARNING: Nenhum resultado retornado por amazon_scrap para EAN: {row['ean']}")
            except Exception as e:
                logger.error("Erro em amazon_scrap para EAN %s: %s", row['ean'], e)
                print(f"ERROR: Erro em amazon_scrap para EAN {row['ean']}: {e}")
        elif "belezanaweb" in row['url']:
            logger.info("Executando beleza_na_web_scrap para EAN: %s", row['ean'])
            print(f"INFO: Executando beleza_na_web_scrap para EAN: {row['ean']}")
            try:
                result = await beleza_na_web_scrap(row['url'], row['ean'], row['brand'])
                if result:
                    logger.info("Resultado de beleza_na_web_scrap: %s", result)
                    print(f"SUCCESS: Resultado de beleza_na_web_scrap: {result}")
                    await post_to_products(result)
                else:
                    logger.warning("Nenhum resultado retornado por beleza_na_web_scrap para EAN: %s", row['ean'])
                    print(f"WARNING: Nenhum resultado retornado por beleza_na_web_scrap para EAN: {row['ean']}")
            except Exception as e:
                logger.error("Erro em beleza_na_web_scrap para EAN %s: %s", row['ean'], e)
                print(f"ERROR: Erro em beleza_na_web_scrap para EAN {row['ean']}: {e}")
        # elif "magazineluiza" in row['url']:
        #     logger.info("Executando magalu_scrap para EAN: %s", row['ean'])
        #     print(f"INFO: Executando magalu_scrap para EAN: {row['ean']}")
        #     try:
        #         result = await magalu_scrap(row['url'], row['ean'], row['brand'])
        #         if result:
        #             logger.info("Resultado de magalu_scrap: %s", result)
        #             print(f"SUCCESS: Resultado de magalu_scrap: {result}")
        #             await post_to_products(result)
        #         else:
        #             logger.warning("Nenhum resultado retornado por magalu_scrap para EAN: %s", row['ean'])
        #             print(f"WARNING: Nenhum resultado retornado por magalu_scrap para EAN: {row['ean']}")
        #     except Exception as e:
        #         logger.error("Erro em magalu_scrap para EAN %s: %s", row['ean'], e)
        #         print(f"ERROR: Erro em magalu_scrap para EAN {row['ean']}: {e}")

    logger.info("========== Fim da Execução - %s ==========", datetime.now().strftime("%Y-%m-%d %H:%M:%S %z"))
    return df

if __name__ == "__main__":
    df = asyncio.run(main())
