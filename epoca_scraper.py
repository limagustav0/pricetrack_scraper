import asyncio
import httpx
import pandas as pd
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

# Limite de concorrência para melhorar a velocidade
CONCURRENCY_LIMIT = 20
REQUEST_TIMEOUT = 8

async def get_from_api(client):
    """Obtém dados da API e retorna um DataFrame."""
    try:
        logger.info("Iniciando requisição GET para: %s", API_ENDPOINT)
        response = await client.get(API_ENDPOINT, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            logger.error("Erro na API: %s", response.status_code)
            return None
        response_data = response.json()
        if isinstance(response_data, list):
            return pd.DataFrame(response_data)
        logger.warning("Resposta não é uma lista")
        return None
    except httpx.RequestError as e:
        logger.error("Erro ao conectar com a API: %s", e)
        return None
    except ValueError as e:
        logger.error("Erro ao processar JSON: %s", e)
        return None

async def post_to_products(products, client):
    """Envia uma lista de produtos para o endpoint POST /products."""
    try:
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
            if "price" in product and "preco_final" not in product:
                product_data["preco_final"] = str(Decimal(str(product["price"])))
            if "image" in product and "imagem" not in product:
                product_data["imagem"] = product["image"]
            payload.append(product_data)
        
        logger.info("Enviando %s produtos para %s", len(payload), PRODUCTS_ENDPOINT)
        response = await client.post(PRODUCTS_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            logger.info("Produtos enviados com sucesso")
            return response.json()
        logger.error("Erro ao enviar produtos: %s", response.status_code)
        return None
    except (httpx.RequestError, ValueError) as e:
        logger.error("Erro ao enviar produtos: %s", e)
        return None

async def scrape_url(row, semaphore, client):
    """Processa uma única linha do DataFrame, executando os scrapers apropriados."""
    async with semaphore:
        ean = row['ean']
        url = row['url']
        brand = row['brand']
        
        # Validação inicial da URL
        if not url or not isinstance(url, str):
            logger.warning("URL inválida para EAN %s, ignorando", ean)
            return None

        logger.info("Processando EAN: %s", ean)
        results = []
        try:
            if "amazon" in url.lower():
                logger.info("Executando amazon_scrap para EAN: %s", ean)
                amazon_result = await amazon_scrap(url, ean, brand)
                if amazon_result:
                    results.extend(amazon_result)
                    logger.info("Resultados obtidos do Amazon para EAN %s", ean)
            elif "belezanaweb" in url.lower():
                # Executa beleza_na_web_scrap e epoca_scrap em paralelo
                logger.info("Executando beleza_na_web_scrap e epoca_scrap para EAN: %s", ean)
                beleza_task = beleza_na_web_scrap(url, ean, brand)
                epoca_task = epoca_scrap(ean, brand)
                beleza_result, epoca_result = await asyncio.gather(beleza_task, epoca_task, return_exceptions=True)
                
                if isinstance(beleza_result, list) and beleza_result:
                    results.extend(beleza_result)
                    logger.info("Resultados obtidos do Beleza na Web para EAN %s", ean)
                elif isinstance(beleza_result, Exception):
                    logger.error("Erro no beleza_na_web_scrap para EAN %s: %s", ean, beleza_result)
                
                if isinstance(epoca_result, list) and epoca_result:
                    results.extend(epoca_result)
                    logger.info("Resultados obtidos do Época para EAN %s", ean)
                elif isinstance(epoca_result, Exception):
                    logger.error("Erro no epoca_scrap para EAN %s: %s", ean, epoca_result)

            if results:
                logger.info("Enviando %s resultados para EAN %s", len(results), ean)
                await post_to_products(results, client)
            else:
                logger.warning("Nenhum resultado retornado para EAN: %s", ean)
            return results
        except Exception as e:
            logger.error("Erro ao processar EAN %s: %s", ean, e)
            return None

async def main():
    logger.info("Início da Execução - %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S %z"))
    
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        df = await get_from_api(client)
        if df is None or df.empty:
            logger.warning("Nenhum dado válido retornado ou DataFrame vazio")
            return None

        logger.info("Colunas do DataFrame: %s", df.columns.tolist())
        required_columns = ['url', 'ean', 'brand']
        if not all(col in df.columns for col in required_columns):
            logger.error("DataFrame não contém todas as colunas necessárias: %s", required_columns)
            return None

        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        results = []
        total_raspagens = len(df)
        raspagens_concluidas = 0
        
        for _, row in df.iterrows():
            result = await scrape_url(row, semaphore, client)
            if result is not None:
                raspagens_concluidas += 1
            results.append(result)
        
        logger.info("Raspagens concluídas: %d de %d", raspagens_concluidas, total_raspagens)

    logger.info("Fim da Execução - %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S %z"))
    return df

if __name__ == "__main__":
    df = asyncio.run(main())
