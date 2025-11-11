import asyncio
import httpx
import pandas as pd
from amazon_scraper import amazon_scrap
from beleza_scraper import beleza_na_web_scrap
from magalu_scraper import magalu_scrap
from epoca_scraper import epoca_scrap
from meli_scraper import mercadolivre_scrap
from decimal import Decimal
import logging
from datetime import datetime, timezone
import time
import random
from otel.trace import tracer

# Configura o logging (apenas console, sem arquivo)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURAÇÕES
# ============================================
URL = "https://pricetrack-api.onrender.com"
PRODUCTS_ENDPOINT = f"{URL}/api/products"
API_ENDPOINT = f"{URL}/api/urls/"

# Limites e timeouts
CONCURRENCY_LIMIT = 5
REQUEST_TIMEOUT = 30.0
MAX_RETRIES = 3
DELAY_BETWEEN_SCRAPES = 2


# ============================================
# FUNÇÕES AUXILIARES
# ============================================

async def get_from_api(client):
    """Obtém dados da API e retorna um DataFrame."""
    for attempt in range(MAX_RETRIES):
        try:
            logger.info("[API] Tentativa %d/%d - GET %s", attempt + 1, MAX_RETRIES, API_ENDPOINT)
            response = await client.get(API_ENDPOINT, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 429:
                wait_time = (2 ** attempt) * random.uniform(2, 5)
                logger.warning("[API] Rate limit atingido, aguardando %.2fs", wait_time)
                await asyncio.sleep(wait_time)
                continue
            
            if response.status_code != 200:
                logger.error("[API] Erro HTTP %d: %s", response.status_code, response.text[:200])
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(random.uniform(2, 5))
                    continue
                return None
            
            response_data = response.json()
            if not isinstance(response_data, list):
                logger.error("[API] Resposta não é uma lista: %s", type(response_data))
                return None
            
            df = pd.DataFrame(response_data)
            logger.info("[API] Obtidos %d registros", len(df))
            return df
            
        except httpx.TimeoutException:
            logger.error("[API] Timeout na tentativa %d/%d", attempt + 1, MAX_RETRIES)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(random.uniform(2, 5))
        except httpx.RequestError as e:
            logger.error("[API] Erro de requisição: %s", e)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(random.uniform(2, 5))
        except Exception as e:
            logger.error("[API] Erro inesperado: %s", e)
            return None
    
    logger.error("[API] Falha após %d tentativas", MAX_RETRIES)
    return None


async def post_to_products(products, client, ean):
    """Envia uma lista de produtos para o endpoint /products."""
    if not products:
        logger.warning("[POST] [EAN %s] Lista de produtos vazia, nada para enviar", ean)
        return None
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info("[POST] [EAN %s] Tentativa %d/%d - Enviando %d produtos", 
                       ean, attempt + 1, MAX_RETRIES, len(products))
            
            response = await client.post(
                PRODUCTS_ENDPOINT, 
                json=products, 
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 429:
                wait_time = (2 ** attempt) * random.uniform(2, 5)
                logger.warning("[POST] [EAN %s] Rate limit, aguardando %.2fs", ean, wait_time)
                await asyncio.sleep(wait_time)
                continue
            
            if response.status_code in [200, 201]:
                response_data = response.json()
                ativos = sum(1 for p in response_data if p.get('status') == 'ativo')
                inativos = sum(1 for p in response_data if p.get('status') == 'inativo')
                logger.info("[POST] [EAN %s] Enviado com sucesso: %d ativos, %d inativos", 
                           ean, ativos, inativos)
                return response_data
            
            logger.error("[POST] [EAN %s] Erro HTTP %d: %s", 
                        ean, response.status_code, response.text[:200])
            
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(random.uniform(2, 5))
                continue
            return None
            
        except httpx.TimeoutException:
            logger.error("[POST] [EAN %s] Timeout na tentativa %d/%d", ean, attempt + 1, MAX_RETRIES)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(random.uniform(2, 5))
        except httpx.RequestError as e:
            logger.error("[POST] [EAN %s] Erro de requisição: %s", ean, e)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(random.uniform(2, 5))
        except Exception as e:
            logger.error("[POST] [EAN %s] Erro inesperado: %s", ean, e)
            return None
    
    logger.error("[POST] [EAN %s] Falha após %d tentativas", ean, MAX_RETRIES)
    return None


async def scrape_url(row, semaphore, scrape_stats, client):
    """Executa os scrapers apropriados para um EAN e envia para a API."""
    async with semaphore:
        ean = row['ean']
        url = row['url']
        brand = row['brand']
        categoria = row.get('categoria', 'cosmetico')
        headless = True
        
        # Validações
        if not url or not isinstance(url, str):
            logger.warning("[EAN %s] URL inválida: %s", ean, url)
            scrape_stats[ean] = {"time": 0, "products": 0, "error": "URL inválida"}
            return None
        
        if not ean or not isinstance(ean, str) or len(ean) != 13:
            logger.warning("[URL %s] EAN inválido: %s", url, ean)
            scrape_stats[ean] = {"time": 0, "products": 0, "error": "EAN inválido"}
            return None
        
        logger.info("=" * 80)
        logger.info("[EAN %s] Iniciando scraping", ean)
        logger.info("[EAN %s] URL: %s", ean, url)
        logger.info("[EAN %s] Marca: %s", ean, brand)
        logger.info("=" * 80)
        
        start_time = time.time()
        all_results = []
        errors = []

        with tracer.start_as_current_span("scrape_url") as span_main:
            span_main.set_attribute("ean", ean)
            span_main.set_attribute("url", url)
            span_main.set_attribute("brand", brand)
            
            try:
                # AMAZON
                if "amazon" in url.lower():
                    with tracer.start_as_current_span("amazon_scraping") as span_child:
                        logger.info("[EAN %s] Iniciando scraping AMAZON", ean)
                        try:
                            amazon_result = await amazon_scrap(url, ean, brand, headless, categoria)
                            if amazon_result and isinstance(amazon_result, list):
                                all_results.extend(amazon_result)
                                logger.info("[EAN %s] Amazon: %d produtos coletados", ean, len(amazon_result))
                            else:
                                logger.warning("[EAN %s] Amazon: nenhum produto retornado", ean)
                        except Exception as e:
                            error_msg = f"Erro Amazon: {str(e)}"
                            errors.append(error_msg)
                            logger.error("[EAN %s] %s", ean, error_msg)
                            span_child.record_exception(e)
                
                elif "mercadolivre" in url.lower():
                    with tracer.start_as_current_span("meli_scraping") as span_child:
                        logger.info("[EAN %s] Iniciando scraping MERCADO LIVRE", ean)
                        try:
                            meli_result = await mercadolivre_scrap(url, ean, brand, headless, categoria)
                            if meli_result and isinstance(meli_result, list):
                                all_results.extend(meli_result)
                                logger.info("[EAN %s] Meli: %d produtos coletados", ean, len(meli_result))
                            else:
                                logger.warning("[EAN %s] Meli: nenhum produto retornado", ean)
                        except Exception as e:
                            error_msg = f"Erro Meli: {str(e)}"
                            errors.append(error_msg)
                            logger.error("[EAN %s] %s", ean, error_msg)
                            span_child.record_exception(e)

                # BELEZA NA WEB + ÉPOCA + MAGALU
                elif "belezanaweb" in url.lower():
                    logger.info("[EAN %s] Iniciando scraping BELEZA + ÉPOCA + MAGALU", ean)
                    
                    tasks = []
                    task_names = []
                    
                    with tracer.start_as_current_span("beleza_scraping"):
                        tasks.append(beleza_na_web_scrap(url, ean, brand, categoria))
                        task_names.append("Beleza na Web")
                    
                    with tracer.start_as_current_span("epoca_scraping"):
                        tasks.append(epoca_scrap(ean, brand, headless, categoria))
                        task_names.append("Época")
                    
                    with tracer.start_as_current_span("magalu_scraping"):
                        tasks.append(magalu_scrap(ean, brand, headless, categoria))
                        task_names.append("Magalu")
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for task_name, result in zip(task_names, results):
                        if isinstance(result, Exception):
                            error_msg = f"Erro {task_name}: {str(result)}"
                            errors.append(error_msg)
                            logger.error("[EAN %s] %s", ean, error_msg)
                        elif isinstance(result, list) and result:
                            all_results.extend(result)
                            logger.info("[EAN %s] %s: %d produtos coletados", ean, task_name, len(result))
                        else:
                            logger.warning("[EAN %s] %s: nenhum produto", ean, task_name)
                
                else:
                    error_msg = f"Marketplace não suportado: {url}"
                    errors.append(error_msg)
                    logger.error("[EAN %s] %s", ean, error_msg)
                    scrape_stats[ean] = {
                        "time": 0,
                        "products": 0,
                        "error": error_msg
                    }
                    return None
                
                # ENVIAR PARA A API
                if all_results:
                    logger.info("[EAN %s] Enviando %d produtos para a API...", ean, len(all_results))
                    api_response = await post_to_products(all_results, client, ean)
                    
                    if api_response:
                        logger.info("[EAN %s] Produtos enviados com sucesso", ean)
                    else:
                        error_msg = "Falha ao enviar produtos para API"
                        errors.append(error_msg)
                        logger.error("[EAN %s] %s", ean, error_msg)
                else:
                    logger.warning("[EAN %s] Nenhum produto para enviar", ean)
                
                # ESTATÍSTICAS
                elapsed_time = time.time() - start_time
                
                scrape_stats[ean] = {
                    "time": round(elapsed_time, 2),
                    "products": len(all_results),
                    "errors": errors if errors else None
                }
                
                logger.info("=" * 80)
                logger.info("[EAN %s] Scraping concluído", ean)
                logger.info("[EAN %s] Tempo: %.2fs", ean, elapsed_time)
                logger.info("[EAN %s] Produtos: %d", ean, len(all_results))
                if errors:
                    logger.info("[EAN %s] Erros: %d", ean, len(errors))
                logger.info("=" * 80)
                
                span_main.set_attribute("products_count", len(all_results))
                span_main.set_attribute("elapsed_time", elapsed_time)
                
                await asyncio.sleep(DELAY_BETWEEN_SCRAPES)
                
                return len(all_results)
                
            except Exception as e:
                elapsed_time = time.time() - start_time
                error_msg = f"Erro geral: {str(e)}"
                errors.append(error_msg)
                
                logger.error("[EAN %s] Erro fatal: %s", ean, e)
                span_main.record_exception(e)
                
                scrape_stats[ean] = {
                    "time": round(elapsed_time, 2),
                    "products": 0,
                    "error": error_msg
                }
                
                return None


# ============================================
# FUNÇÃO PRINCIPAL
# ============================================

async def main():
    """Orquestrador principal do sistema de scraping."""
    logger.info("=" * 80)
    logger.info("INICIANDO SISTEMA DE SCRAPING")
    logger.info("=" * 80)
    logger.info("Início: %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
    logger.info("Concorrência: %d", CONCURRENCY_LIMIT)
    logger.info("Timeout: %.1fs", REQUEST_TIMEOUT)
    logger.info("API Endpoint: %s", PRODUCTS_ENDPOINT)
    logger.info("=" * 80)
    
    start_total_time = time.time()
    scrape_stats = {}
    
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            df = await get_from_api(client)
            
            if df is None or df.empty:
                logger.error("Nenhum dado retornado da API")
                return None
            
            required_columns = ['url', 'ean', 'brand', 'is_active']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error("Colunas ausentes no DataFrame: %s", missing_columns)
                return None
            
            df_original_len = len(df)
            df = df[df['is_active'] == True].copy()
            logger.info("Total de URLs: %d", df_original_len)
            logger.info("URLs ativas: %d", len(df))
            logger.info("URLs inativas: %d", df_original_len - len(df))
            
            if df.empty:
                logger.warning("Nenhuma URL ativa para processar")
                return None
            
            semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
            total = len(df)
            concluido = 0
            total_products = 0
            
            logger.info("=" * 80)
            logger.info("PROCESSANDO %d URLs", total)
            logger.info("=" * 80)
            
            for idx, row in df.iterrows():
                logger.info("\n[%d/%d] Processando próximo EAN...", concluido + 1, total)
                result = await scrape_url(row, semaphore, scrape_stats, client)
                
                if result is not None:
                    concluido += 1
                    total_products += result
                
                progress_pct = (concluido / total) * 100
                logger.info("[PROGRESSO] %.1f%% concluído (%d/%d)", progress_pct, concluido, total)
            
            elapsed_total = time.time() - start_total_time
            
            logger.info("=" * 80)
            logger.info("SCRAPING FINALIZADO")
            logger.info("=" * 80)
            logger.info("Fim: %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
            logger.info("Tempo total: %.2fs (%.2f minutos)", elapsed_total, elapsed_total / 60)
            logger.info("URLs processadas: %d/%d (%.1f%%)", concluido, total, (concluido / total * 100) if total > 0 else 0)
            logger.info("Total de produtos: %d", total_products)
            logger.info("Média: %.2f produtos/URL", total_products / concluido if concluido > 0 else 0)
            logger.info("Tempo médio por URL: %.2fs", elapsed_total / total if total > 0 else 0)
            
            if scrape_stats:
                successful = sum(1 for stat in scrape_stats.values() if stat.get("error") is None)
                failed = len(scrape_stats) - successful
                
                logger.info("=" * 80)
                logger.info("ESTATÍSTICAS DETALHADAS")
                logger.info("=" * 80)
                logger.info("Sucessos: %d", successful)
                logger.info("Falhas: %d", failed)
                
                if failed > 0:
                    logger.info("\nERROS ENCONTRADOS:")
                    for ean, stat in scrape_stats.items():
                        if stat.get("error"):
                            logger.info("  - [EAN %s] %s", ean, stat["error"])
            
            logger.info("=" * 80)
            
            return df
        
    except KeyboardInterrupt:
        logger.warning("\nExecução interrompida pelo usuário")
        return None
    except Exception as e:
        logger.error("Erro fatal no orquestrador: %s", e)
        import traceback
        traceback.print_exc()
        return None


# ============================================
# EXECUÇÃO
# ============================================

async def run_continuously():
    """Executa o scraping a cada 1 hora, indefinidamente."""
    while True:
        try:
            logger.info("=" * 80)
            logger.info("INICIANDO NOVA EXECUÇÃO DO SCRAPING")
            logger.info("Horário: %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
            logger.info("=" * 80)

            await main()

            # Aguarda 1 hora (3600 segundos) antes da próxima execução
            logger.info("Aguardando 1 hora para a próxima execução...")
            await asyncio.sleep(3600)  # 60 minutos

        except KeyboardInterrupt:
            logger.info("\nExecução interrompida pelo usuário. Encerrando...")
            break
        except Exception as e:
            logger.error("Erro crítico na execução contínua: %s", e)
            import traceback
            traceback.print_exc()
            logger.info("Retomando em 1 hora apesar do erro...")
            await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(run_continuously())
    except KeyboardInterrupt:
        logger.info("\nPrograma encerrado pelo usuário.")
