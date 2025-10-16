import asyncio
import json
import re
import random
from datetime import datetime
from urllib.parse import unquote, urlparse, parse_qs
from playwright.async_api import async_playwright
import logging
from otel.trace import tracer
# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lista de User-Agents para rotação
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0"
]

async def epoca_scrap(ean, marca, headless: bool):
    """Realiza o scraping de produtos da Época Cosméticos de forma discreta."""
    url = f"https://www.epocacosmeticos.com.br/pesquisa?q={ean}"
    logger.info("[Época] Iniciando raspagem para: %s", url)

    async with async_playwright() as p:
        # Lançar navegador em modo headless
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),  # Rotação de User-Agent
            viewport={"width": 1280, "height": 720},
            java_script_enabled=True,
            bypass_csp=True,
            ignore_https_errors=True,
            extra_http_headers={
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Referer": "https://www.google.com/",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive"
            }
        )
        # Habilitar cookies
        await context.add_cookies([{
            "name": "cookieConsent",
            "value": "true",
            "domain": ".epocacosmeticos.com.br",
            "path": "/"
        }])

        page = await context.new_page()

        # Acessar a página com retry e backoff
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await page.goto(url, timeout=30000)
                await page.wait_for_load_state("domcontentloaded")
                # Simular comportamento humano: rolagem e espera aleatória
                await page.evaluate("window.scrollBy(0, 500)")
                await asyncio.sleep(random.uniform(2, 4))  # Delay aleatório
                break
            except Exception as e:
                logger.warning("[Época] Erro ao carregar página (tentativa %d/%d): %s", attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(2, 5))  # Backoff aleatório
                else:
                    logger.error("[Época] Falha após %d tentativas para EAN %s", max_retries, ean)
                    await context.close()
                    await browser.close()
                    return []

        # Simular rolagem adicional para carregar conteúdo dinâmico
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(1, 3))

        # Extrair produtos
        produtos = await page.query_selector_all('div[data-testid="productItemComponent"]')
        logger.info("[Época] %d produtos encontrados.", len(produtos))

        lojas = []
        for idx, produto in enumerate(produtos):
            try:
                logger.info("[Época] Processando produto %d/%d", idx + 1, len(produtos))

                # Extrair nome
                nome_el = await produto.query_selector(".name")
                nome = (await nome_el.inner_text()).strip() if nome_el else ""

                # Extrair link
                link_el = await produto.query_selector('a[data-content-item="true"]')
                link = await link_el.get_attribute("href") if link_el else ""
                if link and not link.startswith("http"):
                    link = "https://www.epocacosmeticos.com.br" + link

                # Corrigir URL redirecionada (RichRelevance, algorecs, etc)
                if "recs-p-chi" in link or "algorecs" in link or "richrelevance" in link:
                    parsed_link = urlparse(link)
                    query_params = parse_qs(parsed_link.query)
                    
                    # Tentar extrair a URL real do parâmetro 'ct'
                    if "ct" in query_params:
                        decoded_url = unquote(query_params["ct"][0])
                        link = decoded_url
                        logger.info("[Época] URL corrigida de redirect: %s", link)
                    else:
                        logger.warning("[Época] URL de redirect sem parâmetro 'ct': %s", link)

                # Garantir que a URL está completa
                if link and not link.startswith("http"):
                    link = "https://www.epocacosmeticos.com.br" + link

                logger.info("[Época] Link final extraído: %s", link)

                # Verificar EAN na página de detalhes
                detail = await context.new_page()
                try:
                    await detail.goto(link, timeout=30000)
                    await detail.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(random.uniform(1, 2))  # Delay aleatório

                    ean_html = None
                    ean_el = await detail.query_selector('div.pdp-buybox_referCodeEan__5mCsd')
                    if ean_el:
                        ean_text = await ean_el.inner_text()
                        match_ean = re.search(r'Ref:\s*(\d+)', ean_text)
                        if match_ean:
                            ean_html = match_ean.group(1)
                    
                    if not ean_html or ean_html != ean:
                        logger.info("[Época] EAN divergente ou não encontrado: %s (esperado: %s)", ean_html, ean)
                        continue

                    # Extrair dados com retry para preço
                    max_retries_price = 3
                    resultado = None

                    for retry_count in range(max_retries_price):
                        if retry_count > 0:
                            logger.info("[Época] Tentativa %d/%d de obter preço válido", retry_count + 1, max_retries_price)
                            await asyncio.sleep(random.uniform(2, 5))
                            await detail.reload()
                            await detail.wait_for_load_state("domcontentloaded")
                            await asyncio.sleep(random.uniform(1, 2))

                        # Extrair JSON-LD
                        jsonld_el = await detail.query_selector('script#jsonSchema')
                        json_data = {}
                        if jsonld_el:
                            try:
                                content = await jsonld_el.inner_text()
                                parsed = json.loads(content)
                                json_data = parsed[0] if isinstance(parsed, list) else parsed
                            except Exception as e:
                                logger.warning("[Época] Erro ao parsear JSON-LD: %s", e)

                        sku_original = json_data.get("sku", "")
                        preco_final = float(json_data.get("offers", {}).get("price", 0))
                        
                        # Se preço válido, processar e sair do loop
                        if preco_final > 0:
                            descricao_el = await detail.query_selector('p[data-product-title="true"]')
                            descricao = (await descricao_el.inner_text()).strip() if descricao_el else json_data.get("description", "")
                            imagem = json_data.get("image", "https://via.placeholder.com/150")
                            review = float(json_data.get("aggregateRating", {}).get("ratingValue", 0.0))
                            loja = json_data.get("offers", {}).get("seller", {}).get("name", "Época Cosméticos")

                            data_hora = datetime.utcnow().isoformat() + "Z"
                            status = "ativo"
                            marketplace = "Época Cosméticos"
                            key_loja = loja.lower().replace(" ", "")
                            sku = sku_original or f"{ean}_epoca_cosmeticos"
                            key_sku = f"{key_loja}_{ean}"

                            resultado = {
                                "ean": ean,
                                "url": link,
                                "sku": sku,
                                "descricao": descricao,
                                "loja": loja,
                                "preco_final": preco_final,
                                "imagem": imagem,
                                "review": review,
                                "data_hora": data_hora,
                                "status": status,
                                "marketplace": marketplace,
                                "key_loja": key_loja,
                                "key_sku": key_sku,
                                "marca": marca
                            }

                            logger.info("[Época] Produto processado: %s", resultado)
                            lojas.append(resultado)
                            break
                        else:
                            logger.warning("[Época] Preço zero na tentativa %d/%d", retry_count + 1, max_retries_price)

                    if not resultado:
                        logger.warning("[Época] Erro: Preço final '0' após %d tentativas para o produto %d", max_retries_price, idx + 1)

                finally:
                    await detail.close()

            except Exception as e:
                logger.error("[Época] Erro ao processar produto %d: %s", idx + 1, e)

        await context.close()
        await browser.close()
        return lojas

if __name__ == "__main__":
    async def run_scraper():
        ean = "4064666318356"
        marca = "Wella"
        results = await epoca_scrap(ean, marca, True)
        for result in results:
            print(json.dumps(result, indent=2, ensure_ascii=False))

    asyncio.run(run_scraper())