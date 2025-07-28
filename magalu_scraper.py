import asyncio
import json
import random
from datetime import datetime, timezone
from playwright.async_api import async_playwright, TimeoutError, Error
import logging

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

async def magalu_scrap(ean: str, marca: str):
    """Realiza o scraping de produtos da Magazine Luiza de forma discreta, retornando até 5 primeiros produtos encontrados na ordem original."""
    url_busca = f"https://www.magazineluiza.com.br/busca/{ean}"
    lojas = []

    async with async_playwright() as p:
        # Lança o navegador em modo headless
        browser = await p.chromium.launch(headless=True)
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
            "domain": ".magazineluiza.com.br",
            "path": "/"
        }])

        page = await context.new_page()

        try:
            logger.info("[Magalu] Acessando a URL de busca: %s", url_busca)
            # Acessar a página com retry e backoff para erros 429
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = await page.goto(url_busca, timeout=30000)
                    if response and response.status == 429:
                        wait_time = 2 ** attempt * random.uniform(2, 5)
                        logger.warning("[Magalu] Erro 429 Too Many Requests na tentativa %d/%d para EAN %s, esperando %.2f segundos", 
                                      attempt + 1, max_retries, ean, wait_time)
                        await asyncio.sleep(wait_time)
                        continue
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    # Simular comportamento humano: rolagem e espera aleatória
                    logger.info("[Magalu] Rolando a página para garantir renderização...")
                    await page.evaluate("window.scrollBy(0, 500)")
                    await asyncio.sleep(random.uniform(2, 4))  # Delay aleatório
                    break
                except TimeoutError as e:
                    logger.warning("[Magalu] Timeout ao carregar página (tentativa %d/%d): %s", attempt + 1, max_retries, e)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(random.uniform(2, 5))  # Backoff aleatório
                    else:
                        logger.error("[Magalu] Falha após %d tentativas para EAN %s", max_retries, ean)
                        content = await page.content()
                        logger.error("[Magalu] Conteúdo HTML da página: %s", content)
                        await page.screenshot(path='magalu_timeout_screenshot.png', full_page=True)
                        await context.close()
                        await browser.close()
                        return lojas
                except Error as e:
                    if "net::ERR_TOO_MANY_REQUESTS" in str(e):
                        wait_time = 2 ** attempt * random.uniform(2, 5)
                        logger.warning("[Magalu] Erro 429 Too Many Requests na tentativa %d/%d para EAN %s, esperando %.2f segundos", 
                                      attempt + 1, max_retries, ean, wait_time)
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error("[Magalu] Erro ao acessar a página: %s", e)
                    await context.close()
                    await browser.close()
                    return lojas

            # Simular rolagem adicional para carregar conteúdo dinâmico
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(random.uniform(1, 3))

            # Aguarda o seletor do bloco de resultados
            logger.info("[Magalu] Aguardando bloco de resultados...")
            try:
                await page.wait_for_selector('div[data-testid="product-list"]', state='visible', timeout=30000)
                logger.info("[Magalu] Bloco de resultados encontrado!")
            except TimeoutError as e:
                logger.error("[Magalu] Erro ao aguardar bloco de resultados: %s", e)
                content = await page.content()
                logger.error("[Magalu] Conteúdo HTML da página: %s", content)
                await page.screenshot(path='magalu_error_screenshot.png', full_page=True)
                await context.close()
                await browser.close()
                return lojas

            # Extrai o JSON-LD da página principal
            jsonld_el = await page.query_selector('script[data-testid="jsonld-script"]')
            if not jsonld_el:
                logger.warning("[Magalu] JSON-LD não encontrado na página.")
                await context.close()
                await browser.close()
                return lojas

            jsonld_raw = await jsonld_el.inner_text()
            jsonld_data = json.loads(jsonld_raw)
            products = jsonld_data.get("@graph", [])

            logger.info("[Magalu] Total de produtos encontrados no JSON-LD: %d", len(products))

            for idx, product in enumerate(products):
                if idx >= 5:  # Limita a 5 produtos
                    logger.info("[Magalu] Limite de 5 produtos atingido. Interrompendo processamento.")
                    break
                try:
                    # Extrai informações do JSON-LD
                    descricao = product.get("name", "Descrição não encontrada")
                    produto_url = product.get("offers", {}).get("url", "")
                    preco_final = product.get("offers", {}).get("price", "")
                    try:
                        preco_final = float(preco_final)
                    except (ValueError, TypeError):
                        logger.warning("[Magalu] Preço inválido para produto %d, definindo como infinito", idx + 1)
                        preco_final = float('inf')
                    imagem = product.get("image", "")
                    review = float(product.get("aggregateRating", {}).get("ratingValue", 0))
                    review_count = int(product.get("aggregateRating", {}).get("reviewCount", 0))
                    sku = product.get("sku", "SKU não encontrado")
                    brand = product.get("brand", marca)

                    try:
                        product_page = await context.new_page()
                        for attempt in range(max_retries):
                            try:
                                response = await product_page.goto(produto_url, timeout=30000)
                                if response and response.status == 429:
                                    wait_time = 2 ** attempt * random.uniform(2, 5)
                                    logger.warning("[Magalu] Erro 429 Too Many Requests ao acessar produto %d na tentativa %d/%d, esperando %.2f segundos", 
                                                  idx + 1, attempt + 1, max_retries, wait_time)
                                    await asyncio.sleep(wait_time)
                                    continue
                                await product_page.wait_for_load_state("networkidle", timeout=30000)
                                loja_element = await product_page.query_selector('label[data-testid="link"]')
                                loja = await loja_element.inner_text() if loja_element else "Desconhecido"
                                await product_page.close()
                                break
                            except TimeoutError as e:
                                logger.warning("[Magalu] Timeout ao acessar produto %d (tentativa %d/%d): %s", idx + 1, attempt + 1, max_retries, e)
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(random.uniform(2, 5))
                                else:
                                    logger.error("[Magalu] Falha após %d tentativas ao acessar produto %d", max_retries, idx + 1)
                                    loja = "Desconhecido"
                                    await product_page.close()
                                    break
                            except Error as e:
                                if "net::ERR_TOO_MANY_REQUESTS" in str(e):
                                    wait_time = 2 ** attempt * random.uniform(2, 5)
                                    logger.warning("[Magalu] Erro 429 Too Many Requests ao acessar produto %d na tentativa %d/%d, esperando %.2f segundos", 
                                                  idx + 1, attempt + 1, max_retries, wait_time)
                                    await asyncio.sleep(wait_time)
                                    continue
                                logger.warning("[Magalu] Erro ao acessar produto %d: %s", idx + 1, e)
                                loja = "Desconhecido"
                                await product_page.close()
                                break
                    except Exception as e:
                        logger.warning("[Magalu] Erro ao extrair nome da loja para produto %d: %s", idx + 1, e)
                        loja = "Desconhecido"

                    # Dados auxiliares
                    marketplace = "Magazine Luiza"
                    data_hora = datetime.now(timezone.utc).isoformat()
                    status = "ativo"
                    key_loja = loja.lower().replace(" ", "")
                    key_ean = f"{key_loja}_{ean}" if sku else None

                    resultado = {
                        "ean": ean,
                        "url": produto_url,
                        "sku": sku,
                        "descricao": descricao,
                        "loja": loja,
                        "preco_final": preco_final,
                        "imagem": imagem,
                        "review": review,
                        "review_count": review_count,
                        "data_hora": data_hora,
                        "status": status,
                        "marketplace": marketplace,
                        "key_loja": key_loja,
                        "key_sku": key_ean,
                        "marca": brand
                    }

                    lojas.append(resultado)
                    logger.info("[Magalu] Produto %d extraído: %s", idx + 1, descricao)

                except Exception as e:
                    logger.error("[Magalu] Erro ao processar produto %d: %s", idx + 1, e)
                    continue

            # Retorna até 5 produtos
            logger.info("[Magalu] Total de produtos extraídos: %d", len(lojas))
            for produto in lojas:
                print(json.dumps(produto, indent=2, ensure_ascii=False))

        except TimeoutError as e:
            logger.error("[Magalu] Timeout ao carregar a página ou elementos: %s", e)
            content = await page.content()
            logger.error("[Magalu] Conteúdo HTML da página: %s", content)
            await page.screenshot(path='magalu_timeout_screenshot.png', full_page=True)
        except Exception as e:
            logger.error("[Magalu] Erro geral: %s", e)
            content = await page.content()
            logger.error("[Magalu] Conteúdo HTML da página: %s", content)
            await page.screenshot(path='magalu_error_screenshot.png', full_page=True)
        finally:
            await context.close()
            await browser.close()

    return lojas