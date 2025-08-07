import asyncio
import json
import os
import re
import random
import time
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError
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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36"
]

async def amazon_scrap(target_url: str, ean: str, marca: str,headless) -> list:
    """Realiza o scraping de produtos da Amazon de forma discreta, retornando os 10 primeiros produtos na ordem original."""
    logger.info("[Amazon] Iniciando raspagem para: %s", target_url)
    start_time = time.time()
    lojas = []
    storage_file = "amz_auth.json"

    if not os.path.exists(storage_file):
        logger.error("[Amazon] Arquivo de autenticação %s não encontrado.", storage_file)
        return lojas

    async with async_playwright() as p:
        # Lança o navegador em modo headless
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),  # Rotação de User-Agent
            viewport={"width": 1280, "height": 720},  # Aumentado para maior realismo
            java_script_enabled=True,
            bypass_csp=True,
            ignore_https_errors=True,
            extra_http_headers={
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Referer": "https://www.google.com/",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
        )
        # Habilitar cookies
        await context.add_cookies([{
            "name": "cookieConsent",
            "value": "true",
            "domain": ".amazon.com.br",
            "path": "/"
        }])

        page = await context.new_page()

        try:
            # Carregar cookies
            logger.info("[Amazon] Carregando cookies...")
            try:
                with open(storage_file, 'r') as f:
                    auth_data = json.load(f)
                    await context.add_cookies(auth_data.get('cookies', []))
                logger.info("[Amazon] Cookies carregados.")
            except Exception as e:
                logger.error("[Amazon] Erro ao carregar cookies: %s", e)
                return lojas

            # Navegar para a URL com retry e backoff
            logger.info("[Amazon] Navegando para %s", target_url)
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = await page.goto(target_url, timeout=30000)
                    if response and response.status != 200:
                        logger.warning("[Amazon] Falha ao carregar página %s. Status: %d", target_url, response.status)
                        return lojas
                    await page.wait_for_load_state('domcontentloaded', timeout=15000)
                    # Simular comportamento humano: rolagem e espera aleatória
                    await page.evaluate("window.scrollBy(0, 500)")
                    await asyncio.sleep(random.uniform(1, 3))  # Delay aleatório
                    logger.info("[Amazon] Página carregada.")
                    break
                except TimeoutError as e:
                    logger.warning("[Amazon] Timeout ao carregar página (tentativa %d/%d): %s", attempt + 1, max_retries, e)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(random.uniform(2, 5))  # Backoff aleatório
                    else:
                        logger.error("[Amazon] Falha após %d tentativas para %s", max_retries, target_url)
                        content = await page.content()
                        logger.error("[Amazon] Conteúdo HTML da página: %s", content[:1000])
                        await page.screenshot(path='amazon_timeout_screenshot.png', full_page=True)
                        return lojas

            # Simular rolagem adicional para carregar conteúdo dinâmico
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(random.uniform(1, 2))

            # Extrair SKU
            sku = "SKU não encontrado"
            try:
                match = re.search(r'/dp/([A-Z0-9]{10})', target_url)
                if match:
                    sku = match.group(1)
                logger.info("[Amazon] SKU extraído: %s", sku)
            except Exception as e:
                logger.error("[Amazon] Erro ao extrair SKU: %s", e)

            # Funções para extração concorrente
            async def get_description():
                try:
                    await page.wait_for_selector('#productTitle', timeout=7000)
                    return (await page.locator('#productTitle').first.inner_text()).strip()
                except Exception as e:
                    logger.error("[Amazon] Erro ao extrair descrição: %s", e)
                    return "Descrição não encontrada"

            async def get_image():
                try:
                    await page.wait_for_selector('#landingImage', timeout=7000)
                    return await page.locator('#landingImage').first.get_attribute('src')
                except Exception as e:
                    logger.error("[Amazon] Erro ao extrair imagem: %s", e)
                    return "Imagem não encontrada"

            async def get_review():
                try:
                    review_span = page.locator('a.a-popover-trigger span[aria-hidden="true"]').first
                    review_text = (await review_span.inner_text(timeout=7000)).strip()
                    logger.info("[Amazon] Texto da review capturado: '%s'", review_text)
                    if review_text and re.match(r'^\d+\.\d$', review_text.replace(',', '.')):
                        return float(review_text.replace(',', '.'))
                    logger.info("[Amazon] Review não encontrada ou inválida, usando padrão 4.5")
                    return 4.5
                except Exception as e:
                    logger.error("[Amazon] Erro ao extrair review: %s", e)
                    return 4.5

            # Executar extração concorrente
            descricao, imagem, review = await asyncio.gather(
                get_description(),
                get_image(),
                get_review()
            )
            logger.info("[Amazon] Descrição: %s, Imagem: %s, Review: %s", descricao, imagem, review)

            # Extrair vendedor principal e preço
            logger.info("[Amazon] Extraindo vendedor principal e preço...")
            seller_name = "Não informado"
            preco_final = 0.0
            try:
                seller = page.locator("#sellerProfileTriggerId").first
                seller_name = (await seller.inner_text(timeout=7000)).strip()
                seller_name = re.sub(r'Vendido por\s*', '', seller_name).strip()

                price_span = page.locator('div.a-section.a-spacing-micro span.a-offscreen').first
                price_text = re.sub(r'[^\d,.]', '', (await price_span.inner_text(timeout=7000)).strip()).replace(',', '.')
                if re.match(r'^\d+\.\d+$', price_text):
                    preco_final = float(price_text)
                else:
                    logger.warning("[Amazon] Preço inválido na página principal: %s", price_text)

                if seller_name != "Não informado" and preco_final > 0.0:
                    key_loja = seller_name.lower().replace(' ', '')
                    key_ean = f"{key_loja}_{ean}" if sku != "SKU não encontrado" else f"{key_loja}_sem_sku"
                    lojas.append({
                        'sku': sku,
                        'loja': seller_name,
                        'preco_final': preco_final,
                        'data_hora': datetime.utcnow().isoformat() + 'Z',
                        'marketplace': 'Amazon',
                        'key_loja': key_loja,
                        'key_sku': key_ean,
                        'descricao': descricao,
                        'review': review,
                        'imagem': imagem,
                        'status': 'ativo',
                        "url": target_url,
                        "ean": ean,
                        "marca": marca
                    })
                    logger.info("[Amazon] Vendedor principal capturado: %s, Preço: %s", seller_name, preco_final)
            except Exception as e:
                logger.error("[Amazon] Erro ao extrair vendedor/preço da página principal: %s", e)

            # Limitar a 10 produtos no total
            if len(lojas) >= 10:
                logger.info("[Amazon] Limite de 10 produtos atingido após vendedor principal.")
            else:
                # Acessar página de ofertas
                try:
                    compare_button = page.get_by_role("button", name=re.compile("Comparar outras.*ofertas|Ver todas as ofertas"))
                    await compare_button.wait_for(state='visible', timeout=10000)
                    logger.info("[Amazon] Botão de comparação encontrado")
                    # Simular movimento de mouse antes do clique
                    await compare_button.hover()
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    await compare_button.click(timeout=10000)
                    await asyncio.sleep(random.uniform(1, 2))
                    logger.info("[Amazon] Após clicar no botão de comparação: %.2f segundos", time.time() - start_time)

                    details_link = page.get_by_role("link", name="Ver mais detalhes sobre esta")
                    await details_link.wait_for(state='visible', timeout=10000)
                    logger.info("[Amazon] Link 'Ver mais detalhes' encontrado")
                    await details_link.hover()
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    await details_link.click(timeout=10000)
                    await asyncio.sleep(random.uniform(1, 2))
                    logger.info("[Amazon] Após clicar no link de detalhes: %.2f segundos", time.time() - start_time)

                    await page.wait_for_load_state('domcontentloaded', timeout=15000)
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1, 2))
                    logger.info("[Amazon] Após carregar página de ofertas: %.2f segundos", time.time() - start_time)
                except Exception as e:
                    logger.error("[Amazon] Erro ao acessar página de ofertas: %s", e)
                    content = await page.content()
                    logger.error("[Amazon] Conteúdo HTML da página: %s", content[:1000])
                    return lojas

                # Extrair ofertas
                try:
                    await page.wait_for_selector("#aod-offer", timeout=10000)
                    offer_elements = await page.locator("#aod-offer").all()
                    logger.info("[Amazon] Encontradas %d ofertas", len(offer_elements))
                    for i, offer in enumerate(offer_elements, 1):
                        if len(lojas) >= 10:  # Limita aos 10 primeiros produtos
                            logger.info("[Amazon] Limite de 10 produtos atingido.")
                            break
                        try:
                            preco_final = 0.0
                            try:
                                price_span = offer.locator('span.aok-offscreen').first
                                price_text = re.sub(r'[^\d,.]', '', (await price_span.inner_text(timeout=5000)).strip()).replace(',', '.')
                                if re.match(r'^\d+\.\d+$', price_text):
                                    preco_final = float(price_text)
                                else:
                                    logger.warning("[Amazon] Preço inválido na oferta %d: %s", i, price_text)
                            except Exception:
                                try:
                                    price_whole = (await offer.locator("span.a-price-whole").first.inner_text(timeout=5000)).strip()
                                    price_fraction = (await offer.locator("span.a-price-fraction").first.inner_text(timeout=5000)).strip()
                                    price_text = f"{re.sub(r'[^\d]', '', price_whole)}.{price_fraction}"
                                    if re.match(r'^\d+\.\d+$', price_text):
                                        preco_final = float(price_text)
                                    else:
                                        logger.warning("[Amazon] Preço inválido na oferta %d (fallback): %s", i, price_text)
                                except Exception as e:
                                    logger.error("[Amazon] Erro ao extrair preço na oferta %d: %s", i, e)
                                    continue

                            seller_name = "Não informado"
                            try:
                                seller = offer.locator("a.a-size-small.a-link-normal").first
                                seller_name = (await seller.inner_text(timeout=5000)).strip()
                                seller_name = re.sub(r'Vendido por\s*', '', seller_name).strip()
                            except Exception as e:
                                logger.error("[Amazon] Erro ao extrair vendedor na oferta %d: %s", i, e)
                                continue

                            if any(s['loja'] == seller_name for s in lojas):
                                logger.info("[Amazon] Vendedor %s já capturado, ignorando duplicata", seller_name)
                                continue

                            key_loja = seller_name.lower().replace(' ', '')
                            key_sku = f"{key_loja}_{sku}" if sku != "SKU não encontrado" else f"{key_loja}_sem_sku"
                            lojas.append({
                                'sku': sku,
                                'loja': seller_name,
                                'preco_final': preco_final,
                                'data_hora': datetime.utcnow().isoformat() + 'Z',
                                'marketplace': 'Amazon',
                                'key_loja': key_loja,
                                'key_sku': key_sku,
                                'descricao': descricao,
                                'review': review,
                                'imagem': imagem,
                                'status': 'ativo',
                                "url": target_url,
                                "ean": ean,
                                "marca": marca
                            })
                            logger.info("[Amazon] Oferta %d capturada: %s, Preço: %s", i, seller_name, preco_final)
                        except Exception as e:
                            logger.error("[Amazon] Erro ao processar oferta %d: %s", i, e)
                            continue
                except Exception as e:
                    logger.error("[Amazon] Erro ao extrair ofertas: %s", e)
                    content = await page.content()
                    logger.error("[Amazon] Conteúdo HTML da página: %s", content[:1000])

        except Exception as e:
            logger.error("[Amazon] Erro geral: %s", e)
            content = await page.content()
            logger.error("[Amazon] Conteúdo HTML da página: %s", content[:1000])
            await page.screenshot(path='amazon_error_screenshot.png', full_page=True)
        finally:
            await context.storage_state(path="amz_auth.json")
            await context.close()
            await browser.close()
            logger.info("[Amazon] Raspagem finalizada para: %s", target_url)

    end_time = time.time()
    execution_time = end_time - start_time
    logger.info("[Amazon] Tempo de execução: %.2f segundos", execution_time)
    logger.info("[Amazon] Produtos capturados: %s", lojas)
    return lojas

if __name__ == "__main__":
    async def run_scraper():
        target_url = "https://www.amazon.com.br/Wella-3922-Shampoo-1000Ml-Brilliance/dp/B0C3MHGZXP/ref=sr_1_1"
        ean = "4064666318356"
        marca = "Wella"
        results = await amazon_scrap(target_url, ean, marca,True)

    asyncio.run(run_scraper())