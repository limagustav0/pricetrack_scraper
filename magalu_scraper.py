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

async def magalu_scrap(ean: str, marca: str, headless: bool):
    """Realiza o scraping de produtos da Magazine Luiza de forma discreta, retornando até 5 primeiros produtos encontrados na ordem original."""
    url_busca = f"https://www.magazineluiza.com.br/busca/{ean}"
    lojas = []

    async with async_playwright() as p:
        # Lança o navegador em modo headless
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": random.randint(1200, 1920), "height": random.randint(720, 1080)},
            java_script_enabled=True,
            bypass_csp=True,
            ignore_https_errors=True,
            extra_http_headers={
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Referer": "https://www.google.com/",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Cache-Control": "no-cache",
                "DNT": "1"
            }
        )
        await context.add_cookies([{
            "name": "cookieConsent",
            "value": "true",
            "domain": ".magazineluiza.com.br",
            "path": "/"
        }])

        page = await context.new_page()

        try:
            logger.info("[Magalu] Acessando a URL de busca: %s", url_busca)
            response = await page.goto(url_busca)
            if response:
                logger.info("[Magalu] Status da resposta: %d, Headers: %s", response.status, response.headers)
            if response and response.status == 429:
                logger.warning("[Magalu] Erro 429 Too Many Requests para EAN %s", ean)
                content = await page.content()
                logger.error("[Magalu] Conteúdo HTML da página: %s", content)
                await page.screenshot(path='magalu_timeout_screenshot.png', full_page=True)
                return lojas
            await page.wait_for_load_state("networkidle")
            await page.evaluate("window.scrollBy(0, Math.random() * 500 + 300)")
            await asyncio.sleep(random.uniform(2, 5))

            try:
                await page.wait_for_selector('div[data-testid="product-list"]', state='visible')
                logger.info("[Magalu] Bloco de resultados encontrado!")
            except TimeoutError as e:
                logger.error("[Magalu] Erro ao aguardar bloco de resultados: %s", e)
                content = await page.content()
                logger.error("[Magalu] Conteúdo HTML da página: %s", content)
                await page.screenshot(path='magalu_error_screenshot.png', full_page=True)
                return lojas

            jsonld_el = await page.query_selector('script[data-testid="jsonld-script"]')
            if not jsonld_el:
                logger.warning("[Magalu] JSON-LD não encontrado na página.")
                return lojas

            jsonld_raw = await jsonld_el.inner_text()
            jsonld_data = json.loads(jsonld_raw)
            products = jsonld_data.get("@graph", [])

            logger.info("[Magalu] Total de produtos encontrados no JSON-LD: %d", len(products))

            for idx, product in enumerate(products):
                if idx >= 5:
                    logger.info("[Magalu] Limite de 5 produtos atingido. Interrompendo processamento.")
                    break
                try:
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
                        response = await product_page.goto(produto_url)
                        if response and response.status == 429:
                            logger.warning("[Magalu] Erro 429 Too Many Requests ao acessar produto %d para EAN %s", idx + 1, ean)
                            await product_page.close()
                            loja = "Desconhecido"
                        else:
                            await product_page.wait_for_load_state("networkidle")
                            loja_element = await product_page.query_selector('label[data-testid="link"]')
                            loja = await loja_element.inner_text() if loja_element else "Desconhecido"
                            await product_page.close()
                    except Exception as e:
                        logger.warning("[Magalu] Erro ao extrair nome da loja para produto %d: %s", idx + 1, e)
                        loja = "Desconhecido"

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

            logger.info("[Magalu] Total de produtos extraídos: %d", len(lojas))
            # Removido o print dos produtos

        except TimeoutError as e:
            logger.error("[Magalu] Timeout ao carregar a página ou elementos: %s", e)
            content = await page.content()
            logger.error("[Magalu] Conteúdo HTML da página: %s", content)
        except Exception as e:
            logger.error("[Magalu] Erro geral: %s", e)
            content = await page.content()
            logger.error("[Magalu] Conteúdo HTML da página: %s", content)
        finally:
            await context.close()
            await browser.close()

    return lojas

lista_eans=[
    {"ean": "4064666318356", "brand": "Wella Professionals"},
    {"ean": "7896235353645", "brand": "Wella Professionals"},
    {"ean": "7896235353652", "brand": "Wella Professionals"},
]

async def main():
    for ean in lista_eans:
        await magalu_scrap(ean["ean"], ean["brand"], True)
        await asyncio.sleep(random.uniform(5, 10))  # Delay between requests

if __name__ == "__main__":
    asyncio.run(main())