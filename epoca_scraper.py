import asyncio
import json
import re
from datetime import datetime
from urllib.parse import unquote, urlparse, parse_qs
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

async def process_product(product, idx, ean, marca, context, semaphore):
    async with semaphore:
        print(f"[Época] Processando produto {idx+1}")
        try:
            nome_el = await product.query_selector(".name")
            nome = (await nome_el.inner_text()).strip() if nome_el else ""

            link_el = await product.query_selector('a[data-content-item="true"]')
            link = await link_el.get_attribute("href") if link_el else ""
            if link and not link.startswith("http"):
                link = "https://www.epocacosmeticos.com.br" + link

            if "algorecs" in link:
                parsed_link = urlparse(link)
                query_params = parse_qs(parsed_link.query)
                if "ct" in query_params:
                    link = unquote(query_params["ct"][0])

            max_retries = 3
            retry_count = 0
            preco_final = '0'
            resultado = None

            while retry_count < max_retries and preco_final == '0':
                if retry_count > 0:
                    print(f"[Época] Preço final '0' detectado. Tentativa {retry_count + 1}/{max_retries} após 3 segundos.")
                    await asyncio.sleep(3)

                detail_page = await context.new_page()
                try:
                    await detail_page.goto(link, timeout=30000)
                    await detail_page.wait_for_load_state("domcontentloaded", timeout=30000)
                    await detail_page.wait_for_timeout(2000)

                    ean_html = None
                    ean_el = await detail_page.query_selector('div.pdp-buybox_referCodeEan__5mCsd')
                    if ean_el:
                        ean_text = await ean_el.inner_text()
                        match_ean = re.search(r'Ref:\s*(\d+)', ean_text)
                        if match_ean:
                            ean_html = match_ean.group(1)
                    if not ean_html or ean_html != ean:
                        print(f"[Época] EAN divergente ou não encontrado: {ean_html} (esperado: {ean})")
                        return None

                    jsonld_el = await detail_page.query_selector('script#jsonSchema')
                    json_data = {}
                    if jsonld_el:
                        try:
                            content = await jsonld_el.inner_text()
                            parsed = json.loads(content)
                            json_data = parsed[0] if isinstance(parsed, list) else parsed
                        except Exception:
                            pass

                    sku = json_data.get("sku", "")
                    preco_final = str(json_data.get("offers", {}).get("price", ""))
                    descricao_el = await detail_page.query_selector('p[data-product-title="true"]')
                    descricao = (await descricao_el.inner_text()).strip() if descricao_el else json_data.get("description", "")
                    imagem = json_data.get("image", "")
                    review = float(json_data.get("aggregateRating", {}).get("ratingValue", 0.0))
                    loja = json_data.get("offers", {}).get("seller", {}).get("name", "Época Cosméticos")

                    data_hora = datetime.utcnow().isoformat() + "Z"
                    status = "ativo"
                    marketplace = "Época Cosméticos"
                    key_loja = loja.lower().replace(" ", "")
                    key_sku = f"{key_loja}_{ean}" if sku else None
                    sku = f"{ean}_epoca_cosmeticos"

                    resultado = {
                        "ean": ean,
                        "url": link,
                        "sku": sku if sku else "SKU não encontrado",
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

                except PlaywrightTimeoutError as e:
                    print(f"[Época] Timeout ao processar produto {idx+1}: {e}")
                finally:
                    await detail_page.close()

                retry_count += 1

            if preco_final == '0':
                print(f"[Época] Erro: Preço final '0' após {max_retries} tentativas para o produto {idx+1}")
                return None

            print(resultado)
            return resultado

        except Exception as e:
            print(f"[Época] Erro ao processar produto {idx+1}: {e}")
            return None

async def epoca_scrap(ean, marca):
    url = f"https://www.epocacosmeticos.com.br/pesquisa?q={ean}"
    print(f"[Época] Iniciando raspagem para: {url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 480, "height": 300})
        semaphore = asyncio.Semaphore(5)  # Limite de 5 páginas simultâneas

        page = await context.new_page()
        try:
            await page.goto(url, timeout=30000)
            await page.wait_for_load_state("domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)

            produtos = await page.query_selector_all('div[data-testid="productItemComponent"]')
            print(f"[Época] {len(produtos)} produtos encontrados.")

            tasks = [
                process_product(produto, idx, ean, marca, context, semaphore)
                for idx, produto in enumerate(produtos)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            lojas = [result for result in results if result is not None]

            await page.close()
            await context.close()
            await browser.close()
            return lojas

        except PlaywrightTimeoutError as e:
            print(f"[Época] Timeout ao carregar a página inicial: {e}")
            await page.close()
            await context.close()
            await browser.close()
            return []

if __name__ == "__main__":
    asyncio.run(epoca_scrap('4064666318356', 'Wella'))
