import asyncio
import json
from datetime import datetime
from pprint import pprint
from playwright.async_api import async_playwright, TimeoutError

async def magalu_scrap(target_url: str, ean: str, marca: str):
    url_busca = target_url
    lojas = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # Pode tentar headless=True
        context = await browser.new_context(viewport={"width": 800, "height": 600})
        page = await context.new_page()

        try:
            await page.goto(url_busca, timeout=30000)
            # Aguarda o seletor do bloco ul[data-testid="list"]
            await page.wait_for_selector('ul[data-testid="list"]', timeout=30000)

            # Seleciona apenas os produtos dentro de ul[data-testid="list"] > li.sc-kaaGRQ
            produtos = await page.query_selector_all('ul[data-testid="list"] li.sc-kaaGRQ a[data-testid="product-card-container"]')

            # Conta o número total de produtos no bloco
            print(f"Total de produtos encontrados no bloco ul[data-testid=\"list\"]: {len(produtos)}")

            for idx, produto in enumerate(produtos):
                try:
                    # Link do produto
                    href = await produto.get_attribute("href")
                    produto_url = f"https://www.magazineluiza.com.br{href}"

                    # Abre página de detalhe do produto
                    detail_page = await context.new_page()
                    await detail_page.goto(produto_url)
                    await detail_page.wait_for_load_state("domcontentloaded")
                    await detail_page.wait_for_timeout(1500)

                    # Descrição do produto
                    desc_el = await detail_page.query_selector('h1[data-testid="heading-product-title"]')
                    descricao = (await desc_el.inner_text()).strip() if desc_el else ""

                    # Seller / loja
                    loja_el = await detail_page.query_selector('label[data-testid="link"]')
                    loja = (await loja_el.inner_text()).strip() if loja_el else "Desconhecido"

                    # Marketplace fixo
                    marketplace = "Magazine Luiza"

                    # Dados JSON-LD para preço, sku, imagem, review
                    jsonld_el = await detail_page.query_selector('script[data-testid="jsonld-script"]')
                    jsonld = {}
                    if jsonld_el:
                        jsonld_raw = await jsonld_el.inner_text()
                        jsonld = json.loads(jsonld_raw)

                    preco_final = jsonld.get("offers", {}).get("price", "")
                    try:
                        preco_final = float(preco_final)  # Converte para float para ordenação
                    except (ValueError, TypeError):
                        preco_final = float('inf')  # Define como infinito se não for válido
                    imagem = jsonld.get("image", "")
                    try:
                        review = float(jsonld.get("aggregateRating", {}).get("ratingValue", 0))
                    except:
                        review = 0.0
                    sku = jsonld.get("sku", "")

                    # Dados auxiliares
                    data_hora = datetime.utcnow().isoformat() + "Z"
                    status = "ativo"
                    key_loja = loja.lower().replace(" ", "")
                    key_ean = f"{key_loja}_{ean}" if sku else None

                    resultado = {
                        "ean": ean,
                        "url": produto_url,
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
                        "key_sku": key_ean,
                        "marca": marca # Trunca para evitar erro de validação
                    }

                    lojas.append(resultado)
                    await detail_page.close()

                except Exception as e:
                    print(f"Erro ao processar produto {idx}: {e}")

            # Ordena os produtos por preço (do mais barato ao mais caro)
            lojas_ordenadas = sorted(lojas, key=lambda x: x["preco_final"])
            # Retorna apenas os 5 primeiros
            primeiros_cinco = lojas_ordenadas[:5]

            print("\nOs 5 primeiros produtos ordenados por preço (mais barato ao mais caro):")
            for produto in primeiros_cinco:
                pprint(produto)

        except TimeoutError:
            print("Timeout ao carregar a página ou elementos. Verifique o seletor ou a conexão.")
        except Exception as e:
            print(f"Erro geral: {e}")
        finally:
            await context.close()
            await browser.close()

    return lojas

if __name__ == "__main__":
    asyncio.run(magalu_scrap('https://www.magazineluiza.com.br/busca/4064666318356', '4064666318356', 'Wella Professionals'))
