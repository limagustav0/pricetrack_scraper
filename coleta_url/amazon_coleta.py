import asyncio
import httpx
import re
from urllib.parse import urlparse, urlunparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pprint import pprint

API_ENDPOINT = "http://201.23.64.234:8000/api/urls"

async def enviar_para_api(dados):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(API_ENDPOINT, json=[dados], timeout=10)
            print(f"[API] Enviado para API. Status: {response.status_code}")
            if response.status_code != 201:
                print(f"[API] Erro na API: {response.text}")
        except httpx.RequestError as e:
            print(f"[API] Erro ao conectar com a API: {e}")

async def buscar_produto_amazon(ean, descricao, cliente, is_kit=False, tentativas=4):
    url_busca = f"https://www.amazon.com.br/s?k={ean}"

    for tentativa in range(1, tentativas + 1):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                print(f"[Tentativa {tentativa}] Acessando: {url_busca}")
                await page.goto(url_busca, timeout=15000)
                await page.wait_for_selector("div.puis-card-container", timeout=10000)

                produtos = []
                cards = await page.query_selector_all("div.puis-card-container")

                for card in cards:
                    titulo_el = await card.query_selector("h2 span")
                    link_el = await card.query_selector("a.a-link-normal.s-no-outline")
                    preco_el = await card.query_selector(".a-price .a-offscreen")

                    titulo = await titulo_el.inner_text() if titulo_el else None
                    preco = await preco_el.inner_text() if preco_el else None
                    link = await link_el.get_attribute("href") if link_el else None
                    link = f"https://www.amazon.com.br{link}" if link else None

                    if titulo:
                        produtos.append({
                            "titulo": titulo.strip(),
                            "preco": preco,
                            "link": link,
                            "marca": None,  # Will be updated after clicking
                            "element": link_el
                        })

                filtrados = []
                for prod in produtos:
                    nome = prod["titulo"].lower()
                    tem_kit = "kit" in nome or "+" in nome

                    if is_kit and tem_kit:
                        filtrados.append(prod)
                    elif not is_kit and not tem_kit:
                        filtrados.append(prod)

                if not filtrados:
                    print("⚠️ Nenhum produto encontrado após o filtro.")
                    await browser.close()
                    return {
                        "ean_key": ean + "amazon",
                        "ean": ean,
                        "brand": None,
                        "url": None,
                        "client_name": cliente
                    }

                if len(filtrados) == 1:
                    selecionado = filtrados[0]
                else:
                    descricao_tokens = set(re.findall(r'\w+', descricao.lower()))
                    melhor_score = -1
                    melhor_produto = None

                    for prod in filtrados:
                        nome_tokens = set(re.findall(r'\w+', prod["titulo"].lower()))
                        score = len(descricao_tokens.intersection(nome_tokens))
                        if score > melhor_score:
                            melhor_score = score
                            melhor_produto = prod

                    selecionado = melhor_produto

                print(f"🖱️ Clicando em: {selecionado['titulo']}")
                final_url = selecionado["link"]

                if selecionado["element"]:
                    try:
                        await selecionado["element"].click()
                        await page.wait_for_load_state("load", timeout=15000)
                        full_url = page.url
                        parsed = urlparse(full_url)
                        clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
                        final_url = clean_url

                        # Extract brand from product details page
                        marca_el = await page.query_selector("td.a-span9 span.a-size-base.po-break-word")
                        marca = await marca_el.inner_text() if marca_el else "Marca desconhecida"
                    except Exception as e:
                        print(f"⚠️ Falha ao clicar ou extrair marca: {e}")
                        marca = "Marca desconhecida"

                await browser.close()

                return {
                    "ean_key": ean + "amazon",
                    "ean": ean,
                    "brand": marca,
                    "url": final_url,
                    "client_name": cliente
                }

        except PlaywrightTimeoutError:
            print(f"⏰ Timeout na tentativa {tentativa}")
        except Exception as e:
            print(f"❌ Erro inesperado: {e}")
        await asyncio.sleep(1)

    return {
        "ean_key": ean + "amazon",
        "ean": ean,
        "brand": None,
        "url": None,
        "client_name": cliente
    }

async def coleta_amazon(ean, descricao, cliente="Época Cosméticos", is_kit=False):
    print(f"\n🔎 Buscando produto: {descricao} (EAN: {ean})")
    
    produto = await buscar_produto_amazon(ean, descricao, cliente, is_kit)
    
    if produto["url"]:
        print("[API] Enviando para API:")
        pprint(produto)
        await enviar_para_api(produto)
    else:
        print("Produto não encontrado ou URL inválida.")
    
    print("\n🧾 RESULTADO FINAL:")
    pprint(produto)
    return produto

# Exemplo de uso
if __name__ == "__main__":
    ean = "4064666318356"
    descricao = "Wella Shampoo Color Brilliance 1L Cabelos Coloridos"
    asyncio.run(coleta_amazon(ean, descricao, cliente="Época Cosméticos", is_kit=False))