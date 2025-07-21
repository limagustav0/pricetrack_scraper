import asyncio
import re
from datetime import datetime
from playwright.async_api import async_playwright


async def beleza_na_web_scrap(target_url :str, ean: str, marca:str):
    url = target_url
    print(f"[BNW] Iniciando raspagem para: {url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={"width":480, "height":300})
        page = await context.new_page()

        await page.goto(url)
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(3000)

        # Descrição (nome do produto)
        nome_el = await page.query_selector("h1.nproduct-title")
        nome = await nome_el.inner_text() if nome_el else ""
        nome = nome.strip().replace("\n", " ")

        # SKU
        sku_el = await page.query_selector("div.product-sku")
        sku = ""
        if sku_el:
            sku_text = await sku_el.inner_text()
            match = re.search(r"Cod:\s*(\d+)", sku_text)
            if match:
                sku = match.group(1)

        # Imagem principal
        imagem = ""
        img_el = await page.query_selector("img[src*='/product/']")
        if img_el:
            imagem = await img_el.get_attribute("src")

        # Reviews
        review = 0.0
        svg_title_el = await page.query_selector("svg.star title")
        if svg_title_el:
            review_text = await svg_title_el.evaluate("el => el.textContent")
            match = re.search(r"Review (\d+(?:\.\d+)?)", review_text)
            if match:
                review = float(match.group(1))

        # Lista de vendedores
        sellers = await page.query_selector_all("li.seller-list-item")
        print(f"[BNW] Vendedores encontrados: {len(sellers)}")

        lojas = []

        for idx, seller_el in enumerate(sellers):
            try:
                loja_el = await seller_el.query_selector(".buy-box-seller-ecomm-3p-link")
                loja = await loja_el.inner_text() if loja_el else "Beleza na Web"
                loja = loja.strip()

                preco_el = await seller_el.query_selector(".nproduct-price-value")
                preco_final = ""
                if preco_el:
                    preco_text = await preco_el.inner_text()
                    preco_final = preco_text.strip().replace("R$", "").strip().replace(".", "").replace(",", ".")

                data_hora = datetime.utcnow().isoformat() + "Z"
                status = "ativo"
                marketplace = "Beleza na Web"
                key_loja = loja.lower().replace(" ", "")
                key_sku = f"{key_loja}_{ean}" if ean else None

                resultado = {
                    "ean": ean,
                    "url": url,
                    "sku": sku if sku else "SKU não encontrado",
                    "descricao": nome,
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

                print(f"[BNW] Produto {idx+1}:")
                from pprint import pprint
                pprint(resultado)
                lojas.append(resultado)

            except Exception as e:
                print(f"[BNW] Erro ao processar vendedor {idx+1}: {e}")

        await context.close()
        await browser.close()
        return lojas

if __name__=="__main__":
    asyncio.run(beleza_na_web_scrap("https://www.belezanaweb.com.br/wella-professionals-invigo-color-brilliance-shampoo-1-litro/ofertas-marketplace",'4064666318356','Wella Professionals'))