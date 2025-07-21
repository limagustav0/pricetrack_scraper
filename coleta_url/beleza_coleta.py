import asyncio
import re
import httpx
from playwright.async_api import async_playwright
from pprint import pprint




API_ENDPOINT = "http://127.0.0.1:8000/api/urls"

async def enviar_para_api(dados):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(API_ENDPOINT, json=dados, timeout=10)
            print(f"Enviado para API. Status: {response.status_code}")
            if response.status_code != 201:
                print(f"Erro na API: {response.text}")
        except httpx.RequestError as e:
            print(f"Erro ao conectar com a API: {e}")

async def get_ean_and_url(ean, page, cliente, max_retries=3):
    search_url = f"https://www.belezanaweb.com.br/busca/?q={ean}"
    for tentativa in range(1, max_retries + 1):
        try:
            print(f"Tentativa {tentativa} para EAN: {ean}")
            await page.goto(search_url, timeout=15000)
            final_url = page.url

            marca_el = await page.query_selector("a[data-interaction*='type\": \"marca']")
            marca = await marca_el.inner_text() if marca_el else "Marca desconhecida"

            match = re.match(r"(https://www\.belezanaweb\.com\.br/[^\?]+)", final_url)
            if match:
                clean_url = match.group(1)
                ofertas_url = f"{clean_url}/ofertas-marketplace"

                return {
                    "ean_key": ean + "beleza",
                    "ean": ean,
                    "brand": marca,
                    "url": ofertas_url,
                    "client_name": cliente
                }

        except Exception as e:
            print(f"Erro na tentativa {tentativa} para EAN {ean}: {e}")
            await asyncio.sleep(1)  # Delay entre tentativas

    # Após todas as tentativas
    return {
        "ean_key": ean + "beleza",
        "ean": ean,
        "brand": None,
        "url": None,
        "client_name": cliente
    }

async def coleta_beleza(ean,cliente):
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        print(f"\nBuscando EAN: {ean}")
        data = await get_ean_and_url(ean, page, cliente)
        results.append(data)

        if data["url"]:
            print("Enviando para API:")
            pprint([data])
            await enviar_para_api([data])
        else:
            print("Produto não encontrado ou URL inválida.")
        await asyncio.sleep(1)  # Delay entre EANs

        await browser.close()

    print("\nRESULTADO FINAL:")
    pprint(results)

if __name__ == "__main__":
    cliente = "Época Cosméticos"
    asyncio.run(coleta_beleza("8005610672427",cliente))
