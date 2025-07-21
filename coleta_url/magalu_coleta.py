import asyncio
import httpx
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

API_ENDPOINT = "http://127.0.0.1:8000/api/urls"

async def enviar_para_api(dados):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(API_ENDPOINT, json=[dados], timeout=10)
            print(f"[API] Enviado para API. Status: {response.status_code}")
            if response.status_code not in [200, 201]:
                print(f"[API] Erro na API: {response.text}")
            else:
                response_data = response.json()
                magalu_entry = next((item for item in response_data if item["ean_key"] == dados["ean_key"]), None)
                if magalu_entry:
                    print(f"[API] Entrada Magalu encontrada: {magalu_entry}")
                else:
                    print("[API] Entrada Magalu não encontrada na resposta.")
        except httpx.RequestError as e:
            print(f"[API] Erro ao conectar com a API: {e}")

async def coleta_magalu(ean: str, cliente: str = "Magazine Luiza") -> dict:
    url_busca = f"https://www.magazineluiza.com.br/busca/{ean}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--enable-gpu",
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled"
            ]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            locale="pt-BR",
            viewport={"width": 1280, "height": 720}
        )

        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)

        page = await context.new_page()

        try:
            await page.goto(url_busca, timeout=30000)
            await page.wait_for_selector('a[data-testid="product-card-container"]', timeout=15000)

            primeiro_produto = await page.query_selector('a[data-testid="product-card-container"]')
            if not primeiro_produto:
                result = {"ean_key": ean + "magalu", "ean": ean, "url": None, "brand": None, "client_name": cliente}
                return result

            href = await primeiro_produto.get_attribute("href")
            url_produto = f"https://www.magazineluiza.com.br{href}"

            await primeiro_produto.click()
            await page.wait_for_load_state('domcontentloaded')
            await page.wait_for_selector('a[data-testid="link"][href^="/marcas/"]', timeout=10000)

            marca_el = await page.query_selector('a[data-testid="link"][href^="/marcas/"]')
            marca = await marca_el.inner_text() if marca_el else None

            result = {
                "ean_key": ean + "magalu",
                "ean": ean,
                "url": url_busca,
                "brand": marca,
                "client_name": cliente
            }

            if result["url"]:
                print("[API] Enviando para API:")
                print(result)
                await enviar_para_api(result)

            return result

        except PlaywrightTimeoutError:
            result = {"ean_key": ean + "magalu", "ean": ean, "url": None, "brand": None, "client_name": cliente}
            return result

        finally:
            await browser.close()

# Exemplo de uso
if __name__ == "__main__":
    ean = "652418102137"
    asyncio.run(coleta_magalu(ean, cliente="Época Cosméticos"))
