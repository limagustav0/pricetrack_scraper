import asyncio
import httpx
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

API_ENDPOINT = "http://201.23.64.234:8000/api/urls"


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

lista_magalu = [
    '7792256767402',  # Cadiveu Glamour Rubi Shampoo+Condicionador 3L
    '1007931298970',  # Kit Wella Oil Reflections - Shampoo 1000 ml + Máscara 500 ml
    '78950000533',    # Kit Wella Professionals Invigo Nutri Enrich - Shampoo 1000 ml + Condicionador 1000 ml + Máscara 150 ml
    '1004108613628',  # Kit Wella Oil Reflections - Shampoo 250 ml + Condicionador 200 ml
    '7792255626304',  # Kit Braé Essential Shampoo + Condicionador + Máscara + Leave-in + Ampola Capilar (5 produtos)
    '7798448743193',  # Kit Truss Ultra Hydration Plus Duo (3 produtos)
    '1009156004237',  # Kit de Cronograma Capilar Wella Professionals Profissional - 3 Produtos
    '7795001511893',  # Kit L'Oréal Professionnel Absolut Repair Gold Shampoo Válvula Pump (2 produtos)
    '7792255576432',  # Kit Skelt Amalfi Sunset Hidratante Corporal e Spray Perfumado (2 produtos)
    '1004984939294',  # Kit Wella Professionals Fusion - Shampoo 1000 ml + Condicionador 1000 ml + Máscara 500 ml
    '78950000603',    # Kit Wella Professionals Invigo Nutri Enrich Shampoo 1000 ml - 2 Unidades
    '7907093722648',  # Wella Professionals Kit Fusion Salon Duo de 1L
    '7792254325208',  # Kit Redken All Soft Shampoo Litro e Máscara G (2 produtos)
    '7893697508968',  # Mp324990 Kit Pro Longer Shampoo Mascara E Leave In Loreal
    '7792256847593',  # Kit Keune Care Keratin Smooth Duo (2 produtos)
    '78950000534',    # Kit Wella Professionals Shampoo Oil Reflections 1000 ml - 2 Unidades
    '1003008872234',  # Kit de Cronograma Capilar Wella Home Care - 3 Produtos
    '7795000320748',  # Kit Braé Stages Nutrition Home Care (3 produtos)
    '7792256267216',  # Kit Braé Divine Shampoo e Condicionador (2 produtos)
    '7898759913855',  # Kit L'Oréal Professionnel Serie Expert Vitamino Color – Shampoo 1500 ml + Condicionador 1500 ml
    '78950000612',    # Kit Wella Professionals Fusion Shampoo 1000 ml - 2 Un.
    '7792255216758',  # Wella Professionals Oil Reflections Luminous Reboost Restaure - Máscara Capilar 500ml
    '1001465472370',  # Kit Wella Oil Reflections - Shampoo 250 ml + Máscara 150 ml
    '7794354285123',  # Truss Professional Night Spa Serum 250ml
    '7893790609996',  # Leave-in Serie Expert Pro Longer 150ml - L'oreal
    '7907093733408',  # Kit Truss Equilibrium Duo 300ml (2 Produtos)
    '7792253994795',  # Kit Cadiveu Professional Nutri Glow Duo Grande (2 produtos)
    '7792257688652',  # Kit Senscience Inner Restore Máscara G e True Hue Óleo (2 produtos)
    '7798447882794',  # Truss Equilibrium Shampoo 300ml 2 Unidades
    '3616304175916',  # Gucci Guilty Pour Femme Perfume Feminino - Elixir de Parfum 60ml
    '737052925028',   # Bamboo Gucci Eau De Parfum Perfume Feminino 30ml
    '7792255275915',  # Brae Divine Anti Frizz Home Care Trio (3 Produtos)
    '7792255216277',  # Wella Professionals Fusion Intense Repair - Shampoo 1L
    '7792255216130',  # Wella Invigo Color Brilliance Proteção da Cor - Shampoo 1L
    '3616305267108',  # Kit Gucci Guilty - Eau de Parfum 90ml + Body Lotion 50ml + Travel Size 10ml Kit
    '7792255635894',  # Pote Para Alimentos com 2 Divisórias Inteligentes
    '896364002749',   # Olaplex No 3 Hair Perfector Tratamento Para Coloracao 100ml
    '7794353994545',  # Sebastian Dark Oil Kit Shampoo 1 Litro + Condicionador 1 Litro
    '74469483575',    # Senscience Inner Restore Mascara Capilar 500ml
    '7895893115039',  # Brae Essential Hair Repair Spray 260ml (2 Unidades)
    '7792111462589',  # Mp262207 Truss Amino Liponutriente 225ml
]

# Exemplo de uso
if __name__ == "__main__":
    cliente = "Magazine Luiza"
    for ean in lista_magalu:
        asyncio.run(coleta_magalu(ean, cliente))
