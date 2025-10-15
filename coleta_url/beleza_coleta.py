import asyncio
import re
import httpx
from playwright.async_api import async_playwright
from pprint import pprint




API_ENDPOINT = "http://201.23.64.234:8000/api/urls"


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
            await page.goto(search_url)
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
        page = await browser.new_page(viewport={'width': 300, 'height': 300})

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

produtos = [
    "7898724572612",
    "3616302038633",
    "4064666318356",
    "7896235353645",
    "7896235353652",
    "4064666435459",
    "6291108735411",
    "3349668566372",
    "7896235353751",
    "7896235353775",
    "7898947943848",
    "6291107456041",
    "3614226403056",
    "7898667820726",
    "6291108730515",
    "5055810013110",
    "7896235353812",
    "4064666210131",
    "7896235353706",
    "8005610415871",
    "6085010044712",
    "4064666318233",
    "3474636977369",
    "8005610531632",
    "7899706189620",
    "3349666007921",
    "6291107456058",
    "6290360593166",
    "7898965429645",
    "7899706189941",
    "7896235353720",
    "7899706196666",
    "6297001158432",
    "7896835805797",
    "3349668589678",
    "3337875543248",
    "3349668594412",
    "30158078",
    "3701129800096",
    "7896235353713",
    "7898556756303",
    "7792256767402",
    "7898454151583",
    "7792252353500",
    "7907093652839",
    "1007931298970",
    "78950000533",
    "1004108613628",
    "7898973417023",
    "7792255626304",
    "7898667820160",
    "4064666306179",
    "7798448743193",
    "1009156004237",
    "7795001511893",
    "7792255576432",
    "1004984939294",
    "1006624920518",
    "7702045446616",
    "7500435132534",
    "1006800734823",
    "7907093722648",
    "8435415055956",
    "7795000320748",
    "6012512766736",
    "6291107459196",
    "652418102281",
    "78950000534",
    "3474637217884",
    "3616304175916",
    "1001465472370",
    "7908791000366",
    "7893790609996",
    "6291106811568",
    "7896235353690",
    "652418102137",
    "1003958029603",
    "8005610598635",
    "1003008872234",
    "3474636919963",
    "3614226905185",
    "7899706205177",
    "7907093733408",
    "7896235353829",
    "8002135111974",
    "7893697508968",
    "7792256847593",
    "78950000603",
    "8432225043449",
    "7794354285123",
    "7792111462589",
    "7792253994795",
    "3349668596355",
    "7908517905241",
    "7899706193658",
    "7792257688652",
    "7792254325208",
    "4064666317380",
    "74469483575",
    "4064666040783",
    "7895893115039",
    "3349668613588",
    "7899706181631",
    "7792256267216",
    "7893697509204",
    "3349668595945",
    "4064666319490",
    "737052925028",
    "6291107456355",
    "3616303470906",
    "7898667822027",
    "7898947943084",
    "7896235353836",
    "7898759913855",
    "7899706189767",
    "78950000612",
    "7792255216758",
    "7898667820986",
    "3474637217907",
    "7898536549307",
    "7899706205252",
    "7798447882794",
    "8005610589374",
    "7898623242012",
    "7898724572643",
    "7792255216277",
    "7792255216130",
    "7792255275915",
    "7897975696726",
    "7898578153883",
    "3616305267108",
    "7792255635894",
    "896364002749",
    "26256k",
    "7907093652525",
    "7794353994545",
]

if __name__ == "__main__":
    cliente = "Época Cosméticos"
    for ean in produtos:
        asyncio.run(coleta_beleza(ean,cliente))
