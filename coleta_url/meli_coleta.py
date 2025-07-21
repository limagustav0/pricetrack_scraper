import asyncio
import re
import os
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import aiohttp
import json
import urllib.parse
import unicodedata
import time
import pprint

produtos = [
    {"ean": "7891182019767", "titulo": "Wella Professionals Blondor Multi Blonde Powder 800g"},
    {"ean": "7897039846302", "titulo": "Wella Kit Invigo Nutri Enrich Duo (2 Produtos)"},
    {"ean": "7896149424523", "titulo": "Wella Kit Invigo Enrich Profissional Duo (2 Produtos)"},
    {"ean": "7896182559381", "titulo": "Wella Kit Nutri Enrich Tratament Prof Trio (3 Produtos)"},
    {"ean": "7899054132422", "titulo": "Kit Wella Nutri Enrich Invigo Shampoo 1L, Máscara 500g"},
    {"ean": "7895313315537", "titulo": "Kit Wella Invigo Nutri-Enrich Shampoo 1L, Condicionador 1L, Máscara 500g"},
    {"ean": "7899967777383", "titulo": "WELLA KIT INVIGO COLOR BRILLIANCE (3 produtos)"},
    {"ean": "7898258312845", "titulo": "Wella Kit Fusion Duo (2 Produtos)"},
    {"ean": "7893018556392", "titulo": "Wella Kit Fusion Tratament Professionals Duo (2 Produtos)"},
    {"ean": "7898610358276", "titulo": "Wella Kit Fusion Tratament Salon Duo (2 Produtos)"},
    {"ean": "7899766651532", "titulo": "Wella Kit Fusion Tratament Salon Duo (2 Produtos)"},
    {"ean": "7897449676582", "titulo": "Wella Kit Oil Reflections Professional Duo (2 Produtos)"},
    {"ean": "7899590730793", "titulo": "Wella Kit Oil Reflections Profession Duo (2  Produtos)"},
    {"ean": "7895363252523", "titulo": "Wella Kit Oil Reflections Tratament Prof Trio (3 Produtos)"},
    {"ean": "7891264334306", "titulo": "Wella Kit Oil Reflections Tratament Salon (2 Produtos)"},
    {"ean": "7894014829794", "titulo": "Wella Kit Sp Luxe Oil Tratament Prof Duo (2 Produtos)"},
    {"ean": "7893642526303", "titulo": "Wella Kit Nutri Enrich Sh 1L + Cond 1L + Masq 500g + Oil Reflections 100ml"},
    {"ean": "7898556971355", "titulo": "Kit Wella Professionals Cronograma Capilar Masq Enrich 150g + Masq Fusion 150g + Masq Oil Reflections 150g"},
    {"ean": "7895518057225", "titulo": "Wella Kit Invigo Brilliance Prof Trio (3 Produtos)"},
    {"ean": "7896303338048", "titulo": "Wella Kit Oil Reflections 1l Mask (2 Produtos)"},
    {"ean": "7891684146879", "titulo": "Wella Kit Trio Oil Reflections (3 produtos)"},
    {"ean": "7899509755268", "titulo": "Kit Wella Profesisonals Oil Reflections Sh 1L + Masq 500G + Oil Reflections Light 100ml"},
    {"ean": "4064666306179", "titulo": "Wella Óleo Capilar Oil Reflections 100ml"},
    {"ean": "4064666041650", "titulo": "Wella Condicionador Invigo Sun 200ml"},
    {"ean": "3614226750648", "titulo": "Wella Condicionador Color Motion 200ml"},
    {"ean": "3614226750716", "titulo": "Wella Shampoo Color Motion 1L"},
    {"ean": "3614226750785", "titulo": "Wella Shampoo Color Motion 250ml"},
    {"ean": "3614226750815", "titulo": "Wella Máscara Color Motion 150ml"},
    {"ean": "7896235353737", "titulo": "Wella Máscara Fusion 150ml"},
    {"ean": "3614227348868", "titulo": "Wella Condicionador Nutricurls 200ml"},
    {"ean": "3614227348929", "titulo": "Wella Máscara de Nutrição Nutricurls 500ml"},
    {"ean": "3614228865647", "titulo": "Wella Shampoo Nutricurls curls 250ml"},
    {"ean": "3614226403056", "titulo": "Wella Óleo Capilar Oil Reflections Light 100ml"},
    {"ean": "4064666035628", "titulo": "Wella Professionals Elements Calming Shampoo 250ml"},
    {"ean": "4064666040981", "titulo": "Wella Condicionador Nutricurls 1L"},
    {"ean": "7896235353782", "titulo": "Wella Máscara Oil Reflections 150ml"},
    {"ean": "4064666316246", "titulo": "Wella Shampoo Color Brilliance 250ml"},
    {"ean": "7891182017404", "titulo": "Wella Professionals Color Perfect 4/0 Castanho Médio - Coloração Permanente 60g"},
    {"ean": "7892028789479", "titulo": "Kit Wella Professionals Oil Reflections Sh 250ml + Cond 200ml + Oil Reflections 30ml"},
    {"ean": "7892494929522", "titulo": "Kit Wella Professionals Color Brilliance Sh 250ml + Cond 200ml + Masq 150g + Oil Reflections 30ml"},
    {"ean": "7898200967680", "titulo": "Kit Wella Professionals Nutri Enrich Sh 250ml + Masq 150g + Oil Reflections 30ml"},
    {"ean": "7893001675154", "titulo": "Wella Kit Nutricurls Tratament Profissional Trio(3 Produtos)"},
    {"ean": "7891322681175", "titulo": "Kit Wella Professionals Nutri Enrich Sh 250ml + Masq 150g + Oil Reflections Light 30ml"},
    {"ean": "7891443704777", "titulo": "Kit Wella Professionals Nutri Enrich Sh 1L + Masq 500g + Oil Reflections Light 100ml"},
    {"ean": "7895304973821", "titulo": "Kit Wella Professionals Color Brilliance Sh 250ml + Oil Reflections 100ml"},
    {"ean": "7891508825928", "titulo": "Kit Wella Professionals Oil Reflections Sh 250ml + Cond 200ml + Masq 150g + Oil Reflections Light 30ml"},
    {"ean": "7896235353720", "titulo": "Wella Condicionador Fusion 1L"},
    {"ean": "7896235353850", "titulo": "Wella Condicionador Color Brilliance 200g"},
    {"ean": "7893768816999", "titulo": "Wella Invigo Color Brilliance Kit Shampoo 1000ml + Mascara 500ml"},
    {"ean": "7895350590928", "titulo": "Wella Kit Nutri Enrich Sh 250ml + Cond 200ml + Oil Reflections 30ml"},
    {"ean": "7897017358971", "titulo": "Kit Wella Professionals Color Brilliance Sh 250ml + Cond 200ml + Oil Reflections 30ml"},
    {"ean": "7895934550751", "titulo": "Wella Kit Oil Reflections Tratament Prof Trio (3 Produtos)"},
    {"ean": "7893404922350", "titulo": "Kit Wella Professionals Oil Reflections Sh 250ml + Cond 200ml + Masq 150g + Oil Reflections 30ml"},
    {"ean": "7892922958766", "titulo": "Kit Wella Professionals Nutri Enrich Sh 250ml + Cond 200ml + Oil Reflections Light 30ml"},
    {"ean": "7891904575328", "titulo": "Kit Wella Professionals Enrich Sh 250ml + Oil Reflections 100ml"},
    {"ean": "7897291362985", "titulo": "Wella Kit Nutri Enrich Sh 250ml + Cond 200ml + Masq 150g + Oil Reflections 30ml"},
    {"ean": "7897192037272", "titulo": "Wella Kit Oil Reflections Tratament Prof Trio (3 Produtos)"},
    {"ean": "7899340948706", "titulo": "Wella Kit Invigo Brillianc Prof Duo + Oil Ref 100ml"},
    {"ean": "7894870977080", "titulo": "Kit Wella Professionals Enrich Sh 250ml + Oil Reflections Light 100ml"},
    {"ean": "8005610415482", "titulo": "Wella Shampoo Fusion 1L"},
    {"ean": "7896235353874", "titulo": "Wella Condicionador Fusion 200ml"},
    {"ean": "7896235353744", "titulo": "Wella Máscara Fusion 500ml"},
    {"ean": "4064666306179", "titulo": "Wella Tratamento Reconstrutor Fusion Amino Refiller 70ml"},
    {"ean": "4064666316161", "titulo": "Wella Shampoo Fusion 250ml"},
    {"ean": "4064666036274", "titulo": "Wella Shampoo Elements Renew 250ml"},
    {"ean": "7896235353881", "titulo": "Wella Condicionador Oil Reflections 200ml"},
    {"ean": "7896235353751", "titulo": "Wella Shampoo Oil Reflections 1L"},
    {"ean": "4064666043623", "titulo": "Wella Shampoo Oil Reflections 250ml"},
    {"ean": "7896235353775", "titulo": "Wella Máscara Oil Reflections 500ml"},
    {"ean": "8005610573717", "titulo": "Wella Óleo Capilar Oil Reflections 30ml"},
    {"ean": "8005610640327", "titulo": "Wella Spray Fixador Eimi Mistify Strong 500ml"},
    {"ean": "8005610643885", "titulo": "Wella Leave In Nutri-Enrich Wonder Balm 150ml"},
    {"ean": "8005610644233", "titulo": "Wella BB Spray Miracle Color Brilliance 150ml"},
    {"ean": "8005610645261", "titulo": "Kit Wella Ampola Antiqueda Treatment 8X6ml"},
    {"ean": "8005610645681", "titulo": "Wella Máscara Volume Boost Crystal 145ml"},
    {"ean": "4064666318288", "titulo": "Wella Shampoo Color Brilliance 1L"},
    {"ean": "7896235353669", "titulo": "Wella Máscara Color Brilliance 150g"},
    {"ean": "7896235353652", "titulo": "Wella Máscara Color Brilliance 500g"},
    {"ean": "8005610672397", "titulo": "Wella Shampoo Nutri-Enrich 250g"},
    {"ean": "4064666435459", "titulo": "Wella Shampoo Nutri-Enrich 1L"},
    {"ean": "7896235353867", "titulo": "Wella Condicionador Nutri-Enrich 200ml"},
    {"ean": "7896235353690", "titulo": "Wella Condicionador Nutri-Enrich 1L"},
    {"ean": "7896235353713", "titulo": "Wella Máscara de Nutrição Nutri-Enrich 150g"},
    {"ean": "7896235353706", "titulo": "Wella Máscara de Nutrição Nutri-Enrich 500g"},
    {"ean": "3614226771650", "titulo": "Wella Máscara Elements Renew 500ml"},
    {"ean": "4064666035376", "titulo": "Wella Professionals Marula Oil Blender Primer 150ml"},
    {"ean": "3614228820097", "titulo": "Wella Condicionador Color Motion 1L"},
    {"ean": "7896235353645", "titulo": "Wella Condicionador Color Brilliance 1L"},
    {"ean": "8005610644394", "titulo": "Wella Máscara Invigo Nutri Enrich Warming Express Máscara De Aquecimento 150Ml"},
    {"ean": "8005610645230", "titulo": "Wella Ampola de Nutrição Nutri-Enrich 10ml"},
    {"ean": "4064666040806", "titulo": "Wella Condicionador Elements Light Renew 1L"},
    {"ean": "7891950227462", "titulo": "Wella Kit Shampoo e Condicionador Nutri Enrich Invigo"},
    {"ean": "4064666043777", "titulo": "Wella Máscara Volume Boost Crystal 500ml"},
    {"ean": "4064666244389", "titulo": "Wella Condicionador SP Luxe Oil Keratin 200ml"},
    {"ean": "8005610567631", "titulo": "Wella Condicionador SP Luxe Oil Keratin 1L"},
    {"ean": "7896235353805", "titulo": "Wella Shampoo Blond Recharge Desamarelador 250ml"},
    {"ean": "7894872669433", "titulo": "Kit Wella Professionals Cronograma Capilar Masq Enrich 500g + Masq Fusion 500g + Masq Oil Reflections 500g"},
]

sellers_desejados = ["Amobeleza"]

def normalizar_nome(nome):
    nome = unicodedata.normalize('NFKD', nome)
    nome = ''.join([c for c in nome if not unicodedata.combining(c)])
    nome = nome.lower().strip()
    nome = ' '.join(nome.split())
    return nome

async def scrape_product(ean, max_retries=3):
    # URL de busca
    search_url = f"https://www.amazon.com.br/s?k={ean}"
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()
            try:
                # Abre a URL de busca
                await page.goto(search_url)
                await page.wait_for_selector('div.s-main-slot a.a-link-normal.s-no-outline', timeout=10000)
                primeiro_produto = await page.query_selector('div.s-main-slot a.a-link-normal.s-no-outline')
                if primeiro_produto:
                    await primeiro_produto.click()
                    await page.wait_for_load_state('load')
                    produto_url = page.url
                else:
                    print("Nenhum produto encontrado.")
                    await browser.close()
                    return None

                # Extrai EAN da URL de busca (ou mantém o parâmetro recebido)
                ean_value = ean

                # Tenta extrair a marca da página do produto
                marca = None
                try:
                    # Primeiro tenta pelo bylineInfo
                    byline = await page.query_selector('#bylineInfo')
                    if byline:
                        marca = (await byline.text_content()).strip()
                    # Se não encontrar, tenta pelo tr de marca na tabela de atributos
                    if not marca:
                        tr_brand = await page.query_selector('tr.po-brand')
                        if tr_brand:
                            # Busca o segundo <td> dentro do <tr> de marca
                            tds = await tr_brand.query_selector_all('td')
                            if len(tds) >= 2:
                                span = await tds[1].query_selector('span.a-size-base.po-break-word')
                                if span:
                                    marca = (await span.text_content()).strip()
                    # Se ainda não encontrar, tenta pelo span com a classe específica (fallback)
                    if not marca:
                        span_brand = await page.query_selector('span.a-size-base.po-break-word')
                        if span_brand:
                            marca = (await span_brand.text_content()).strip()
                except Exception:
                    pass

                result = {
                    'ean': ean_value,
                    'brand': marca,
                    'url': None
                }
                # Limpa a URL para remover parâmetros e garantir que termina após o código do produto
                if produto_url:
                    match = re.search(r'(https://www\.amazon\.com\.br/.*/dp/[A-Z0-9]+)', produto_url)
                    if match:
                        clean_url = match.group(1)
                    else:
                        # fallback para o método anterior
                        parsed = urllib.parse.urlparse(produto_url)
                        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    result['url'] = clean_url
                print(json.dumps(result, ensure_ascii=False, indent=2))
                await browser.close()
                print(result)
                return result
            except PlaywrightTimeoutError as e:
                print(f"[Tentativa {attempt}] Timeout ao buscar/clicar no produto: {e}. Reiniciando browser...")
                await browser.close()
                if attempt >= max_retries:
                    print("Número máximo de tentativas atingido. Abortando.")
                    return None
            except Exception as e:
                print(f"[Tentativa {attempt}] Erro inesperado: {e}")
                await browser.close()
                return None

async def scrape_mercadolivre(ean, seller_name, descricao, cookies_path='meli_auth.json'):
    search_url = f"https://lista.mercadolivre.com.br/{ean}"
    descricao_pedacos = [p.strip().lower() for p in descricao.split()]
    resultado_url = None

    # Carrega cookies do arquivo meli_auth.json ANTES de abrir o browser/context
    try:
        with open(cookies_path, 'r', encoding='utf-8') as f:
            auth_data = json.load(f)
            cookies = auth_data['cookies'] if 'cookies' in auth_data else auth_data
    except Exception as e:
        print(f'Erro ao carregar cookies: {e}')
        return None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        await context.add_cookies(cookies)
        page = await context.new_page()

        print(f"Tentando acessar: {search_url}")
        await page.goto(search_url)
        print(f"URL atual após goto: {page.url}")

        if page.url == "about:blank":
            print("Erro: Navegação não ocorreu. Verifique cookies, user-agent e domínio.")
            await browser.close()
            return None

        # Detecta se o produto buscado é kit ou unitário
        titulo_busca = descricao.lower()
        is_kit_busca = 'kit' in titulo_busca or '+' in titulo_busca

        cards = await page.query_selector_all('li.ui-search-layout__item')
        print(f"Items encontrados: {len(cards)}")
        time.sleep(15)
        for idx, card in enumerate(cards[:10], 1):
            print(f"Verificando item {idx}")
            # Extrai o título do produto no card
            titulo_el = await card.query_selector('a.poly-component__title')
            if not titulo_el:
                continue
            titulo_card = (await titulo_el.text_content()).lower()

            # Se o produto buscado for unitário, ignore cards de kit
            if not is_kit_busca and ('kit' in titulo_card or '+' in titulo_card):
                print('Ignorando kit, buscando unitário.')
                continue
            # Se o produto buscado for kit, ignore cards unitários
            if is_kit_busca and not ('kit' in titulo_card or '+' in titulo_card):
                print('Ignorando unitário, buscando kit.')
                continue

            # Quebra o nome do produto buscado em pedaços e exige pelo menos dois no título do card
            descricao_pedacos = [p.strip() for p in descricao.lower().split()]
            match_count = sum(1 for p in descricao_pedacos if p in titulo_card)
            if match_count < 2:
                print('Poucos pedaços do nome encontrados, ignorando.')
                continue  # Não é o produto certo

            # Clica no produto para abrir a página de detalhes
            produto_link = await card.query_selector('a.poly-component__title')
            if not produto_link:
                continue
            produto_url = await produto_link.get_attribute('href')
            if not produto_url:
                continue

            # Abre a página do produto em uma nova aba
            produto_page = await context.new_page()
            await produto_page.goto(produto_url)
            await produto_page.wait_for_load_state('domcontentloaded')
            await produto_page.wait_for_timeout(1000)

            # Tenta encontrar o botão de ver mais opções de forma flexível
            ver_mais_btn = await produto_page.query_selector('span.andes-button__content:has-text("opções a partir de")')
            if not ver_mais_btn:
                # Fallback: tenta por XPath (caso o texto esteja fragmentado)
                ver_mais_btn = await produto_page.query_selector('//span[contains(@class, "andes-button__content") and contains(text(), "opções a partir de")]')
            if not ver_mais_btn:
                print('Não encontrou botão "Ver mais opções" neste produto.')
                await produto_page.close()
                continue
            print('Clicando no botão "Ver mais opções"...')

            # Clica no botão (subindo para o pai clicável)
            parent_btn = await ver_mais_btn.evaluate_handle('node => node.closest("button, a")')
            if parent_btn:
                await parent_btn.click()
                await produto_page.wait_for_load_state('domcontentloaded')
                await produto_page.wait_for_timeout(2000)
                sellers_url = produto_page.url
                print(f"URL de todos os sellers: {sellers_url}")
                # Lista todos os vendedores encontrados
                seller_cells = await produto_page.query_selector_all('div.ui-pdp-table__cell.ui-pdp-s-table__seller.ui-pdp-s-table__cell')
                print(f"Vendedores encontrados: {len(seller_cells)}")
                for sidx, seller_cell in enumerate(seller_cells, 1):
                    seller_btn = await seller_cell.query_selector('button.ui-pdp-seller__link-trigger-button span')
                    if seller_btn:
                        seller_name_found = (await seller_btn.text_content()).strip()
                        print(f"  Seller {sidx}: {seller_name_found}")
                        # Verifica se algum dos sellers desejados está presente
                        for seller_desejado in sellers_desejados:
                            if normalizar_nome(seller_desejado) == normalizar_nome(seller_name_found):
                                print(f"Seller desejado encontrado: {seller_desejado}")
                                # Tenta extrair a marca da página do produto
                                marca = None
                                try:
                                    marca_el = await produto_page.query_selector('span.poly-component__brand')
                                    if marca_el:
                                        marca = (await marca_el.text_content()).strip()
                                except Exception:
                                    marca = None
                                await produto_page.close()
                                await browser.close()
                                return {
                                    'ean': ean,
                                    'brand': marca,
                                    'url': sellers_url
                                }
                print('Seller desejado não encontrado neste produto.')
            await produto_page.close()
        await browser.close()
        return None

async def main():
    resultados = []
    for produto in produtos:
        print(f"\nBuscando EAN: {produto['ean']} | Título: {produto['titulo']}")
        resultado = None
        for seller in sellers_desejados:
            resultado = await scrape_mercadolivre(produto["ean"], seller, produto["titulo"])
            if resultado:
                print(f"Seller encontrado: {seller}")
                print(f"Resultado: {resultado}")
                resultados.append(resultado)
                break
        if not resultado:
            print("Nenhum seller desejado encontrado para este produto.")
    print("\nResultados finais:")
    for r in resultados:
        pprint(r)

if __name__ == "__main__":
    asyncio.run(main())