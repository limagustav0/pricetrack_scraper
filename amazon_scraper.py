import asyncio
import json
import os
import re
import time
from datetime import datetime
from pprint import pprint

import aiohttp
from playwright.async_api import async_playwright



async def amazon_scrap(target_url: str, ean:str, marca:str) -> list:
    print(f"[Amazon] Iniciando raspagem para: {target_url}")
    start_time = time.time()
    lojas = []
    storage_file = "amz_auth.json"

    if not os.path.exists(storage_file):
        print(f"[Amazon] Erro: Arquivo de autenticação {storage_file} não encontrado.")
        return lojas

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width":480, "height":300})
        page = await context.new_page()
        print("[Amazon] Página criada, carregando cookies e navegando para a URL...")

        try:
            # Carregar cookies
            try:
                with open(storage_file, 'r') as f:
                    auth_data = json.load(f)
                    await context.add_cookies(auth_data.get('cookies', []))
                print(f"[Amazon] Cookies carregados.")
            except Exception as e:
                print(f"[Amazon] Erro ao carregar cookies: {e}")
                return lojas

            # Navegar para a URL
            print(f"[Amazon] Navegando para {target_url}")
            response = await page.goto(target_url, timeout=30000)
            if response and response.status != 200:
                print(f"[Amazon] Falha ao carregar página {target_url}. Status: {response.status}")
                return lojas
            await page.wait_for_load_state('domcontentloaded', timeout=15000)
            print(f"[Amazon] Página carregada.")

            # Extrair SKU
            sku = "SKU não encontrado"
            try:
                match = re.search(r'/dp/([A-Z0-9]{10})', target_url)
                if match:
                    sku = match.group(1)
                print(f"[Amazon] SKU extraído: {sku}")
            except Exception as e:
                print(f"[Amazon] Erro ao extrair SKU: {e}")

            # Funções para extração concorrente
            async def get_description():
                try:
                    await page.wait_for_selector('#productTitle', timeout=7000)
                    return (await page.locator('#productTitle').first.inner_text()).strip()
                except Exception as e:
                    print(f"Erro ao extrair descrição: {e}")
                    return "Descrição não encontrada"

            async def get_image():
                try:
                    await page.wait_for_selector('#landingImage', timeout=7000)
                    return await page.locator('#landingImage').first.get_attribute('src')
                except Exception as e:
                    print(f"Erro ao extrair imagem: {e}")
                    return "Imagem não encontrada"

            async def get_review():
                try:
                    review_span = page.locator('a.a-popover-trigger span[aria-hidden="true"]').first
                    review_text = (await review_span.inner_text(timeout=7000)).strip()
                    print(f"Texto da review capturado: '{review_text}'")
                    if review_text and re.match(r'^\d+\.\d$', review_text.replace(',', '.')):
                        return float(review_text.replace(',', '.'))
                    print("Review não encontrada ou inválida, usando padrão 4.5")
                    return 4.5
                except Exception as e:
                    print(f"Erro ao extrair review: {e}")
                    return 4.5

            # Executar extração concorrente
            descricao, imagem, review = await asyncio.gather(
                get_description(),
                get_image(),
                get_review()
            )
            print(f"[Amazon] Descrição: {descricao}, Imagem: {imagem}, Review: {review}")

            # Extrair vendedor principal e preço
            print(f"[Amazon] Extraindo vendedor principal e preço...")
            seller_name = "Não informado"
            preco_final = 0.0
            try:
                seller = page.locator("#sellerProfileTriggerId").first
                seller_name = (await seller.inner_text(timeout=7000)).strip()
                seller_name = re.sub(r'Vendido por\s*', '', seller_name).strip()

                price_span = page.locator('div.a-section.a-spacing-micro span.a-offscreen').first
                price_text = re.sub(r'[^\d,.]', '', (await price_span.inner_text(timeout=7000)).strip()).replace(',', '.')
                if re.match(r'^\d+\.\d+$', price_text):
                    preco_final = float(price_text)
                else:
                    print(f"Preço inválido na página principal: {price_text}")

                if seller_name != "Não informado" and preco_final > 0.0:
                    key_loja = seller_name.lower().replace(' ', '')
                    key_ean = f"{key_loja}_{ean}" if sku != "SKU não encontrado" else f"{key_loja}_sem_sku"
                    lojas.append({
                        'sku': sku,
                        'loja': seller_name,
                        'preco_final': preco_final,
                        'data_hora': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'marketplace': 'Amazon',
                        'key_loja': key_loja,
                        'key_sku': key_ean,
                        'descricao': descricao,
                        'review': review,
                        'imagem': imagem,
                        'status': 'ativo',
                        "url":target_url,
                        "ean": ean,
                        "marca":marca
                    })
                    print(f"Vendedor principal capturado: {seller_name}, Preço: {preco_final}")
            except Exception as e:
                print(f"Erro ao extrair vendedor/preço da página principal: {e}")

            # Acessar página de ofertas
            try:
                compare_button = page.get_by_role("button", name=re.compile("Comparar outras.*ofertas|Ver todas as ofertas"))
                await compare_button.wait_for(state='visible', timeout=10000)
                print("Botão de comparação encontrado")
                await compare_button.click(timeout=10000)
                print(f"After clicking compare button: {time.time() - start_time:.2f} seconds")

                details_link = page.get_by_role("link", name="Ver mais detalhes sobre esta")
                await details_link.wait_for(state='visible', timeout=10000)
                print("Link 'Ver mais detalhes' encontrado")
                await details_link.click(timeout=10000)
                print(f"After clicking details link: {time.time() - start_time:.2f} seconds")

                await page.wait_for_load_state('domcontentloaded', timeout=15000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)
                print(f"After loading offers page: {time.time() - start_time:.2f} seconds")
            except Exception as e:
                print(f"Erro ao acessar página de ofertas: {e}")
                print("Page content for debugging:", await page.content()[:1000])
                return lojas

            # Extrair ofertas
            try:
                await page.wait_for_selector("#aod-offer", timeout=10000)
                offer_elements = await page.locator("#aod-offer").all()
                print(f"Encontradas {len(offer_elements)} ofertas")
                for i, offer in enumerate(offer_elements, 1):
                    try:
                        preco_final = 0.0
                        try:
                            price_span = offer.locator('span.aok-offscreen').first
                            price_text = re.sub(r'[^\d,.]', '', (await price_span.inner_text(timeout=5000)).strip()).replace(',', '.')
                            if re.match(r'^\d+\.\d+$', price_text):
                                preco_final = float(price_text)
                            else:
                                print(f"Preço inválido na oferta {i}: {price_text}")
                        except Exception:
                            try:
                                price_whole = (await offer.locator("span.a-price-whole").first.inner_text(timeout=5000)).strip()
                                price_fraction = (await offer.locator("span.a-price-fraction").first.inner_text(timeout=5000)).strip()
                                price_text = f"{re.sub(r'[^\d]', '', price_whole)}.{price_fraction}"
                                if re.match(r'^\d+\.\d+$', price_text):
                                    preco_final = float(price_text)
                                else:
                                    print(f"Preço inválido na oferta {i} (fallback): {price_text}")
                            except Exception as e:
                                print(f"Erro ao extrair preço na oferta {i}: {e}")
                                continue

                        seller_name = "Não informado"
                        try:
                            seller = offer.locator("a.a-size-small.a-link-normal").first
                            seller_name = (await seller.inner_text(timeout=5000)).strip()
                            seller_name = re.sub(r'Vendido por\s*', '', seller_name).strip()
                        except Exception as e:
                            print(f"Erro ao extrair vendedor na oferta {i}: {e}")
                            continue

                        if any(s['loja'] == seller_name for s in lojas):
                            print(f"Vendedor {seller_name} já capturado, ignorando duplicata")
                            continue

                        key_loja = seller_name.lower().replace(' ', '')
                        key_sku = f"{key_loja}_{sku}" if sku != "SKU não encontrado" else f"{key_loja}_sem_sku"
                        lojas.append({
                            'sku': sku,
                            'loja': seller_name,
                            'preco_final': preco_final,
                            'data_hora': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                            'marketplace': 'Amazon',
                            'key_loja': key_loja,
                            'key_sku': key_sku,
                            'descricao': descricao,
                            'review': review,
                            'imagem': imagem,
                            'status': 'ativo',
                            "url":target_url,
                            "ean": ean,
                            "marca":marca
                        })
                        print(f"Oferta {i} capturada: {seller_name}, Preço: {preco_final}")
                    except Exception as e:
                        print(f"Erro ao processar oferta {i}: {e}")
                        continue
            except Exception as e:
                print(f"Erro ao extrair ofertas: {e}")
                print("Page content for debugging:", await page.content()[:1000])

        finally:
            await context.storage_state(path="amz_auth.json")
            await context.close()
            await browser.close()
            print(f"[Amazon] Raspagem finalizada para: {target_url}")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"[Amazon] Tempo de execução: {execution_time:.2f} segundos")
    pprint(lojas)
    return lojas


if __name__=="__main__":
    asyncio.run(amazon_scrap("https://www.amazon.com.br/Wella-3922-Shampoo-1000Ml-Brilliance/dp/B0C3MHGZXP/ref=sr_1_1","4064666318356","Wella"))
