import asyncio
import random
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse, parse_qs
from playwright.async_api import async_playwright
import logging
import re
import json

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lista de user agents para rotação
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0"
]

async def epoca_scrap(ean, marca):
    """Realiza o scraping de produtos da Época Cosméticos, filtrando por todas as marcas semelhantes (incluindo variações em maiúsculas) e todos os sellers sem desmarcar."""
    url = f"https://www.epocacosmeticos.com.br/pesquisa?q={ean}&ordenacao=az"
    logger.info("[Época] Iniciando para: %s", url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1280, "height": 720},
            java_script_enabled=True,
            bypass_csp=True,
            ignore_https_errors=True,
            extra_http_headers={
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Referer": "https://www.google.com/",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive"
            }
        )

        # Consentimento de cookies
        await context.add_cookies([{
            "name": "cookieConsent",
            "value": "true",
            "domain": ".epocacosmeticos.com.br",
            "path": "/"
        }])

        page = await context.new_page()
        lojas = []
        processed_skus = set()  # Evitar duplicatas

        # Carregamento com retries
        for attempt in range(3):
            try:
                await page.goto(url, timeout=30000)
                await page.wait_for_load_state("domcontentloaded")
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await asyncio.sleep(random.uniform(2, 4))
                try:
                    await page.get_by_role("button", name="CONCORDAR E FECHAR").click(timeout=5000)
                    logger.info("[Época] Tooltip de privacidade fechado.")
                except:
                    logger.info("[Época] Nenhum tooltip de privacidade encontrado.")
                break
            except Exception as e:
                logger.warning("[Época] Erro ao carregar página (tentativa %d/3): %s", attempt + 1, e)
                if attempt == 2:
                    logger.error("[Época] Falha após 3 tentativas para EAN %s", ean)
                    await context.close()
                    await browser.close()
                    return []

        # Expande a seção de filtros (Marca ou Vendido por) e retorna os itens
        async def expand_filter_section(filter_name):
            filter_sections = await page.query_selector_all('div.SearchFilterItem')
            for section in filter_sections:
                header = await section.query_selector('p')
                if header and (await header.inner_text()).strip() == filter_name:
                    collapse_div = await section.query_selector('div.colapse_options')
                    if collapse_div and "max-h-0" in (await collapse_div.get_attribute('class') or ''):
                        await header.click()
                        await page.wait_for_selector(f'div.SearchFilterItem ul.p-2.pl-3 > li', state='visible', timeout=15000)
                        logger.info("[Época] Seção '%s' expandida.", filter_name)
                    else:
                        logger.info("[Época] Seção '%s' já está expandida.", filter_name)
                    return await section.query_selector_all('ul.p-2.pl-3 > li')
            raise Exception(f"Seção '{filter_name}' não encontrada")

        # Seleciona todas as marcas semelhantes no filtro de Marca, incluindo variações em maiúsculas
        async def select_brand_filter(marca):
            try:
                brand_items = await expand_filter_section("Marca")
                # Quebra a marca em partes e inclui a versão em maiúsculas
                marca_parts = [part.lower() for part in marca.split() if part]  # Ex.: ["wella", "professionals"]
                marca_upper = marca.upper()
                marca_upper_parts = [part for part in marca_upper.split() if part]  # Ex.: ["WELLA", "PROFESSIONALS"]
                all_parts = list(set(marca_parts + marca_upper_parts))  # Ex.: ["wella", "professionals", "WELLA", "PROFESSIONALS"]
                selected_brands = []
                
                # Itera sobre os itens de marca, reavaliando a lista após cada clique
                for _ in range(len(brand_items)):  # Limita ao número de itens para evitar loop infinito
                    brand_items = await page.query_selector_all('ul.p-2.pl-3 > li')  # Reavalia o DOM
                    for item in brand_items:
                        brand_name_el = await item.query_selector('span.flex.items-center > span:first-child')
                        if brand_name_el:
                            brand_name = (await brand_name_el.inner_text()).strip()
                            brand_name_lower = brand_name.lower()
                            # Verifica se a marca já foi selecionada
                            if brand_name in selected_brands:
                                logger.info("[Época] Marca '%s' já selecionada. Pulando.", brand_name)
                                continue
                            # Verifica se alguma parte da marca (original ou maiúscula) está no nome da marca do filtro
                            if any(part.lower() in brand_name_lower for part in all_parts):
                                checkbox = await item.query_selector('span.icon_wrapper')
                                if checkbox and await checkbox.is_visible():
                                    try:
                                        await checkbox.click()
                                        selected_brands.append(brand_name)
                                        logger.info("[Época] Filtro de marca '%s' selecionado.", brand_name)
                                        await page.wait_for_timeout(5000)  # Aguarda 5 segundos para o filtro ser aplicado
                                    except Exception as e:
                                        logger.error("[Época] Erro ao clicar no checkbox da marca '%s': %s", brand_name, e)
                                else:
                                    logger.warning("[Época] Checkbox não encontrado ou não visível para a marca '%s'.", brand_name)
                    # Sai do loop se nenhuma nova marca for selecionada
                    if not selected_brands:
                        logger.warning("[Época] Nenhuma marca semelhante a '%s' ou '%s' encontrada no filtro.", marca, marca_upper)
                        break
                if selected_brands:
                    logger.info("[Época] Marcas selecionadas: %s", ", ".join(selected_brands))
                else:
                    logger.warning("[Época] Nenhuma marca selecionada para '%s' ou '%s'.", marca, marca_upper)
            except Exception as e:
                logger.error("[Época] Erro ao selecionar filtros de marca '%s': %s", marca, e)

        # Valida se a descrição contém partes da marca
        def validate_product_by_brand(marca, descricao):
            marca_parts = [part.lower() for part in marca.split() if part]  # Ex.: ["wella", "professionals"]
            descricao_lower = descricao.lower()
            # Pelo menos uma parte da marca deve estar na descrição
            return any(part in descricao_lower for part in marca_parts)

        # Processa os produtos filtrados para o seller atual
        async def process_products(page, produtos, seller_name, ean, marca, processed_skus):
            lojas = []
            for idx, produto in enumerate(produtos):
                try:
                    logger.info("[Época] Processando produto %d/%d para seller %s", idx + 1, len(produtos), seller_name)

                    # Extrair SKU
                    sku = await produto.get_attribute("data-product-sku") or f"{ean}_epoca_cosmeticos_{seller_name.lower().replace(' ', '')}_{idx+1}"
                    if sku in processed_skus:
                        logger.info("[Época] Produto com SKU %s já processado. Pulando.", sku)
                        continue

                    # Extrair marca
                    marca_el = await produto.query_selector(".brand")
                    marca_produto = (await marca_el.inner_text()).strip().upper() if marca_el else marca

                    # Extrair nome/descrição
                    nome_el = await produto.query_selector(".name")
                    descricao = (await nome_el.inner_text()).strip() if nome_el else ""

                    # Validar por similaridade com a marca
                    if not validate_product_by_brand(marca, descricao):
                        logger.info("[Época] Produto '%s' (SKU: %s) não contém partes da marca '%s'. Pulando.", descricao, sku, marca)
                        continue

                    # Extrair preço final
                    preco_el = await produto.query_selector(".product-price_spotPrice__k_4YC")
                    preco_final = "0"
                    if preco_el:
                        preco_text = (await preco_el.inner_text()).strip()
                        match = re.search(r'R\$\s*([\d,.]+)', preco_text)
                        if match:
                            preco_final = match.group(1).replace(",", ".")

                    # Extrair link
                    link_el = await produto.query_selector('a[data-content-item="true"]')
                    link = await link_el.get_attribute("href") if link_el else ""
                    if link and not link.startswith("http"):
                        link = "https://www.epocacosmeticos.com.br" + link

                    # Corrigir URL redirecionada
                    if "algorecs" in link:
                        parsed_link = urlparse(link)
                        query_params = parse_qs(parsed_link.query)
                        if "ct" in query_params:
                            decoded_url = unquote(query_params["ct"][0])
                            link = decoded_url

                    # Extrair imagem e garantir HTTPS
                    img_el = await produto.query_selector("img")
                    imagem = await img_el.get_attribute("src") if img_el else "https://via.placeholder.com/150"
                    if imagem and imagem.startswith("//"):
                        imagem = "https:" + imagem
                    elif imagem and not imagem.startswith("http"):
                        imagem = "https://www.epocacosmeticos.com.br" + imagem

                    # Extrair avaliação (review)
                    review_el = await produto.query_selector(".rate__gray > div")
                    review = 0.0
                    if review_el:
                        width_style = await review_el.get_attribute("style")
                        match = re.search(r"width:\s*(\d+)%", width_style)
                        if match:
                            review = float(match.group(1)) / 20  # Converte de 0-100% para 0-5

                    # Outros dados
                    data_hora = datetime.now(timezone.utc).isoformat()
                    status = "ativo"
                    marketplace = "Época Cosméticos"
                    key_loja = seller_name.lower().replace(" ", "")
                    key_sku = f"{key_loja}_{ean}_{idx+1}" if sku.startswith(f"{ean}_epoca_cosmeticos") else f"{key_loja}_{ean}"

                    resultado = {
                        "ean": ean,
                        "url": link,
                        "sku": sku,
                        "descricao": descricao,
                        "loja": seller_name,
                        "preco_final": preco_final,
                        "imagem": imagem,
                        "review": review,
                        "data_hora": data_hora,
                        "status": status,
                        "marketplace": marketplace,
                        "key_loja": key_loja,
                        "key_sku": key_sku,
                        "marca": marca_produto
                    }

                    logger.info("[Época] Produto processado: %s", resultado)
                    lojas.append(resultado)
                    processed_skus.add(sku)

                except Exception as e:
                    logger.error("[Época] Erro ao processar produto %d para seller %s: %s", idx + 1, seller_name, e)

            return lojas

        try:
            # Selecionar filtros de todas as marcas semelhantes
            await select_brand_filter(marca)

            total = 0
            while True:
                # Sempre atualiza a lista de sellers (DOM pode mudar)
                seller_items = await expand_filter_section("Vendido por")

                if total >= len(seller_items):
                    logger.info("[Época] Todos os sellers processados.")
                    break

                # Marca o vendedor atual
                curr_item = seller_items[total]
                checkbox = await curr_item.query_selector('span.icon_wrapper')
                seller_name_el = await curr_item.query_selector('span.flex.items-center > span:first-child')
                seller_name = (await seller_name_el.inner_text()).strip() if seller_name_el else f"Desconhecido_{total+1}"

                if checkbox:
                    await checkbox.click()
                    logger.info(f"[Época] Clicado em: {seller_name} (índice {total})")
                    await page.wait_for_timeout(5000)  # Tempo para filtro aplicar

                    # Coletar produtos filtrados
                    produtos = await page.query_selector_all('div[data-testid="productItemComponent"]')
                    logger.info("[Época] %d produtos encontrados para o seller %s", len(produtos), seller_name)
                    lojas.extend(await process_products(page, produtos, seller_name, ean, marca, processed_skus))

                else:
                    logger.error(f"[Época] Checkbox não encontrado para {seller_name} (índice {total})")
                    break

                total += 1

        except Exception as e:
            logger.error("[Época] Erro ao processar vendedores: %s", e)

        await context.close()
        await browser.close()
        return lojas