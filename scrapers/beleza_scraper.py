import asyncio
import re
from datetime import datetime
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from otel.trace import tracer



async def beleza_na_web_scrap(target_url: str, ean: str, marca: str, headless: bool):
    with tracer.start_as_current_span("beleza_na_web_scrap") as main_span:
        """
        Crawls the Beleza na Web product page and extracts store data.
        """
        if not target_url or not target_url.startswith("http"):
            raise ValueError("URL inválida")
        if not ean:
            raise ValueError("EAN não pode ser vazio")

        print(f"[BNW] Iniciando raspagem para: {target_url}")
        lojas = []

        browser_config = BrowserConfig(headless=headless)
        run_config = CrawlerRunConfig()

        try:
            with tracer.start_as_current_span("generate-markdown") as span_markdown:
                async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
                    result = await crawler.arun(url=target_url, config=run_config)
                    markdown_content = result.markdown
                    print(f"[BNW] Markdown gerado para {target_url}")

            # === Extração do SKU ===
            with tracer.start_as_current_span("scrap-sku") as span_sku:
                sku_pattern = r'\*\*Cod:\*\* (MP\d+|\d+)'
                sku_match = re.search(sku_pattern, markdown_content)
                sku = sku_match.group(1) if sku_match else "SKU não encontrado"
                if not sku_match:
                    print(f"[BNW] SKU não encontrado no Markdown para {target_url}")
                    return lojas
                span_sku.set_attribute("sku", sku)

            # === Extração da descrição ===
            with tracer.start_as_current_span("scrap-description") as span_desc:
                desc_pattern = r'\[Voltar para a página do produto\]\(https://www\.belezanaweb\.com\.br/(.+?)\)'
                desc_match = re.search(desc_pattern, markdown_content)
                if desc_match:
                    url_text = desc_match.group(1)
                    descricao = ' '.join(word.capitalize() for word in url_text.split('-'))
                    descricao = descricao.replace('Condicionador ', 'Condicionador - ')
                else:
                    descricao = "Descrição não encontrada"
                print(f"[BNW] Descrição capturada: {descricao!r}")
                span_desc.set_attribute("descricao", descricao)

            # === Extração do review ===
            with tracer.start_as_current_span("scrap-review") as span_review:
                review_pattern = r'Review[:\s]*(\d+[\.,]\d+|\d+)'
                review_match = re.search(review_pattern, markdown_content)
                review = float(review_match.group(1).replace(',', '.')) if review_match else 4.5
                print(f"[BNW] Review capturado: {review}")
                span_review.set_attribute("review", review)

            # === Extração da imagem ===
            with tracer.start_as_current_span("scrap-image") as span_img:
                img_pattern_with_desc = r'!\[.*?\]\((https://res\.cloudinary\.com/beleza-na-web/image/upload/.*?/v1/imagens/product/.*?/.*?\.(?:png|jpg))\)'
                img_match_with_desc = re.search(img_pattern_with_desc, markdown_content)
                imagem = img_match_with_desc.group(1) if img_match_with_desc else "Imagem não encontrada"
                if imagem == "Imagem não encontrada":
                    img_pattern_empty = r'!\[\]\((https?://[^\s)]+)\)'
                    img_matches_empty = re.findall(img_pattern_empty, markdown_content)
                    imagem = img_matches_empty[0] if img_matches_empty else "Imagem não encontrada"
                print(f"[BNW] Imagem capturada: {imagem}")
                span_img.set_attribute("imagem", imagem)

            # === Extração das lojas e preços ===
            with tracer.start_as_current_span("scrap-prices-sellers") as span_prices:
                loja_pattern = r'Vendido por \*\*(.*?)\*\* Entregue por Beleza na Web'
                preco_com_desconto_pattern = r'-[\d]+%.*?\nR\$ ([\d,\.]+)'
                preco_venda_pattern = r'(?<!De )R\$ ([\d,\.]+)(?!\s*3x)'
                blocos = re.split(r'(?=Vendido por \*\*.*?\*\* Entregue por Beleza na Web)', markdown_content)

                with tracer.start_as_current_span(f"scrap-seller") as span_seller:
                    for idx, bloco in enumerate(blocos):
                        if 'Vendido por' not in bloco:
                            continue

                            loja_match = re.search(loja_pattern, bloco)
                            preco_com_desconto_match = re.search(preco_com_desconto_pattern, bloco)
                            preco_venda_match = re.search(preco_venda_pattern, bloco)

                            loja = loja_match.group(1).strip() if loja_match else "Beleza na Web"

                            if preco_com_desconto_match:
                                preco_final_str = preco_com_desconto_match.group(1)
                            elif preco_venda_match:
                                preco_final_str = preco_venda_match.group(1)
                            else:
                                preco_final_str = "0,0"

                            preco_final_str = preco_final_str.replace('.', '').replace(',', '.')
                            try:
                                preco_final = float(preco_final_str)
                            except ValueError:
                                print(f"[BNW] Preço inválido para vendedor {idx+1}: {preco_final_str}")
                                continue

                            data_hora = datetime.utcnow().isoformat() + "Z"
                            status = "ativo"
                            marketplace = "Beleza na Web"
                            key_loja = loja.lower().replace(" ", "")
                            key_sku = f"{key_loja}_{ean}"

                            resultado = {
                                "ean": ean,
                                "url": target_url,
                                "sku": sku,
                                "descricao": descricao,
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

                            span_seller.set_attributes({
                                "loja": loja,
                                "preco_final": preco_final,
                                "key_sku": key_sku
                            })

                            print(f"[BNW] Produto {idx+1}:")
                            print(resultado)
                            lojas.append(resultado)

        except Exception as e:
            with tracer.start_as_current_span("error") as span_error:
                span_error.record_exception(e)
            print(f"[BNW] Erro ao processar {target_url}: {e}")
            return lojas

        return lojas


if __name__ == "__main__":
    lojas = asyncio.run(beleza_na_web_scrap(
        "https://www.belezanaweb.com.br/wella-professionals-invigo-nutrienrich-mascara-capilar-500ml/ofertas-marketplace",
        "7896235353706",
        "Wella Professionals",
        True
    ))
    print(lojas)
