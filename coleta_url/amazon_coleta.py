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
produtos = [
    {"ean": "7898724572612", "descricao": "Fran by Franciny Ehlke LipHoney - Gloss Labial 3,5ml"},
    {"ean": "3616302038633", "descricao": "Chloé Signature Refilável - Perfume Feminino - Eau de Parfum 100ml"},
    {"ean": "4064666318356", "descricao": "Wella Professionals Invigo Color Brilliance Shampoo 1 Litro"},
    {"ean": "7896235353645", "descricao": "Wella Professionals Invigo Color Brilliance Condicionador - 1 Litro"},
    {"ean": "7896235353652", "descricao": "Wella Invigo Color Máscara 500ml, Cor: branco"},
    {"ean": "4064666435459", "descricao": "Wella Professionals Nutri Enrich Shampoo 1000 ml"},
    {"ean": "6291108735411", "descricao": "Lattafa Perfume Asad para Eau de Parfum Spray unissex, 100 ml"},
    {"ean": "3349668566372", "descricao": "1 Million Masculino Eau de Toilette"},
    {"ean": "7896235353751", "descricao": "Wella Professionals Oil Reflections Luminous Reveal Shampoo 1 Litro"},
    {"ean": "7896235353775", "descricao": "Máscara Wella Professionals Oil Reflections 500ML"},
    {"ean": "7898947943848", "descricao": "Truss Reconstrutor Uso Obrigatório | Tratamento Capilar com Proteção Térmica, Brilho e Redução de Quebra | 260ml"},
    {"ean": "6291107456041", "descricao": "Eau de Parfum Lattafa Fakhar spray feminino, 100 ml"},
    {"ean": "3614226403056", "descricao": "Wella Professionals Oil Reflections Light Oleo Capilar 100ml"},
    {"ean": "7898667820726", "descricao": "BRAÉ ESSENTIAL SPRAY FINALIZADOR 260ml"},
    {"ean": "6291108730515", "descricao": "Perfume Importado Eau de Parfum Yara Lattafa"},
    {"ean": "5055810013110", "descricao": "Al Wataniah Sabah Al Ward Edp 100Ml, Al Wataniah"},
    {"ean": "7896235353812", "descricao": "Shampoo Wella Professionals Aqua Pure 1000ml"},
    {"ean": "4064666210131", "descricao": "Sebastian Professional Dark Oil Mascara Capilar 150ml"},
    {"ean": "7896235353706", "descricao": "Wella Professionals Invigo Nutri-Enrich - Máscara Capilar 500ml"},
    {"ean": "8005610415871", "descricao": "Wella Professionals Fusion Mascara Reparadora 500ml"},
    {"ean": "6085010044712", "descricao": "Armaf Club De Nuit Intense Man Eau De Toilette, 102 g"},
    {"ean": "4064666318233", "descricao": "Wella Invigo Fusion Shampoo 1L"},
    {"ean": "3474636977369", "descricao": "Loréal Absolut Repair Oil 10in1 - Óleo Reparador 90ml"},
    {"ean": "8005610531632", "descricao": "Wella Professionals Oil Reflections Luminous Reveal Shampoo 1 Litro"},
    {"ean": "7899706189620", "descricao": "L'oreal Pro Serie Exp Absolut Repair Gold Quinoa Sha 1500 Ml"},
    {"ean": "3349666007921", "descricao": "1 Million Masculino Eau de Toilette"},
    {"ean": "6291107456058", "descricao": "Lattafa Fakhar Gold Extrait Eau de Parfum 100Ml"},
    {"ean": "6290360593166", "descricao": "Lattafa Fakhar Gold Extrait Eau de Parfum 100Ml"},
    {"ean": "7898965429645", "descricao": "Skelt Amalfi Sunset Fine Hidratante Corporal 200g"},
    {"ean": "7899706189941", "descricao": "L'oreal Pro Serie Exp Absolut Repair Gold Quinoa Cond 1500ml"},
    {"ean": "7896235353720", "descricao": "Fusion - Condicionador 1000ml"},
    {"ean": "7899706196666", "descricao": "Vichy Neovadiol Menopausa Lifting Creme Facial 50g"},
    {"ean": "6297001158432", "descricao": "Orientica Azure Fantasy Perfume Árabe Extrait de Parfum Orientica Azure Fantasy Extrait de Parfum 80ml"},
    {"ean": "7896835805797", "descricao": "Deva Curl Supercream Creme De Coco Para Cachos 500G"},
    {"ean": "3349668589678", "descricao": "Invictus Masculino Eau de Toilette 200ml"},
    {"ean": "3337875543248", "descricao": "Vichy Minéral 89 Hidratante Facial 50ml"},
    {"ean": "3349668594412", "descricao": "Fame Paco Rabanne – Perfume Feminino – Eau de Parfum 80ml"},
    {"ean": "30158078", "descricao": "Loreal Professionnel Metal Detox Shampoo 300ml"},
    {"ean": "3701129800096", "descricao": "Bioderma Pigmentbio Sensitive Areas Serum Uniformizador Corporal Para Areas Intimas 75ml"},
    {"ean": "7896235353713", "descricao": "Wella Professionals Invigo Nutri-Enrich - Máscara de Nutrição 150ml"},
    {"ean": "7898556756303", "descricao": "Lowell Liso Mágico - Fluido Termoativado 200ml"},
    {"ean": "7792256767402", "descricao": "Cadiveu Glamour Rubi Shampoo+Condicionador 3L"},
    {"ean": "7898454151583", "descricao": "Femme9 Therapybrows - Sérum Acelerador de Crescimento da Sobrancelhas 10g"},
    {"ean": "7792252353500", "descricao": "Kit Wella Professionals Invigo Nutri-Enrich Shampoo Extra e Válvula (4 produtos)"},
    {"ean": "7907093652839", "descricao": "Kit Wella Professionals Oil Reflections Treatment Salon 2 Produtos"},
    {"ean": "1007931298970", "descricao": "Kit Wella Oil Reflections - Shampoo 1000 ml + Máscara 500 ml"},
    {"ean": "78950000533", "descricao": "Kit Wella Professionals Invigo Nutri Enrich - Shampoo 1000 ml + Condicionador 1000 ml + Máscara 150 ml"},
    {"ean": "1004108613628", "descricao": "Kit Wella Oil Reflections - Shampoo 250 ml + Condicionador 200 ml"},
    {"ean": "7898973417023", "descricao": "Óleo Capilar Sebastian Professional Dark Oil 95 ml"},
    {"ean": "7792255626304", "descricao": "Kit Braé Essential Shampoo + Condicionador + Máscara + Leave-in + Ampola Capilar (5 produtos)"},
    {"ean": "7898667820160", "descricao": "Gorgeous Shine Oil Revival 60ml Reparador, BRAÉ"},
    {"ean": "4064666306179", "descricao": "Wella Professionals Oil Reflections - Óleo Capilar 100ml"},
    {"ean": "7798448743193", "descricao": "Kit Truss Ultra Hydration Plus Duo (3 produtos)"},
    {"ean": "1009156004237", "descricao": "Kit de Cronograma Capilar Wella Professionals Profissional - 3 Produtos"},
    {"ean": "7795001511893", "descricao": "Kit L'Oréal Professionnel Absolut Repair Gold Shampoo Válvula Pump (2 produtos)"},
    {"ean": "7792255576432", "descricao": "Kit Skelt Amalfi Sunset Hidratante Corporal e Spray Perfumado (2 produtos)"},
    {"ean": "1004984939294", "descricao": "Kit Wella Professionals Fusion - Shampoo 1000 ml + Condicionador 1000 ml + Máscara 500 ml"},
    {"ean": "1006624920518", "descricao": "Kit Wella Professionals Fusion - Shampoo 1000 ml + Máscara 500 ml"},
    {"ean": "7702045446616", "descricao": "Senscience Inner Restore Mascara Capilar 500ml"},
    {"ean": "7500435132534", "descricao": "Mp144017 Fralda Pampers Premium Care Rn 36 Unidades"},
    {"ean": "1006800734823", "descricao": "Kit Wella Professionals Invigo Nutri Enrich - Shampoo 1000 ml + Máscara 500 ml"},
    {"ean": "7907093722648", "descricao": "Wella Professionals Kit Fusion Salon Duo de 1L"},
    {"ean": "8435415055956", "descricao": "Scandal Pour Homme Jean Paul Gaultier Eau De Toilette Refil Perfume Masculino 200ml"},
    {"ean": "7795000320748", "descricao": "Kit Braé Stages Nutrition Home Care (3 produtos)"},
    {"ean": "6012512766736", "descricao": "Pour Homme Versace Eau de Toilette Masculino-100 ml"},
    {"ean": "6291107459196", "descricao": "Mp438617 Perfume Arabe Maison Alhambra Delilah 100ml"},
    {"ean": "652418102281", "descricao": "N P P E Sh Rd Nutra Therapy Protein Cream Leave In 80ml"},
    {"ean": "78950000534", "descricao": "Kit Wella Professionals Shampoo Oil Reflections 1000 ml - 2 Unidades"},
    {"ean": "3474637217884", "descricao": "Loreal Professionnel Absolut Repair Molecular Mascara Capilar 250ml"},
    {"ean": "3616304175916", "descricao": "Gucci Guilty Pour Femme Perfume Feminino - Elixir de Parfum 60ml"},
    {"ean": "1001465472370", "descricao": "Kit Wella Oil Reflections - Shampoo 250 ml + Máscara 150 ml"},
    {"ean": "7908791000366", "descricao": "Creamy Antiaging Retinal Serum Facial 30g"},
    {"ean": "7893790609996", "descricao": "Leave-in Serie Expert Pro Longer 150ml - L'oreal"},
    {"ean": "6291106811568", "descricao": "Royal Amber Orientica Perfumes Eau De Parfum Perfume Feminino 80ml"},
    {"ean": "7896235353690", "descricao": "Wella Professionals Invigo Nutrienrich Condicionador - 1 Litro"},
    {"ean": "652418102137", "descricao": "N P P E Sh Rd Nutra Therapy Protein Cream Leave In 150ml"},
    {"ean": "1003958029603", "descricao": "Kit Wella Pro Fusion Sh1l+Cd1l"},
    {"ean": "8005610598635", "descricao": "Sebastian Professional Dark Oil Oleo Capilar 95ml"},
    {"ean": "1003008872234", "descricao": "Kit de Cronograma Capilar Wella Home Care - 3 Produtos"},
    {"ean": "3474636919963", "descricao": "Redken All Soft Shampoo 1l"},
    {"ean": "3614226905185", "descricao": "Perfume Burberry London Eau De Parfum Feminino 100ml"},
    {"ean": "7899706205177", "descricao": "Loreal Professionnel Serie Expert Nutrioil Leavein 150ml"},
    {"ean": "7907093733408", "descricao": "Kit Truss Equilibrium Duo 300ml (2 Produtos)"},
    {"ean": "7896235353829", "descricao": "Wella Professionals Invigo Balance Aqua Pure - Shampoo Antirresíduos 250ml"},
    {"ean": "8002135111974", "descricao": "Ferrari Black Eau de Toilette Masculino-125 ml"},
    {"ean": "7893697508968", "descricao": "Mp324990 Kit Pro Longer Shampoo Mascara E Leave In Loreal"},
    {"ean": "7792256847593", "descricao": "Kit Keune Care Keratin Smooth Duo (2 produtos)"},
    {"ean": "78950000603", "descricao": "Kit Wella Professionals Invigo Nutri Enrich Shampoo 1000 ml - 2 Unidades"},
    {"ean": "8432225043449", "descricao": "Revlon Professional Uniq One - Leave-in 150ml"},
    {"ean": "7794354285123", "descricao": "Truss Professional Night Spa Serum 250ml"},
    {"ean": "7792111462589", "descricao": "Mp262207 Truss Amino Liponutriente 225ml"},
    {"ean": "7792253994795", "descricao": "Kit Cadiveu Professional Nutri Glow Duo Grande (2 produtos)"},
    {"ean": "3349668596355", "descricao": "Paco Rabanne Phantom Eau de Toilette Refil - Perfume Masculino 200ml"},
    {"ean": "7908517905241", "descricao": "Vizzela Brow Up Fix Incolor Gel Fixador Para Sobrancelhas 3g"},
    {"ean": "7899706193658", "descricao": "La Rocheposay Pure Vitamin C10 Serum Facial 30ml"},
    {"ean": "7792257688652", "descricao": "Kit Senscience Inner Restore Máscara G e True Hue Óleo (2 produtos)"},
    {"ean": "7792254325208", "descricao": "Kit Redken All Soft Shampoo Litro e Máscara G (2 produtos)"},
    {"ean": "4064666317380", "descricao": "Sebastian Professional Penetraitt Mascara De Tratamento Reconstrutor 150ml"},
    {"ean": "74469483575", "descricao": "Senscience Inner Restore Mascara Capilar 500ml"},
    {"ean": "4064666040783", "descricao": "Wella Professionals Elements Renewing Shampoo 1l"},
    {"ean": "7895893115039", "descricao": "Brae Essential Hair Repair Spray 260ml (2 Unidades)"},
    {"ean": "3349668613588", "descricao": "Conjunto 1 Million Rabanne Masculino Perfume Eau De Toilette 200ml Travel Size 10ml"},
    {"ean": "7899706181631", "descricao": "Redken All Soft Heavy Cream Mascara Capilar 500ml"},
    {"ean": "7792256267216", "descricao": "Kit Braé Divine Shampoo e Condicionador (2 produtos)"},
    {"ean": "7893697509204", "descricao": "Mp310411 Kit Loreal Gold Quinoa Shampoo E Condicionador - 750ml"},
    {"ean": "3349668595945", "descricao": "Fame Rabanne Eau De Parfum Refil Perfume Feminino 200ml"},
    {"ean": "4064666319490", "descricao": "Máscara de Reconstrução Sebastian Professional Penetraitt 500 ml"},
    {"ean": "737052925028", "descricao": "Bamboo Gucci Eau De Parfum Perfume Feminino 30ml"},
    {"ean": "6291107456355", "descricao": "Ameerat Al Arab Asdaaf Lattafa Perfumes Eau De Parfum Perfume Feminino 100ml"},
    {"ean": "3616303470906", "descricao": "Flora Gorgeous Magnolia Gucci Eau De Parfum Perfume Feminino 50ml"},
    {"ean": "7898667822027", "descricao": "Brae Hair Protein Leavein 80g"},
    {"ean": "7898947943084", "descricao": "Truss Equilibrium Shampoo 300mll"},
    {"ean": "7896235353836", "descricao": "Wella Professionals Invigo Volume Boost Shampoo 1 Litro"},
    {"ean": "7898759913855", "descricao": "Kit L'Oréal Professionnel Serie Expert Vitamino Color – Shampoo 1500 ml + Condicionador 1500 ml"},
    {"ean": "7899706189767", "descricao": "Loreal Professionnel Serie Expert Vitamino Color Resveratrol Shampoo 15 Litro"},
    {"ean": "78950000612", "descricao": "Kit Wella Professionals Fusion Shampoo 1000 ml - 2 Un."},
    {"ean": "7792255216758", "descricao": "Wella Professionals Oil Reflections Luminous Reboost Restaure - Máscara Capilar 500ml"},
    {"ean": "7898667820986", "descricao": "Braé Beauty Sleep Night - Sérum Tratamento Noturno 100ml"},
    {"ean": "3474637217907", "descricao": "Loreal Professionnel Absolut Repair Molecular Mascara Capilar 500ml"},
    {"ean": "7898536549307", "descricao": "Deva Curl Styling Cream Creme Modelador 500g"},
    {"ean": "7899706205252", "descricao": "Loreal Professionnel Serie Expert Nutrioil Mascara Capilar 250g"},
    {"ean": "7798447882794", "descricao": "Truss Equilibrium Shampoo 300ml 2 Unidades"},
    {"ean": "8005610589374", "descricao": "Wella Professionals Eimi Thermal Image Protetor Termico 150ml"},
    {"ean": "7898623242012", "descricao": "Widi Care Condicionando a Juba Hidro-Nutritivo Condicionador - 500ml"},
    {"ean": "7898724572643", "descricao": "Gloss Fran By Franciny Ehlke Lip Bunny 5g Marrom"},
    {"ean": "7792255216277", "descricao": "Wella Professionals Fusion Intense Repair - Shampoo 1L"},
    {"ean": "7792255216130", "descricao": "Wella Invigo Color Brilliance Proteção da Cor - Shampoo 1L"},
    {"ean": "7792255275915", "descricao": "Brae Divine Anti Frizz Home Care Trio (3 Produtos)"},
    {"ean": "7897975696726", "descricao": "Adcos Collagen Colo E Pescoco Creme Redutor De Linhas 50g"},
    {"ean": "7898578153883", "descricao": "Escova Secadora Mondial ES-02-BI 1200W"},
    {"ean": "3616305267108", "descricao": "Kit Gucci Guilty - Eau de Parfum 90ml + Body Lotion 50ml + Travel Size 10ml Kit"},
    {"ean": "7792255635894", "descricao": "Pote Para Alimentos com 2 Divisórias Inteligentes | Tampa Hermética Antivazamento | Durável, Reutilizável e Ideal para Marmita, Lancheira ou Viagem"},
    {"ean": "896364002749", "descricao": "Olaplex No 3 Hair Perfector Tratamento Para Coloracao 100ml"},
    {"ean": "26256k", "descricao": "Parafuso Allen Recartilhado Sem Cabeca Jomarca 1/4X5/16 % 26256 - 1 Cento"},
    {"ean": "7907093652525", "descricao": "Kit Wella Professionals Invigo Nutri-Enrich Shampoo e Condicionador Litro (2 produtos)"},
    {"ean": "7794353994545", "descricao": "Sebastian Dark Oil Kit Shampoo 1 Litro + Condicionador 1 Litro"}
]

if __name__ == "__main__":
    for produto in produtos:
        ean = produto["ean"]
        descricao = produto["descricao"]
        asyncio.run(coleta_amazon(ean, descricao, cliente="Época Cosméticos", is_kit=None))