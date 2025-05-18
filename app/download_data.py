import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# URL base para download dos arquivos da Receita Federal
base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-05/"

# Pasta onde os arquivos serão salvos
output_dir = "./dados_cnpj_2025-05"
os.makedirs(output_dir, exist_ok=True)

# Função para baixar todos os arquivos .zip com "Empresas" e "Estabelecimentos" no nome
def baixar_arquivos_cnpj(base_url, destino, tipos=['Empresas', 'Estabelecimentos']):
    print(f"\n🔍 Acessando {base_url}")
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    arquivos_baixados = []
    arquivos_ignorados = []

    links = [
        link.get('href') for link in soup.find_all('a')
        if link.get('href') and link.get('href').endswith('.zip') and 
        any(tipo in link.get('href') for tipo in tipos)
    ]

    print(f"📦 {len(links)} arquivos encontrados para download.")

    # Organizar os links por tipo para melhor visualização no download
    links_por_tipo = {}
    for href in links:
        for tipo in tipos:
            if tipo in href:
                if tipo not in links_por_tipo:
                    links_por_tipo[tipo] = []
                links_por_tipo[tipo].append(href)
                break
    
    # Mostrar quantidade por tipo
    for tipo, lista_links in links_por_tipo.items():
        print(f"   - {len(lista_links)} arquivos de '{tipo}'")
    
    for href in links:
        arquivo_url = urljoin(base_url, href)
        nome_arquivo = os.path.join(destino, os.path.basename(href))

        if os.path.exists(nome_arquivo):
            print(f"🟡 Arquivo já existe, ignorando: {href}")
            arquivos_ignorados.append(nome_arquivo)
            continue

        print(f"⬇️  Baixando: {href}")
        with requests.get(arquivo_url, stream=True) as r:
            r.raise_for_status()
            with open(nome_arquivo, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        arquivos_baixados.append(nome_arquivo)

    # Mostrar resumo por tipo
    arquivos_por_tipo = {}
    for arquivo in arquivos_baixados:
        nome_base = os.path.basename(arquivo)
        for tipo in tipos:
            if tipo in nome_base:
                if tipo not in arquivos_por_tipo:
                    arquivos_por_tipo[tipo] = 0
                arquivos_por_tipo[tipo] += 1
                break
    
    print(f"\n✅ Download finalizado. {len(arquivos_baixados)} novos arquivos baixados:")
    for tipo, quantidade in arquivos_por_tipo.items():
        print(f"   - {quantidade} arquivos de '{tipo}'")
        
    if arquivos_ignorados:
        print(f"🔁 {len(arquivos_ignorados)} arquivos já existiam e foram ignorados.")
    return arquivos_baixados

# Executar
if __name__ == "__main__":
    # Por padrão, baixa arquivos de Empresas e Estabelecimentos
    baixar_arquivos_cnpj(base_url, output_dir)
    
    # Para baixar apenas um tipo específico, descomente e use uma das linhas abaixo:
    # baixar_arquivos_cnpj(base_url, output_dir, tipos=['Empresas'])
    # baixar_arquivos_cnpj(base_url, output_dir, tipos=['Estabelecimentos'])
