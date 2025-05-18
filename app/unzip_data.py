import os
import zipfile

def extrair_arquivos(pasta_zips, pasta_saida="./extraidos", padroes=["EMPRECSV", "ESTABELE"]):
    """
    Extrai arquivos específicos (EMPRECSV, ESTABELE, etc) dos ZIPs baixados
    
    Args:
        pasta_zips: Diretório com os arquivos ZIP
        pasta_saida: Diretório onde os arquivos serão extraídos
        padroes: Lista de padrões para identificar os arquivos a serem extraídos
    
    Returns:
        dict: Dicionário com os arquivos extraídos por tipo
    """
    os.makedirs(pasta_saida, exist_ok=True)
    arquivos_por_tipo = {padrao: [] for padrao in padroes}
    total_extraidos = 0

    for nome_arquivo in os.listdir(pasta_zips):
        caminho_zip = os.path.join(pasta_zips, nome_arquivo)

        if not zipfile.is_zipfile(caminho_zip):
            continue

        try:
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                for arquivo in zip_ref.namelist():
                    for padrao in padroes:
                        if padrao in arquivo.upper():
                            print(f"🗜️  Extraindo {arquivo} de {nome_arquivo}")
                            zip_ref.extract(arquivo, path=pasta_saida)
                            caminho_extraido = os.path.join(pasta_saida, arquivo)
                            arquivos_por_tipo[padrao].append(caminho_extraido)
                            total_extraidos += 1
                            break  # Sai do loop de padrões após encontrar um match
        except zipfile.BadZipFile:
            print(f"⚠️ Arquivo ZIP corrompido: {nome_arquivo}")
        except Exception as e:
            print(f"⚠️ Erro ao extrair {nome_arquivo}: {e}")

    # Resumo de extração por tipo
    print(f"\n✅ Extração finalizada. {total_extraidos} arquivos extraídos:")
    for padrao, arquivos in arquivos_por_tipo.items():
        print(f"   - {len(arquivos)} arquivos {padrao}")
    
    return arquivos_por_tipo

# Função legada para manter compatibilidade com código existente
def extrair_emprecsv(pasta_zips, pasta_saida="./extraidos"):
    """
    Função para extrair apenas arquivos EMPRECSV (mantida para compatibilidade)
    """
    resultado = extrair_arquivos(pasta_zips, pasta_saida, ["EMPRECSV"])
    return resultado["EMPRECSV"]
