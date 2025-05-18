import os
import argparse
import logging
from dotenv import load_dotenv
from app.database import testar_conexao
from app.download_data import baixar_arquivos_cnpj
from app.unzip_data import extrair_arquivos
from app.parse_csv import processar_csv_para_postgres, processar_estabelecimentos_csv

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('main')

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

def main():
    """
    Script principal para download e processamento de dados de CNPJ da Receita Federal.
    
    Este script orquestra o processo completo:
    1. Download dos arquivos ZIP da Receita Federal
    2. Extração dos arquivos EMPRECSV dos ZIPs
    3. Processamento dos arquivos CSV e carregamento no banco de dados PostgreSQL
    """
    # Configuração de diretórios
    base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-05/"
    caminho_zips = "./dados_cnpj_2025-05"
    caminho_extraidos = "./extraidos"
    
    # Configuração da conexão PostgreSQL - será buscada do arquivo .env
    conexao_str = os.getenv('DATABASE_URL')
    
    # Criar pasta para os ZIPs e extraídos, se não existirem
    os.makedirs(caminho_zips, exist_ok=True)
    os.makedirs(caminho_extraidos, exist_ok=True)
    
    # Testar conexão com o banco de dados
    logger.info("Testando conexão com o banco de dados PostgreSQL...")
    if testar_conexao():
        logger.info("✅ Conexão com o banco de dados estabelecida com sucesso!")
    else:
        logger.error("❌ Não foi possível conectar ao banco de dados PostgreSQL!")
        logger.error("Verifique se o PostgreSQL está em execução e as credenciais estão corretas.")
        print("\n❌ ERRO: Não foi possível conectar ao banco de dados PostgreSQL!")
        print("Verifique se o PostgreSQL está em execução e as credenciais estão corretas.")
        return
    
    # Parse argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Processamento de dados de CNPJ da Receita Federal')
    parser.add_argument('--skip-download', action='store_true', help='Pular o download dos arquivos')
    parser.add_argument('--skip-extract', action='store_true', help='Pular a extração dos arquivos')
    parser.add_argument('--skip-db', action='store_true', help='Pular o carregamento no banco de dados')
    parser.add_argument('--skip-empresas', action='store_true', help='Pular a inserção de empresas no banco de dados')
    parser.add_argument('--skip-estabelecimentos', action='store_true', help='Pular a inserção de estabelecimentos no banco de dados')
    parser.add_argument('--pausar-erro', action='store_true', help='Pausar após cada erro para permitir a interação do usuário')
    args = parser.parse_args()
    
    # Etapa 1: Download dos arquivos
    if not args.skip_download:
        print("\n📥 ETAPA 1: DOWNLOAD DOS ARQUIVOS")
        print("=" * 50)
        arquivos_baixados = baixar_arquivos_cnpj(base_url, caminho_zips)
        print(f"Arquivos baixados: {len(arquivos_baixados)}")
    else:
        print("\n📥 ETAPA 1: DOWNLOAD DOS ARQUIVOS [PULADO]")
    
    # Etapa 2: Extração dos arquivos EMPRECSV e ESTABELE
    if not args.skip_extract:
        print("\n📦 ETAPA 2: EXTRAÇÃO DOS ARQUIVOS DE EMPRESAS E ESTABELECIMENTOS")
        print("=" * 50)
        arquivos_por_tipo = extrair_arquivos(caminho_zips, caminho_extraidos, ["EMPRECSV", "ESTABELE"])
        print(f"Total de arquivos extraídos: {sum(len(arquivos) for arquivos in arquivos_por_tipo.values())}")
        for tipo, arquivos in arquivos_por_tipo.items():
            print(f"   - {len(arquivos)} arquivos {tipo}")
    else:
        print("\n📦 ETAPA 2: EXTRAÇÃO DOS ARQUIVOS [PULADO]")
    
    # Etapa 3: Processamento dos CSV e carregamento no banco de dados
    if not args.skip_db:
        print("\n💽 ETAPA 3: PROCESSAMENTO DOS DADOS E CARREGAMENTO NO BANCO PostgreSQL")
        print("=" * 50)
        
        total_empresas = 0
        total_estabelecimentos = 0
        arquivos_empresas = []
        arquivos_estabelecimentos = []
        
        # Processar empresas (se não for para pular)
        if not args.skip_empresas:
            print("\n   🏢 Processando dados de EMPRESAS...")
            total_empresas, arquivos_empresas = processar_csv_para_postgres(caminho_extraidos, conexao_str, dry_run=False)
            print(f"   ✅ Total de registros de empresas: {total_empresas}")
            print(f"   ✅ Arquivos de empresas processados: {len(arquivos_empresas)}")
        else:
            print("\n   🏢 Processando dados de EMPRESAS...[PULADO]")
            
        # Processar estabelecimentos (se não for para pular)
        if not args.skip_estabelecimentos:
            print("\n   🏪 Processando dados de ESTABELECIMENTOS...")
            total_estabelecimentos, arquivos_estabelecimentos = processar_estabelecimentos_csv(caminho_extraidos, conexao_str, dry_run=False)
            print(f"   ✅ Total de registros de estabelecimentos: {total_estabelecimentos}")
            print(f"   ✅ Arquivos de estabelecimentos processados: {len(arquivos_estabelecimentos)}")
        else:
            print("\n   🏪 Processando dados de ESTABELECIMENTOS...[PULADO]")


        # Total geral
        total_registros = total_empresas + total_estabelecimentos
        total_arquivos = len(arquivos_empresas) + len(arquivos_estabelecimentos)
        print(f"\n📊 TOTAL GERAL: {total_registros} registros em {total_arquivos} arquivos")
    else:
        print("\n💽 ETAPA 3: PROCESSAMENTO DOS DADOS E CARREGAMENTO NO BANCO [PULADO]")
    
    print("\n✅ PROCESSAMENTO CONCLUÍDO!")

if __name__ == "__main__":
    main()
