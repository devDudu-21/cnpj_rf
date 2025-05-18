import os
import csv
from app.database import inicializar_banco_dados, inserir_empresas_lote, inserir_estabelecimentos_lote

def limpar_string(valor):
    """
    Remove caracteres nulos e sanitiza a string para inserção no banco de dados
    
    Args:
        valor: String a ser limpa
        
    Returns:
        String limpa sem caracteres nulos ou problemáticos
    """
    if valor is None:
        return ""
    
    if not isinstance(valor, str):
        return str(valor)
    
    # Remover caracteres problemáticos
    # 0x00 (NUL) - Causa o erro "string literal cannot contain NUL (0x00) characters"
    # Outros caracteres de controle que podem causar problemas
    chars_problematicos = ['\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', 
                          '\x08', '\x0b', '\x0c', '\x0e', '\x0f', '\x10', '\x11', '\x12', 
                          '\x13', '\x14', '\x15', '\x16', '\x17', '\x18', '\x19', '\x1a', 
                          '\x1b', '\x1c', '\x1d', '\x1e', '\x1f']
    
    resultado = valor
    for char in chars_problematicos:
        resultado = resultado.replace(char, '')
    
    return resultado

def processar_csv_para_postgres(diretorio_csv, conexao_str=None, tamanho_lote=50000, dry_run=False):
    """
    Processa os arquivos CSV da pasta especificada e os carrega no banco PostgreSQL
    
    Args:
        diretorio_csv: Caminho para o diretório com os arquivos CSV
        conexao_str: String de conexão com o PostgreSQL
                    Formato: "postgresql://usuario:senha@host:porta/banco"
        tamanho_lote: Tamanho do lote para inserção em massa (para performance)
        dry_run: Se True, apenas conta os arquivos e registros, sem inserir no banco
    
    Returns:
        tuple: (total_registros, arquivos_processados)
    """
    # Inicializar banco de dados (apenas se não for dry_run)
    conn = None if dry_run else inicializar_banco_dados(conexao_str)
    
    # Contador de registros processados
    total_registros = 0
    arquivos_processados = []
    
    # Lista para acumular registros para inserção em lote
    lote_empresas = []
    
    # Processar cada arquivo CSV no diretório
    for arquivo in os.listdir(diretorio_csv):
        if arquivo.endswith(".EMPRECSV"):
            caminho_arquivo = os.path.join(diretorio_csv, arquivo)
            print(f"Processando arquivo: {arquivo}")
            
            try:
                with open(caminho_arquivo, 'r', encoding='latin-1') as f:
                    # Usar CSV DictReader para facilitar o mapeamento dos campos
                    reader = csv.reader(f, delimiter=';')
                    
                    for linha in reader:
                        # Validar se a linha tem dados suficientes
                        if len(linha) >= 7:
                            try:
                                # Converter o capital social para float
                                try:
                                    capital_social = float(linha[5].replace(',', '.'))
                                except (ValueError, IndexError):
                                    capital_social = 0.0
                                    
                                # Mapear as colunas do CSV para os campos da tabela
                                empresa = (
                                    limpar_string(linha[0]),                  # cnpj_basico
                                    limpar_string(linha[1]),                  # razao_social
                                    limpar_string(linha[2]) if len(linha) > 2 else "",  # natureza_juridica
                                    limpar_string(linha[3]) if len(linha) > 3 else "",  # qualificacao_responsavel
                                    capital_social,            # capital_social
                                    limpar_string(linha[6]) if len(linha) > 6 else "",  # porte_empresa
                                    limpar_string(linha[7]) if len(linha) > 7 else ""   # ente_federativo
                                )
                                
                                # Adicionar a empresa ao lote para inserção
                                lote_empresas.append(empresa)
                                
                                # Se atingiu o tamanho do lote, inserir no banco (se não for dry_run)
                                if len(lote_empresas) >= tamanho_lote:
                                    if not dry_run:
                                        inserir_empresas_lote(conn, lote_empresas)
                                    total_registros += len(lote_empresas)
                                    lote_empresas = []
                                    
                            except Exception as e:
                                print(f"Erro ao processar linha: {e}")
                                continue
                
                # Inserir o restante do lote, se houver (se não for dry_run)
                if lote_empresas:
                    if not dry_run:
                        inserir_empresas_lote(conn, lote_empresas)
                    total_registros += len(lote_empresas)
                    lote_empresas = []
                
                arquivos_processados.append(arquivo)
                    
            except Exception as e:
                print(f"Erro ao processar o arquivo {arquivo}: {e}")
    
    # Fechar a conexão com o banco
    conn.close()
    
    return total_registros, arquivos_processados

def processar_estabelecimentos_csv(diretorio_csv, conexao_str=None, tamanho_lote=50000, dry_run=False):
    """
    Processa os arquivos ESTABELE da pasta especificada e os carrega no banco PostgreSQL
    
    Args:
        diretorio_csv: Caminho para o diretório com os arquivos CSV
        conexao_str: String de conexão com o PostgreSQL
        tamanho_lote: Tamanho do lote para inserção em massa (para performance)
        dry_run: Se True, apenas conta os arquivos e registros, sem inserir no banco
    
    Returns:
        tuple: (total_registros, arquivos_processados)
    """
    # Inicializar banco de dados (apenas se não for dry_run)
    conn = None if dry_run else inicializar_banco_dados(conexao_str)
    
    # Contador de registros processados
    total_registros = 0
    arquivos_processados = []
    
    # Lista para acumular registros para inserção em lote
    lote_estabelecimentos = []
    
    # Processar cada arquivo ESTABELE no diretório
    for arquivo in os.listdir(diretorio_csv):
        if arquivo.endswith(".ESTABELE"):
            caminho_arquivo = os.path.join(diretorio_csv, arquivo)
            print(f"Processando arquivo: {arquivo}")
            
            try:
                with open(caminho_arquivo, 'r', encoding='latin-1') as f:
                    # Usar CSV reader para processar os dados
                    reader = csv.reader(f, delimiter=';', quotechar='"')
                    
                    for linha in reader:
                        # Validar se a linha tem dados suficientes
                        if len(linha) >= 14:  # Mínimo de 14 campos conforme especificação
                            try:
                                # Mapear as colunas do CSV para os campos da tabela
                                # Conforme o exemplo do arquivo ESTABELE:
                                # "44778741";"0001";"73";"1";...
                                # Verificar e tratar cada campo para garantir que não haja caracteres nulos (0x00)
                                # e que os índices estão dentro da faixa
                                estabelecimento = (
                                    limpar_string(linha[0]) if len(linha) > 0 else "",                  # cnpj_basico
                                    limpar_string(linha[1]) if len(linha) > 1 else "",                  # cnpj_ordem
                                    limpar_string(linha[2]) if len(linha) > 2 else "",                  # cnpj_dv
                                    limpar_string(linha[3]) if len(linha) > 3 else "",                  # identificador_matriz (1=matriz, 2=filial)
                                    limpar_string(linha[4]) if len(linha) > 4 else "",                  # nome_fantasia
                                    limpar_string(linha[5]) if len(linha) > 5 else "",                  # situacao_cadastral
                                    limpar_string(linha[6]) if len(linha) > 6 else "",                  # data_situacao_cadastral
                                    limpar_string(linha[11]) if len(linha) > 11 else "",                # cnae_principal
                                    limpar_string(linha[14]) if len(linha) > 14 else "",                # tipo_logradouro
                                    limpar_string(linha[15]) if len(linha) > 15 else "",                # logradouro
                                    limpar_string(linha[19]) if len(linha) > 19 else "",                # bairro
                                    limpar_string(linha[20]) if len(linha) > 20 else "",                # cep
                                    limpar_string(linha[21]) if len(linha) > 21 else "",                # uf
                                    limpar_string(linha[28]) if len(linha) > 28 else "",                # email
                                )
                                
                                # Adicionar o estabelecimento ao lote para inserção
                                lote_estabelecimentos.append(estabelecimento)
                                
                                # Se atingiu o tamanho do lote, inserir no banco (se não for dry_run)
                                if len(lote_estabelecimentos) >= tamanho_lote:
                                    if not dry_run:
                                        inserir_estabelecimentos_lote(conn, lote_estabelecimentos)
                                    total_registros += len(lote_estabelecimentos)
                                    lote_estabelecimentos = []
                                    
                            except Exception as e:
                                print(f"Erro ao processar linha de estabelecimento: {e}")
                                continue
                
                # Inserir o restante do lote, se houver (se não for dry_run)
                if lote_estabelecimentos:
                    if not dry_run:
                        inserir_estabelecimentos_lote(conn, lote_estabelecimentos)
                    total_registros += len(lote_estabelecimentos)
                    lote_estabelecimentos = []
                
                arquivos_processados.append(arquivo)
                    
            except Exception as e:
                print(f"Erro ao processar o arquivo {arquivo}: {e}")
    
    # Fechar a conexão com o banco (se não for dry_run)
    if not dry_run and conn:
        conn.close()
    
    return total_registros, arquivos_processados
