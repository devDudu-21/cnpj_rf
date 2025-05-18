import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('database')

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

def sanitizar_tupla(tupla):
    """
    Remove caracteres nulos e outros caracteres problemáticos de uma tupla de strings
    
    Args:
        tupla: Tupla com strings para sanitizar
        
    Returns:
        Nova tupla com strings sanitizadas
    """
    resultado = []
    for item in tupla:
        if item is None:
            resultado.append("")
        elif isinstance(item, str):
            # Remover caracteres nulos (0x00) e outros caracteres de controle
            sanitizado = item
            for i in range(0, 32):  # Caracteres de controle ASCII 0-31
                if i != 9 and i != 10 and i != 13:  # Preservar tab (9), LF (10) e CR (13)
                    sanitizado = sanitizado.replace(chr(i), '')
            resultado.append(sanitizado)
        else:
            resultado.append(item)
    return tuple(resultado)

def testar_conexao():
    """
    Função para testar se o banco de dados está acessível.
    Retorna True se a conexão foi bem-sucedida, False caso contrário.
    """
    # Obter string de conexão do .env ou usar padrão
    conexao_str = os.getenv('DATABASE_URL')
    if not conexao_str:
        usuario = os.getenv('DB_USER', 'postgres')
        senha = os.getenv('DB_PASSWORD', 'postgres')
        host = os.getenv('DB_HOST', 'localhost')
        porta = os.getenv('DB_PORT', '5432')
        banco = os.getenv('DB_NAME', 'dados_cnpj')
        
        conexao_str = f"postgresql://{usuario}:{senha}@{host}:{porta}/{banco}"
    
    try:
        conn = psycopg2.connect(conexao_str)
        conn.close()
        logger.info("Teste de conexão concluído com sucesso!")
        return True
    except Exception as e:
        logger.error(f"Falha no teste de conexão: {e}")
        return False

def inicializar_banco_dados(conexao_str=None):
    """
    Inicializa o banco de dados PostgreSQL e cria as tabelas necessárias
    
    Args:
        conexao_str: String de conexão com o PostgreSQL
                    Formato: "postgresql://usuario:senha@host:porta/banco"
                    Se None, usa variáveis de ambiente do arquivo .env
    """
    # Se não for fornecida string de conexão, tenta usar variáveis de ambiente do .env
    if not conexao_str:
        # Verificar se existe DATABASE_URL configurado
        conexao_str = os.getenv('DATABASE_URL')
        
        # Se não existe DATABASE_URL, constrói a string a partir das outras variáveis
        if not conexao_str:
            usuario = os.getenv('DB_USER', 'postgres')
            senha = os.getenv('DB_PASSWORD', 'postgres')
            host = os.getenv('DB_HOST', 'localhost')
            porta = os.getenv('DB_PORT', '5432')
            banco = os.getenv('DB_NAME', 'dados_cnpj')
            
            conexao_str = f"postgresql://{usuario}:{senha}@{host}:{porta}/{banco}"
    
    try:
        # Log da tentativa de conexão
        logger.info(f"Tentando conectar ao PostgreSQL usando: {conexao_str.replace(os.getenv('DB_PASSWORD', 'postgres'), '*****')}")
        
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(conexao_str)
        conn.autocommit = True  # Para criar banco de dados se necessário
        
        # Log de conexão bem-sucedida
        db_info = conn.get_dsn_parameters()
        logger.info(f"Conexão estabelecida com sucesso! Banco: {db_info['dbname']}, Host: {db_info['host']}, Porta: {db_info['port']}")
        
        # Verificar se o banco de dados existe, se não existir, criar
        with conn.cursor() as cursor:
            # Verificar se a tabela empresas existe
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = 'empresas'
            );
            """)
            
            tabela_empresas_existe = cursor.fetchone()[0]
            
            if not tabela_empresas_existe:
                logger.info("Criando tabela empresas no banco de dados...")
                print("Criando tabela empresas no banco de dados...")
                
                # Criar tabela de empresas baseada na estrutura do arquivo EMPRECSV
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS empresas (
                    id SERIAL PRIMARY KEY,
                    cnpj_basico TEXT NOT NULL,
                    razao_social TEXT,
                    natureza_juridica TEXT,
                    qualificacao_responsavel TEXT,
                    capital_social NUMERIC,
                    porte_empresa TEXT,
                    ente_federativo TEXT
                )
                ''')
                
                # Criar índices para melhorar a performance de consultas
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_cnpj_basico_empresas ON empresas (cnpj_basico)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_razao_social ON empresas (razao_social)')
                
                logger.info("Tabela empresas criada com sucesso!")
                print("Tabela empresas criada com sucesso!")
            else:
                logger.info("Tabela empresas já existe no banco de dados")
                
            # Verificar se a tabela estabelecimentos existe
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = 'estabelecimentos'
            );
            """)
            
            tabela_estabelecimentos_existe = cursor.fetchone()[0]
            
            if not tabela_estabelecimentos_existe:
                logger.info("Criando tabela estabelecimentos no banco de dados...")
                print("Criando tabela estabelecimentos no banco de dados...")
                
                # Criar tabela de estabelecimentos baseada na estrutura do arquivo ESTABELE
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS estabelecimentos (
                    id SERIAL PRIMARY KEY,
                    cnpj_basico TEXT NOT NULL,
                    cnpj_ordem TEXT NOT NULL,
                    cnpj_dv TEXT NOT NULL,
                    identificador_matriz TEXT,
                    nome_fantasia TEXT,
                    situacao_cadastral TEXT,
                    data_situacao_cadastral TEXT,
                    cnae_principal TEXT,
                    tipo_logradouro TEXT,
                    logradouro TEXT,
                    bairro TEXT,
                    cep TEXT,
                    uf TEXT,
                    email TEXT
                )
                ''')
                
                # Criar índices para melhorar a performance de consultas
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_cnpj_comp ON estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_cnpj_basico ON estabelecimentos (cnpj_basico)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_nome_fantasia ON estabelecimentos (nome_fantasia)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_uf ON estabelecimentos (uf)')
                
                logger.info("Tabela estabelecimentos criada com sucesso!")
                print("Tabela estabelecimentos criada com sucesso!")
            else:
                logger.info("Tabela estabelecimentos já existe no banco de dados")
        
        # Desativar autocommit para as operações normais
        conn.autocommit = False
        
        # Log da configuração de autocommit
        logger.info("Configuração de conexão finalizada. Autocommit definido como False.")
        
        return conn
        
    except psycopg2.Error as e:
        # Log detalhado do erro
        erro_msg = f"Erro ao conectar ao PostgreSQL: {e}"
        logger.error(erro_msg)
        logger.error(f"Detalhes da conexão: Host={os.getenv('DB_HOST')}, Porta={os.getenv('DB_PORT')}, Banco={os.getenv('DB_NAME')}")
        print(erro_msg)
        raise
    
def inserir_empresas_lote(conn, empresas):
    """
    Insere múltiplas empresas no banco de dados PostgreSQL em uma única transação
    
    Args:
        conn: Conexão com o banco de dados
        empresas: Lista de tuplas com os dados das empresas
    """
    # Log de início da inserção em lote
    logger.info(f"Iniciando inserção em lote de {len(empresas)} registros...")
    
    try:
        # Sanitizar cada tupla antes da inserção
        empresas_sanitizadas = [sanitizar_tupla(emp) for emp in empresas]
        
        # Usar execute_values do psycopg2.extras para inserção em lote (muito mais eficiente)
        with conn.cursor() as cursor:
            # Inserir registros sem tentar resolver conflitos
            execute_values(
                cursor,
                '''
                INSERT INTO empresas 
                (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, 
                capital_social, porte_empresa, ente_federativo)
                VALUES %s
                ''',
                empresas_sanitizadas,
                template=None,  # Usa o template padrão do execute_values
                page_size=1000  # Processa em lotes de 1000 registros
            )
        
        # Commit da transação
        conn.commit()
        logger.info(f"Inserção de {len(empresas)} registros concluída com sucesso!")
    except Exception as e:
        # Log de erro na inserção
        logger.error(f"Erro ao inserir registros no banco: {e}")
        conn.rollback()  # Rollback em caso de erro
        raise

def inserir_estabelecimentos_lote(conn, estabelecimentos):
    """
    Insere múltiplos estabelecimentos no banco de dados PostgreSQL em uma única transação
    
    Args:
        conn: Conexão com o banco de dados
        estabelecimentos: Lista de tuplas com os dados dos estabelecimentos
    """
    # Log de início da inserção em lote
    logger.info(f"Iniciando inserção em lote de {len(estabelecimentos)} registros de estabelecimentos...")
    
    try:
        # Sanitizar cada tupla antes da inserção
        estabelecimentos_sanitizados = [sanitizar_tupla(est) for est in estabelecimentos]
        
        # Usar execute_values do psycopg2.extras para inserção em lote (muito mais eficiente)
        with conn.cursor() as cursor:
            # Inserir registros sem tentar resolver conflitos
            execute_values(
                cursor,
                '''
                INSERT INTO estabelecimentos 
                (cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz, nome_fantasia,
                situacao_cadastral, data_situacao_cadastral, cnae_principal,
                tipo_logradouro, logradouro, bairro, cep, uf, email)
                VALUES %s
                ''',
                estabelecimentos_sanitizados,
                template=None,  # Usa o template padrão do execute_values
                page_size=1000  # Processa em lotes de 1000 registros
            )
        
        # Commit da transação
        conn.commit()
        logger.info(f"Inserção de {len(estabelecimentos)} registros de estabelecimentos concluída com sucesso!")
    except Exception as e:
        # Log de erro na inserção
        logger.error(f"Erro ao inserir registros de estabelecimentos no banco: {e}")
        conn.rollback()  # Rollback em caso de erro
        raise
