# Script para verificar a conectividade com o banco de dados
# filepath: /home/devdudu/workplace/cnpj-sqlite/check_db.py

import os
import sys
import logging
from dotenv import load_dotenv
from app.database import inicializar_banco_dados

# Configurar logging específico para este script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('db_connection_check.log')
    ]
)
logger = logging.getLogger('db_checker')

def main():
    """
    Script para testar a conexão com o banco de dados PostgreSQL.
    """
    logger.info("=== Iniciando teste de conexão com o banco de dados ===")
    
    # Carregar variáveis de ambiente
    load_dotenv()
    
    # Obter string de conexão do .env ou usar padrão
    conexao_str = os.getenv('DATABASE_URL')
    if not conexao_str:
        usuario = os.getenv('DB_USER', 'postgres')
        senha = os.getenv('DB_PASSWORD', 'postgres')
        host = os.getenv('DB_HOST', 'localhost')
        porta = os.getenv('DB_PORT', '5432')
        banco = os.getenv('DB_NAME', 'dados_cnpj')
        
        logger.info(f"Usando configurações: Host={host}, Porta={porta}, Banco={banco}, Usuário={usuario}")
        conexao_str = f"postgresql://{usuario}:{senha}@{host}:{porta}/{banco}"
    
    try:
        # Tentar conectar ao banco
        logger.info("Tentando conectar ao banco de dados...")
        conn = inicializar_banco_dados(conexao_str)
        
        # Executar uma consulta simples para testar
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"Conexão estabelecida com sucesso! Versão do PostgreSQL: {version[0]}")
            
            # Verificar tabelas existentes
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tabelas = cursor.fetchall()
            logger.info(f"Tabelas existentes no banco: {[t[0] for t in tabelas]}")
        
        # Fechar conexão
        conn.close()
        logger.info("Teste de conexão concluído com sucesso!")
        return True
        
    except Exception as e:
        logger.error(f"Falha na conexão com o banco de dados: {e}")
        logger.error("Verifique se o PostgreSQL está rodando e as credenciais estão corretas")
        return False

if __name__ == "__main__":
    result = main()
    sys.exit(0 if result else 1)
