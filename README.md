# Processador de Dados de CNPJ

Este projeto baixa, extrai e processa dados de CNPJ da Receita Federal, armazenando-os em um banco de dados PostgreSQL.

## Configuração

1. Clone este repositório
2. Instale as dependências:

   ````shell
   pip install -r requirements.txt
   ```shell

   ````

3. Configure as variáveis de ambiente copiando o arquivo de exemplo:

   ```shell
   cp .env.example .env
   ```

4. Edite o arquivo `.env` com suas configurações do PostgreSQL

## Uso

Execute o script principal:

```shell
python main.py
```

Opções disponíveis:

- `--skip-download`: Pula o download dos arquivos
- `--skip-extract`: Pula a extração dos arquivos
- `--skip-db`: Pula o processamento e carregamento no banco de dados

## Estrutura do Projeto

- `app/main.py`: Script principal que coordena todo o processo
- `app/download_data.py`: Módulo para download dos arquivos da Receita Federal
- `app/unzip_data.py`: Módulo para extração dos arquivos EMPRECSV
- `app/parse_csv.py`: Módulo para processamento dos arquivos CSV
- `app/database.py`: Módulo de comunicação com o banco de dados PostgreSQL

## Requisitos

- Python 3.8+
- PostgreSQL 12+
