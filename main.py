#!/usr/bin/env python3
# filepath: /home/devdudu/workplace/cnpj-sqlite/main.py

"""
Ponto de entrada principal para o processamento de dados de CNPJ.
"""

import sys
import os

# Adiciona o diretório atual ao PYTHONPATH para poder importar os módulos da aplicação
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Importa a função main do módulo app.main
from app.main import main

if __name__ == "__main__":
    main()
