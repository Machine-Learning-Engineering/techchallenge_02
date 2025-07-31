#!/usr/bin/env python3
"""
Script para limpar a pasta data após o upload dos arquivos para o MinIO.
Remove arquivos CSV e Parquet de forma segura.
"""

import os
import shutil
import logging
import sys
from pathlib import Path


# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def cleanup_data_folder(data_path: str = "data") -> bool:
    """
    Remove a pasta data e seu conteúdo de forma segura.
    
    Args:
        data_path: Caminho para a pasta data
        
    Returns:
        bool: True se a limpeza foi bem sucedida, False caso contrário
    """
    try:
        data_dir = Path(data_path)
        
        if not data_dir.exists():
            logger.info(f"Pasta '{data_path}' não existe. Nada para limpar.")
            return True
            
        # Listar arquivos antes da remoção
        logger.info(f"Listando arquivos em '{data_path}' antes da remoção:")
        for file_path in data_dir.glob('*'):
            logger.info(f"- {file_path.name}")
            
        # Remover a pasta e todo seu conteúdo
        logger.info(f"Removendo pasta '{data_path}' e seu conteúdo...")
        shutil.rmtree(data_path)
        
        # Verificar se a remoção foi bem sucedida
        if not data_dir.exists():
            logger.info(f"✓ Pasta '{data_path}' removida com sucesso!")
            return True
        else:
            logger.error(f"Falha ao remover pasta '{data_path}'")
            return False
            
    except Exception as err:
        logger.error(f"Erro ao limpar pasta '{data_path}': {err}")
        return False


def main():
    print("="*60)
    print("🧹 LIMPEZA DA PASTA DATA")
    print("="*60)
    
    data_path = "data"
    
    # Tentar limpar a pasta data
    if cleanup_data_folder(data_path):
        print(f"✓ Pasta '{data_path}' limpa com sucesso!")
    else:
        print(f"✗ Erro ao limpar pasta '{data_path}'. Verifique os logs.")


if __name__ == "__main__":
    main()
