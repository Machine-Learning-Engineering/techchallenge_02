#!/usr/bin/env python3
"""
Script para executar o pipeline manualmente (teste).
Executa todos os scripts em sequência sem agendamento.
"""

import sys
import os
from pathlib import Path

# Adicionar o diretório src ao path para importar o main_pipeline
sys.path.append(str(Path(__file__).parent))

try:
    from main_pipeline import main
    
    if __name__ == "__main__":
        print("🧪 EXECUTANDO PIPELINE MANUALMENTE (TESTE)")
        print("="*60)
        success = main()
        if success:
            print("\n✅ Pipeline executado com sucesso!")
            sys.exit(0)
        else:
            print("\n❌ Pipeline falhou!")
            sys.exit(1)
            
except ImportError as e:
    print(f"❌ Erro ao importar main_pipeline: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Erro inesperado: {e}")
    sys.exit(1)
