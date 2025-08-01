#!/usr/bin/env python3
"""
Script principal para executar o pipeline completo de dados do IBOV.
Executa em sequência:
1. 01_ibov_scraper.py - Scraping dos dados do IBOV
2. 02_csv_to_parquet.py - Conversão de CSV para Parquet
3. 03_s3_client.py - Upload para S3
4. 04_cleanup_data.py - Limpeza dos arquivos locais
"""

import subprocess
import sys
import logging
import os
from datetime import datetime
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


def run_script(script_path: str) -> bool:
    """
    Executa um script Python e retorna True se bem sucedido.
    
    Args:
        script_path: Caminho para o script Python
        
    Returns:
        bool: True se o script executou com sucesso, False caso contrário
    """
    try:
        script_name = os.path.basename(script_path)
        logger.info(f"Iniciando execução do script: {script_name}")
        print(f"\n{'='*60}")
        print(f"🚀 EXECUTANDO: {script_name}")
        print(f"{'='*60}")
        
        # Executar o script
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=os.path.dirname(os.path.abspath(__file__)) + "/..",
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos de timeout
        )
        
        # Log da saída
        if result.stdout:
            logger.info(f"Saída do {script_name}:\n{result.stdout}")
            print(result.stdout)
            
        if result.stderr:
            logger.warning(f"Erro/Aviso do {script_name}:\n{result.stderr}")
            print(f"Avisos/Erros:\n{result.stderr}")
        
        # Verificar se executou com sucesso
        if result.returncode == 0:
            logger.info(f"✓ Script {script_name} executado com sucesso!")
            print(f"✓ {script_name} - SUCESSO!")
            return True
        else:
            logger.error(f"✗ Script {script_name} falhou com código: {result.returncode}")
            print(f"✗ {script_name} - FALHOU!")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"✗ Script {script_name} excedeu o timeout de 5 minutos")
        print(f"✗ {script_name} - TIMEOUT!")
        return False
    except Exception as err:
        logger.error(f"✗ Erro ao executar {script_name}: {err}")
        print(f"✗ {script_name} - ERRO: {err}")
        return False


def main():
    """Executa o pipeline completo."""
    start_time = datetime.now()
    
    print("="*80)
    print("🎯 PIPELINE DE DADOS IBOV - INÍCIO")
    print("="*80)
    print(f"Data/Hora de início: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Lista dos scripts para executar em ordem
    scripts = [
        "src/01_ibov_scraper.py",
        "src/02_csv_to_parquet_glue.py",  # Usar versão compatível com AWS Glue
        "src/03_s3_client.py",
        "src/04_cleanup_data.py"
    ]
    
    # Verificar se todos os scripts existem
    base_dir = Path(__file__).parent.parent
    missing_scripts = []
    for script in scripts:
        script_path = base_dir / script
        if not script_path.exists():
            missing_scripts.append(script)
    
    if missing_scripts:
        logger.error(f"Scripts não encontrados: {missing_scripts}")
        print(f"✗ Erro: Scripts não encontrados: {missing_scripts}")
        return False
    
    # Executar scripts em sequência
    success_count = 0
    failed_scripts = []
    
    for script in scripts:
        script_path = base_dir / script
        if run_script(str(script_path)):
            success_count += 1
        else:
            failed_scripts.append(script)
            # Parar a execução se um script falhar
            logger.error(f"Pipeline interrompido devido a falha em: {script}")
            break
    
    # Relatório final
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*80)
    print("📊 RELATÓRIO FINAL DO PIPELINE")
    print("="*80)
    print(f"Data/Hora de início: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data/Hora de término: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duração total: {duration}")
    print(f"Scripts executados com sucesso: {success_count}/{len(scripts)}")
    
    if failed_scripts:
        print(f"Scripts que falharam: {failed_scripts}")
        logger.error(f"Pipeline finalizado com erros. Scripts que falharam: {failed_scripts}")
        return False
    else:
        print("✅ Todos os scripts executados com sucesso!")
        logger.info("Pipeline finalizado com sucesso!")
        return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
