#!/usr/bin/env python3
"""
Agendador para executar o pipeline de dados do IBOV.
Executa de segunda a sexta-feira às 20:00.
"""

import schedule
import time
import subprocess
import sys
import logging
import os
from datetime import datetime, date
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


def run_pipeline():
    """Executa o pipeline principal."""
    try:
        logger.info("Iniciando execução agendada do pipeline")
        print(f"\n{'='*80}")
        print(f"⏰ EXECUÇÃO AGENDADA - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
        # Caminho para o script principal
        base_dir = Path(__file__).parent
        pipeline_script = base_dir / "main_pipeline.py"
        
        if not pipeline_script.exists():
            logger.error(f"Script principal não encontrado: {pipeline_script}")
            print(f"✗ Erro: Script principal não encontrado: {pipeline_script}")
            return
        
        # Executar o pipeline
        result = subprocess.run(
            [sys.executable, str(pipeline_script)],
            cwd=str(base_dir.parent),
            capture_output=True,
            text=True
        )
        
        # Log da saída
        if result.stdout:
            logger.info(f"Saída do pipeline:\n{result.stdout}")
            print(result.stdout)
            
        if result.stderr:
            logger.warning(f"Erro/Aviso do pipeline:\n{result.stderr}")
            print(f"Avisos/Erros:\n{result.stderr}")
        
        # Verificar resultado
        if result.returncode == 0:
            logger.info("✅ Pipeline executado com sucesso!")
            print("✅ Pipeline executado com sucesso!")
        else:
            logger.error(f"❌ Pipeline falhou com código: {result.returncode}")
            print(f"❌ Pipeline falhou com código: {result.returncode}")
            
    except Exception as err:
        logger.error(f"Erro ao executar pipeline agendado: {err}")
        print(f"❌ Erro ao executar pipeline agendado: {err}")


def is_weekday():
    """Verifica se hoje é um dia útil (segunda a sexta)."""
    today = date.today()
    return today.weekday() < 5  # 0-4 são segunda a sexta


def job():
    """Job que será executado pelo agendador."""
    if is_weekday():
        logger.info("Dia útil detectado. Executando pipeline...")
        run_pipeline()
    else:
        logger.info("Fim de semana detectado. Pipeline não será executado.")
        print(f"🏖️  Fim de semana - Pipeline não executado ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")


def main():
    """Função principal do agendador."""
    print("="*80)
    print("📅 AGENDADOR DO PIPELINE IBOV")
    print("="*80)
    print("Configuração:")
    print("- Horário: 20:00 (8:00 PM)")
    print("- Dias: Segunda a Sexta-feira")
    print("- Timezone: Local do sistema")
    print("="*80)

    hora = os.environ.get('HORA_PIPELINE', '20:00')
    
    # Agendar para segunda a sexta às 20:00
    schedule.every().monday.at("20:00").do(job)
    schedule.every().tuesday.at("20:00").do(job)
    schedule.every().wednesday.at("20:00").do(job)
    schedule.every().thursday.at("20:00").do(job)
    schedule.every().friday.at("20:00").do(job)
    
    logger.info("Agendador iniciado. Pipeline será executado de segunda a sexta às 20:00")
    print(f"🚀 Agendador iniciado em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("⏰ Pipeline será executado de segunda a sexta às 20:00")
    print("📝 Logs serão salvos em scheduler.log")
    print("\nPressione Ctrl+C para parar o agendador\n")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Verificar a cada minuto
    except KeyboardInterrupt:
        logger.info("Agendador interrompido pelo usuário")
        print("\n👋 Agendador interrompido pelo usuário")


if __name__ == "__main__":
    main()
