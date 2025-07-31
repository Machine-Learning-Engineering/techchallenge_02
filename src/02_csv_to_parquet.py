#!/usr/bin/env python3
"""
Conversor de arquivos CSV para Parquet.
Converte todos os arquivos CSV da pasta 'data' para formato Parquet.
"""

import os
import pandas as pd
import glob
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_csv_to_parquet(csv_file_path: str, output_dir: str = None) -> str:
    """
    Converte um arquivo CSV para Parquet.
    
    Args:
        csv_file_path: Caminho para o arquivo CSV
        output_dir: Diretório de saída (opcional)
        
    Returns:
        Caminho do arquivo Parquet criado
    """
    try:
        # Definir diretório de saída
        if output_dir is None:
            output_dir = os.path.dirname(csv_file_path)
        
        # Criar nome do arquivo Parquet
        base_name = os.path.splitext(os.path.basename(csv_file_path))[0]
        parquet_file_path = os.path.join(output_dir, f"{base_name}.parquet")
        
        logger.info(f"Convertendo: {csv_file_path}")
        
        # Ler CSV
        df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
        
        # Informações sobre o DataFrame
        logger.info(f"  Linhas: {len(df)}, Colunas: {len(df.columns)}")
        
        # Otimizações de tipos de dados para reduzir tamanho
        if 'data_coleta' in df.columns:
            # Converter data_coleta para datetime
            df['data_coleta'] = pd.to_datetime(df['data_coleta'], errors='coerce')
        
        if 'participacao_pct' in df.columns:
            # Limpar e converter participação para float
            df['participacao_pct'] = df['participacao_pct'].astype(str).str.replace(',', '.').str.replace('%', '')
            df['participacao_pct'] = pd.to_numeric(df['participacao_pct'], errors='coerce')
        
        if 'quantidade_teorica' in df.columns:
            # Limpar e converter quantidade para numeric
            df['quantidade_teorica'] = df['quantidade_teorica'].astype(str).str.replace('.', '').str.replace(',', '.')
            df['quantidade_teorica'] = pd.to_numeric(df['quantidade_teorica'], errors='coerce')
        
        # Otimizar tipos de string
        for col in ['codigo', 'empresa', 'tipo', 'url_fonte']:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        # Salvar como Parquet
        df.to_parquet(
            parquet_file_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        # Verificar tamanhos
        csv_size = os.path.getsize(csv_file_path)
        parquet_size = os.path.getsize(parquet_file_path)
        reduction = ((csv_size - parquet_size) / csv_size) * 100
        
        logger.info(f"  Arquivo salvo: {parquet_file_path}")
        logger.info(f"  Tamanho CSV: {csv_size:,} bytes")
        logger.info(f"  Tamanho Parquet: {parquet_size:,} bytes")
        logger.info(f"  Redução: {reduction:.1f}%")
        
        return parquet_file_path
        
    except Exception as e:
        logger.error(f"Erro ao converter {csv_file_path}: {e}")
        return ""


def convert_all_csv_in_directory(directory: str = "data") -> list:
    """
    Converte todos os arquivos CSV de um diretório para Parquet.
    
    Args:
        directory: Diretório contendo os arquivos CSV
        
    Returns:
        Lista de arquivos Parquet criados
    """
    if not os.path.exists(directory):
        logger.error(f"Diretório '{directory}' não encontrado!")
        return []
    
    # Buscar todos os arquivos CSV
    csv_pattern = os.path.join(directory, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        logger.warning(f"Nenhum arquivo CSV encontrado em '{directory}'")
        return []
    
    logger.info(f"Encontrados {len(csv_files)} arquivo(s) CSV para converter")
    
    converted_files = []
    
    for csv_file in csv_files:
        parquet_file = convert_csv_to_parquet(csv_file)
        if parquet_file:
            converted_files.append(parquet_file)
    
    return converted_files


def main():
    """
    Função principal para converter CSV para Parquet.
    """
    print("="*60)
    print("🔄 CONVERSOR CSV PARA PARQUET")
    print("📁 Convertendo arquivos da pasta 'data'")
    print("="*60)
    
    try:
        # Verificar se pandas tem suporte a Parquet
        try:
            import pyarrow
            logger.info("✅ PyArrow disponível para conversão Parquet")
        except ImportError:
            logger.error("❌ PyArrow não encontrado. Instalando...")
            import subprocess
            subprocess.check_call(["pip", "install", "pyarrow"])
            import pyarrow
            logger.info("✅ PyArrow instalado com sucesso")
        
        # Converter todos os CSV
        converted_files = convert_all_csv_in_directory("data")
        
        if converted_files:
            print(f"\n✅ CONVERSÃO CONCLUÍDA COM SUCESSO!")
            print(f"📊 Total de arquivos convertidos: {len(converted_files)}")
            
            print(f"\n📋 Arquivos Parquet criados:")
            for file_path in converted_files:
                file_size = os.path.getsize(file_path)
                print(f"  • {os.path.basename(file_path)} ({file_size:,} bytes)")
            
            # Mostrar comparação de tamanhos total
            csv_files = glob.glob("data/*.csv")
            total_csv_size = sum(os.path.getsize(f) for f in csv_files if os.path.exists(f))
            total_parquet_size = sum(os.path.getsize(f) for f in converted_files)
            
            if total_csv_size > 0:
                total_reduction = ((total_csv_size - total_parquet_size) / total_csv_size) * 100
                print(f"\n📈 Estatísticas de compressão:")
                print(f"  • Tamanho total CSV: {total_csv_size:,} bytes")
                print(f"  • Tamanho total Parquet: {total_parquet_size:,} bytes")
                print(f"  • Redução total: {total_reduction:.1f}%")
            
            print("\n💡 Vantagens do formato Parquet:")
            print("  • Compressão eficiente (menor tamanho)")
            print("  • Leitura mais rápida")
            print("  • Preserva tipos de dados")
            print("  • Suporte nativo em muitas ferramentas de análise")
            
        else:
            print("❌ Nenhum arquivo foi convertido.")
            print("💡 Verifique se existem arquivos CSV na pasta 'data'")
    
    except Exception as e:
        logger.error(f"Erro durante conversão: {e}")
        print(f"❌ Erro: {e}")
    
    print("="*60)


if __name__ == "__main__":
    main()
