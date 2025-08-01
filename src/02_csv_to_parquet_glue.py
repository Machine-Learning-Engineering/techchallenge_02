#!/usr/bin/env python3
"""
Conversor CSV para Parquet otimizado para AWS Glue.
Corrige problemas de compatibilidade com Spark/Glue.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_glue_compatible_schema(df: pd.DataFrame) -> pa.Schema:
    """
    Cria um schema PyArrow compatível com AWS Glue/Spark.
    """
    fields = []
    
    for col in df.columns:
        dtype = df[col].dtype
        
        # Tratamento especial para data_coleta que agora é string AAAAMMDD
        if col == 'data_coleta':
            field = pa.field(col, pa.string())
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            # Outros timestamps devem ser TIMESTAMP_MILLIS para compatibilidade com Spark
            field = pa.field(col, pa.timestamp('ms'))
        elif pd.api.types.is_integer_dtype(dtype):
            # Usar int64 para inteiros
            field = pa.field(col, pa.int64())
        elif pd.api.types.is_float_dtype(dtype):
            # Usar double para floats
            field = pa.field(col, pa.float64())
        elif pd.api.types.is_bool_dtype(dtype):
            # Boolean
            field = pa.field(col, pa.bool_())
        else:
            # String para tudo mais
            field = pa.field(col, pa.string())
        
        fields.append(field)
    
    return pa.schema(fields)


def convert_csv_to_glue_parquet(csv_file_path: str, output_dir: str = None) -> str:
    """
    Converte CSV para Parquet compatível com AWS Glue.
    """
    try:
        # Definir diretório de saída
        if output_dir is None:
            output_dir = os.path.dirname(csv_file_path)
        
        # Criar nome do arquivo Parquet
        base_name = os.path.splitext(os.path.basename(csv_file_path))[0]
        parquet_file_path = os.path.join(output_dir, f"{base_name}.parquet")
        
        logger.info(f"Convertendo para AWS Glue: {csv_file_path}")
        
        # Ler CSV
        df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
        logger.info(f"  Linhas: {len(df)}, Colunas: {len(df.columns)}")
        
        # Processamento específico para dados IBOV
        if 'data_coleta' in df.columns:
            # Converter para datetime primeiro para garantir parsing correto
            df['data_coleta'] = pd.to_datetime(df['data_coleta'], errors='coerce')
            # Converter para formato AAAAMMDD (string) ignorando hora
            df['data_coleta'] = df['data_coleta'].dt.strftime('%Y%m%d')
            # Preencher valores nulos com data atual em formato AAAAMMDD
            current_date = datetime.now().strftime('%Y%m%d')
            df['data_coleta'] = df['data_coleta'].fillna(current_date)
        
        # Limpar e converter campos numéricos
        numeric_fields = ['participacao_pct', 'preco', 'variacao', 'volume', 'quantidade_teorica']
        for field in numeric_fields:
            if field in df.columns:
                if field == 'participacao_pct':
                    # Remover % e converter para decimal
                    df[field] = df[field].astype(str).str.replace(',', '.').str.replace('%', '')
                    df[field] = pd.to_numeric(df[field], errors='coerce')
                elif field in ['quantidade_teorica', 'volume']:
                    # Limpar separadores de milhares
                    df[field] = df[field].astype(str).str.replace('.', '').str.replace(',', '.')
                    df[field] = pd.to_numeric(df[field], errors='coerce')
                else:
                    # Campos como preco, variacao
                    df[field] = df[field].astype(str).str.replace(',', '.')
                    df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Garantir que campos de texto sejam strings válidas
        text_fields = ['codigo', 'nome', 'empresa', 'tipo', 'url_fonte', 'data_coleta']
        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).replace('nan', '')
        
        # Remover linhas com dados inválidos críticos
        if 'codigo' in df.columns:
            df = df[df['codigo'].notna() & (df['codigo'] != '') & (df['codigo'] != 'nan')]
        
        logger.info(f"  Após limpeza: {len(df)} linhas")
        
        # Criar schema compatível com Glue
        schema = create_glue_compatible_schema(df)
        
        # Converter para Arrow Table
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        
        # Salvar com configurações otimizadas para AWS Glue
        pq.write_table(
            table,
            parquet_file_path,
            compression='snappy',
            use_deprecated_int96_timestamps=False,  # Usar timestamp padrão
            coerce_timestamps='ms',  # Forçar precisão de milissegundos
            allow_truncated_timestamps=True,
            write_statistics=True,  # Habilitar estatísticas para otimização
            use_dictionary=True,    # Usar dicionários para strings repetitivas
            row_group_size=50000   # Tamanho de row group otimizado
        )
        
        # Verificar tamanhos
        csv_size = os.path.getsize(csv_file_path)
        parquet_size = os.path.getsize(parquet_file_path)
        reduction = ((csv_size - parquet_size) / csv_size) * 100
        
        logger.info(f"  ✅ Arquivo AWS Glue compatível: {parquet_file_path}")
        logger.info(f"  📊 CSV: {csv_size:,} bytes → Parquet: {parquet_size:,} bytes")
        logger.info(f"  📈 Redução: {reduction:.1f}%")
        
        # Validar arquivo criado
        validate_parquet_file(parquet_file_path)
        
        return parquet_file_path
        
    except Exception as e:
        logger.error(f"❌ Erro ao converter {csv_file_path}: {e}")
        return ""


def validate_parquet_file(parquet_path: str) -> bool:
    """
    Valida se o arquivo Parquet está correto e compatível com Glue.
    """
    try:
        # Tentar ler o arquivo
        table = pq.read_table(parquet_path)
        
        # Verificar schema
        schema = table.schema
        logger.info(f"  🔍 Schema validado: {len(schema)} campos")
        
        # Verificar tipos problemáticos
        for field in schema:
            if field.type == pa.timestamp('ns'):
                logger.warning(f"  ⚠️  Campo {field.name} usa timestamp nanosegundos (pode causar problemas no Glue)")
                return False
            elif str(field.type).startswith('timestamp') and 'ns' in str(field.type):
                logger.warning(f"  ⚠️  Campo {field.name} tem timestamp incompatível: {field.type}")
                return False
        
        # Verificar se pode ser convertido para pandas (teste de compatibilidade)
        df_test = table.to_pandas()
        logger.info(f"  ✅ Conversão pandas OK: {len(df_test)} registros")
        
        return True
        
    except Exception as e:
        logger.error(f"  ❌ Erro na validação: {e}")
        return False


def convert_all_csv_for_glue(directory: str = "data") -> list:
    """
    Converte todos os CSV para Parquet compatível com AWS Glue.
    """
    if not os.path.exists(directory):
        logger.error(f"Diretório '{directory}' não encontrado!")
        return []
    
    # Buscar arquivos CSV
    csv_pattern = os.path.join(directory, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        logger.warning(f"Nenhum arquivo CSV encontrado em '{directory}'")
        return []
    
    logger.info(f"🔄 Convertendo {len(csv_files)} arquivo(s) para AWS Glue compatível")
    
    converted_files = []
    
    for csv_file in csv_files:
        parquet_file = convert_csv_to_glue_parquet(csv_file)
        if parquet_file:
            converted_files.append(parquet_file)
    
    return converted_files


def main():
    """
    Função principal - conversão otimizada para AWS Glue.
    """
    print("="*70)
    print("🔄 CONVERSOR CSV → PARQUET (AWS GLUE COMPATÍVEL)")
    print("="*70)
    
    try:
        # Verificar dependências
        logger.info("Verificando dependências...")
        import pyarrow
        logger.info(f"✅ PyArrow {pyarrow.__version__} disponível")
        
        # Converter arquivos
        converted_files = convert_all_csv_for_glue("data")
        
        if converted_files:
            print(f"\n✅ CONVERSÃO CONCLUÍDA!")
            print(f"📊 Arquivos convertidos: {len(converted_files)}")
            
            print(f"\n📋 Arquivos Parquet compatíveis com AWS Glue:")
            total_size = 0
            for file_path in converted_files:
                file_size = os.path.getsize(file_path)
                total_size += file_size
                print(f"  • {os.path.basename(file_path)} ({file_size:,} bytes)")
            
            print(f"\n📈 Tamanho total: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
            
            print(f"\n🎯 COMPATIBILIDADE AWS GLUE:")
            print("  ✅ Timestamps em milissegundos")
            print("  ✅ Schema compatível com Spark")
            print("  ✅ Compressão Snappy")
            print("  ✅ Estatísticas habilitadas")
            print("  ✅ Sem precisão de nanossegundos")
            
            print(f"\n💡 Próximos passos:")
            print("  1. Upload para MinIO via script 03_minio_client.py")
            print("  2. Usar no AWS Glue sem erros de compatibilidade")
            
        else:
            print("❌ Nenhum arquivo foi convertido")
            print("💡 Verifique se existem arquivos CSV na pasta 'data'")
    
    except ImportError:
        print("❌ PyArrow não instalado!")
        print("💡 Execute: pip install pyarrow")
    except Exception as e:
        logger.error(f"Erro durante conversão: {e}")
        print(f"❌ Erro: {e}")
    
    print("="*70)


if __name__ == "__main__":
    main()
