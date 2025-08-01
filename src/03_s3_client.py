import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import os
import logging
import sys
from datetime import datetime
import glob


# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "ibovtech")



def connect_s3():
    """Cria e retorna um cliente S3 da AWS."""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_DEFAULT_REGION
        )
        
        # Testar a conexão listando buckets
        s3_client.list_buckets()
        logger.info("Conexão com AWS S3 estabelecida com sucesso")
        return s3_client
        
    except NoCredentialsError:
        logger.error("Credenciais AWS não encontradas")
        raise
    except ClientError as err:
        logger.error(f"Erro ao conectar com AWS S3: {err}")
        raise
    except Exception as err:
        logger.error(f"Erro inesperado ao conectar com AWS S3: {err}")
        raise


def ensure_bucket_exists(s3_client, bucket_name: str) -> bool:
    """Verifica se o bucket existe e tenta criá-lo se não existir."""
    try:
        # Verificar se o bucket existe
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' já existe")
        return True
        
    except ClientError as err:
        error_code = err.response['Error']['Code']
        
        if error_code == '404':
            # Bucket não existe, tentar criar
            try:
                logger.info(f"Bucket '{bucket_name}' não existe. Tentando criar...")
                
                # Para regiões diferentes de us-east-1, precisamos especificar a configuração de localização
                if AWS_DEFAULT_REGION == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': AWS_DEFAULT_REGION}
                    )
                
                logger.info(f"Bucket '{bucket_name}' criado com sucesso")
                return True
                
            except ClientError as create_err:
                logger.error(f"Erro ao criar bucket '{bucket_name}': {create_err}")
                return False
                
        elif error_code == '403':
            logger.error(f"Sem permissão para acessar o bucket '{bucket_name}'")
            return False
        else:
            logger.error(f"Erro ao verificar bucket '{bucket_name}': {err}")
            return False
    
    except Exception as err:
        logger.error(f"Erro inesperado ao verificar bucket '{bucket_name}': {err}")
        return False


def create_date_folder(s3_client, bucket_name: str) -> str:
    """Cria uma pasta no bucket com estrutura raw/AAAAMMDD e retorna o nome da pasta completa."""
    try:
        # Gerar nome da pasta com data atual
        current_date = datetime.now()
        date_folder = current_date.strftime("%Y%m%d")
        
        # Estrutura: raw/AAAAMMDD
        full_folder_path = f"raw/{date_folder}"
        
        # Criar um objeto vazio para simular a pasta (S3 não tem pastas reais)
        # Usamos um arquivo .keep dentro da pasta para garantir que ela exista
        object_name = f"{full_folder_path}/.keep"
        
        # Verificar se a pasta já existe
        try:
            s3_client.head_object(Bucket=bucket_name, Key=object_name)
            logger.info(f"Pasta '{full_folder_path}' já existe no bucket '{bucket_name}'")
        except ClientError as err:
            error_code = err.response['Error']['Code']
            if error_code == '404':
                # Pasta não existe, criar
                logger.info(f"Criando estrutura de pasta '{full_folder_path}' no bucket '{bucket_name}'")
                s3_client.put_object(
                    Bucket=bucket_name, 
                    Key=object_name, 
                    Body=b""
                )
                logger.info(f"Estrutura de pasta '{full_folder_path}' criada com sucesso")
            else:
                raise err
        
        return full_folder_path
        
    except ClientError as err:
        logger.error(f"Erro ao criar estrutura de pasta no bucket '{bucket_name}': {err}")
        return ""


def clean_existing_parquet_files(s3_client, bucket_name: str, folder_path: str) -> int:
    """
    Remove todos os arquivos .parquet existentes na pasta especificada.
    
    Args:
        s3_client: Cliente boto3 S3
        bucket_name: Nome do bucket S3
        folder_path: Caminho da pasta no S3 (ex: raw/20250801)
        
    Returns:
        int: Número de arquivos removidos
    """
    deleted_count = 0
    
    try:
        # Listar objetos na pasta específica
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_path + "/",
            Delimiter="/"
        )
        
        if 'Contents' not in response:
            logger.info(f"Nenhum objeto encontrado na pasta '{folder_path}'")
            return 0
        
        # Filtrar apenas arquivos .parquet
        parquet_objects = [
            obj for obj in response['Contents'] 
            if obj['Key'].endswith('.parquet')
        ]
        
        if not parquet_objects:
            logger.info(f"Nenhum arquivo .parquet encontrado na pasta '{folder_path}'")
            return 0
        
        logger.info(f"Encontrados {len(parquet_objects)} arquivo(s) .parquet para remoção na pasta '{folder_path}'")
        print(f"🗑️  Removendo {len(parquet_objects)} arquivo(s) .parquet existente(s)...")
        
        # Remover cada arquivo parquet
        for obj in parquet_objects:
            try:
                file_name = obj['Key'].split('/')[-1]  # Obter apenas o nome do arquivo
                
                logger.info(f"Removendo arquivo existente: '{obj['Key']}'")
                print(f"   🗑️  Removendo: {file_name}")
                
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                deleted_count += 1
                
                logger.info(f"Arquivo removido com sucesso: '{file_name}'")
                print(f"   ✅ Removido: {file_name}")
                
            except ClientError as err:
                logger.error(f"Erro ao remover arquivo '{obj['Key']}': {err}")
                print(f"   ❌ Erro ao remover {file_name}: {err}")
            except Exception as err:
                logger.error(f"Erro inesperado ao remover arquivo '{obj['Key']}': {err}")
                print(f"   ❌ Erro inesperado ao remover {file_name}: {err}")
        
        logger.info(f"Limpeza concluída: {deleted_count}/{len(parquet_objects)} arquivos removidos")
        print(f"🧹 Limpeza concluída: {deleted_count} arquivo(s) removido(s)")
        
    except ClientError as err:
        logger.error(f"Erro ao listar objetos na pasta '{folder_path}': {err}")
        print(f"❌ Erro ao acessar pasta '{folder_path}': {err}")
    except Exception as err:
        logger.error(f"Erro inesperado durante limpeza da pasta '{folder_path}': {err}")
        print(f"❌ Erro inesperado durante limpeza: {err}")
    
    return deleted_count


def upload_parquet_files(s3_client, bucket_name: str, folder_path: str, local_data_path: str = "data") -> int:
    """
    Faz upload de arquivos .parquet da pasta local para a estrutura raw/AAAAMMDD no S3.
    
    IMPORTANTE: Apenas UM arquivo .parquet é permitido por pasta de data.
    Se já existirem arquivos .parquet na pasta, eles serão removidos antes do upload.
    Se houver múltiplos arquivos .parquet locais, apenas o mais recente será enviado.
    
    Args:
        s3_client: Cliente boto3 S3
        bucket_name: Nome do bucket S3
        folder_path: Caminho completo da pasta no S3 (ex: raw/20250801)
        local_data_path: Caminho local da pasta contendo os arquivos .parquet
        
    Returns:
        int: 1 se um arquivo foi enviado com sucesso, 0 caso contrário
    """
    try:
        # Verificar se a pasta local existe
        if not os.path.exists(local_data_path):
            logger.warning(f"Pasta local '{local_data_path}' não encontrada")
            print(f"📁 Pasta local '{local_data_path}' não encontrada")
            return 0
        
        # Buscar todos os arquivos .parquet na pasta local
        parquet_pattern = os.path.join(local_data_path, "*.parquet")
        parquet_files = glob.glob(parquet_pattern)
        
        if not parquet_files:
            logger.info(f"Nenhum arquivo .parquet encontrado em '{local_data_path}'")
            print(f"📁 Nenhum arquivo .parquet encontrado em '{local_data_path}'")
            return 0
        
        # Se houver múltiplos arquivos, selecionar apenas o mais recente
        if len(parquet_files) > 1:
            logger.info(f"Encontrados {len(parquet_files)} arquivos .parquet. Selecionando o mais recente...")
            print(f"📊 Encontrados {len(parquet_files)} arquivos .parquet. Selecionando o mais recente...")
            
            # Ordenar arquivos por data de modificação (mais recente primeiro)
            parquet_files.sort(key=os.path.getmtime, reverse=True)
            
            most_recent_file = parquet_files[0]
            logger.info(f"Arquivo mais recente selecionado: '{os.path.basename(most_recent_file)}'")
            print(f"🎯 Arquivo selecionado: {os.path.basename(most_recent_file)}")
            
            # Listar arquivos que serão ignorados
            ignored_files = parquet_files[1:]
            for ignored_file in ignored_files:
                logger.info(f"Arquivo ignorado (mais antigo): '{os.path.basename(ignored_file)}'")
                print(f"   ⏭️  Ignorado: {os.path.basename(ignored_file)}")
        else:
            most_recent_file = parquet_files[0]
            logger.info(f"Encontrado 1 arquivo .parquet: '{os.path.basename(most_recent_file)}'")
            print(f"📄 Encontrado 1 arquivo .parquet: {os.path.basename(most_recent_file)}")
        
        # Limpar arquivos .parquet existentes na pasta do S3
        print(f"\n🧹 Verificando e limpando arquivos existentes na pasta '{folder_path}'...")
        deleted_count = clean_existing_parquet_files(s3_client, bucket_name, folder_path)
        
        # Preparar upload do arquivo selecionado
        file_name = os.path.basename(most_recent_file)
        object_name = f"{folder_path}/{file_name}"
        file_size = os.path.getsize(most_recent_file)
        
        # Fazer o upload
        logger.info(f"Fazendo upload de '{file_name}' ({file_size} bytes) para '{object_name}'")
        print(f"\n⬆️  Uploading: {file_name} ({file_size / (1024*1024):.2f} MB)")
        print(f"    Destino: s3://{bucket_name}/{object_name}")
        
        s3_client.upload_file(most_recent_file, bucket_name, object_name)
        
        logger.info(f"Upload concluído com sucesso: '{file_name}'")
        print(f"✅ Upload concluído com sucesso: {file_name}")
        
        # Resumo final
        print(f"\n📊 Resumo da Operação:")
        print(f"   • Pasta de destino: {folder_path}")
        print(f"   • Arquivos removidos: {deleted_count}")
        print(f"   • Arquivo enviado: {file_name}")
        print(f"   • Status: ✅ Apenas 1 arquivo .parquet na pasta (política respeitada)")
        
        return 1
        
    except ClientError as err:
        logger.error(f"Erro ao fazer upload: {err}")
        print(f"❌ Erro ao fazer upload: {err}")
        return 0
    except Exception as err:
        logger.error(f"Erro inesperado durante upload: {err}")
        print(f"❌ Erro inesperado durante upload: {err}")
        return 0


def upload_single_file(s3_client, bucket_name: str, local_file_path: str, target_date: str = None) -> bool:
    """
    Faz upload de um único arquivo parquet para a estrutura raw/AAAAMMDD no S3.
    
    Remove qualquer arquivo .parquet existente na pasta de destino antes do upload.
    
    Args:
        s3_client: Cliente boto3 S3
        bucket_name: Nome do bucket S3
        local_file_path: Caminho local do arquivo
        target_date: Data no formato AAAAMMDD (se None, usa data atual)
        
    Returns:
        bool: True se o upload foi bem sucedido, False caso contrário
    """
    try:
        # Verificar se o arquivo local existe
        if not os.path.exists(local_file_path):
            logger.error(f"Arquivo local não encontrado: {local_file_path}")
            print(f"❌ Arquivo local não encontrado: {local_file_path}")
            return False
        
        # Verificar se é um arquivo .parquet
        if not local_file_path.endswith('.parquet'):
            logger.error(f"Arquivo deve ser .parquet: {local_file_path}")
            print(f"❌ Arquivo deve ser .parquet: {local_file_path}")
            return False
        
        # Definir a data de destino
        if target_date is None:
            target_date = datetime.now().strftime("%Y%m%d")
        
        # Validar formato da data
        try:
            datetime.strptime(target_date, "%Y%m%d")
        except ValueError:
            logger.error(f"Formato de data inválido: {target_date}. Use AAAAMMDD")
            print(f"❌ Formato de data inválido: {target_date}. Use AAAAMMDD")
            return False
        
        # Definir estrutura de pasta
        folder_path = f"raw/{target_date}"
        
        # Obter informações do arquivo
        file_size = os.path.getsize(local_file_path)
        file_name = os.path.basename(local_file_path)
        s3_key = f"{folder_path}/{file_name}"
        
        print(f"📤 Preparando upload para estrutura raw/data...")
        print(f"   📁 Pasta de destino: {folder_path}")
        print(f"   📄 Arquivo: {file_name} ({file_size / (1024*1024):.2f} MB)")
        
        # Limpar arquivos .parquet existentes na pasta
        print(f"\n🧹 Limpando arquivos existentes na pasta '{folder_path}'...")
        deleted_count = clean_existing_parquet_files(s3_client, bucket_name, folder_path)
        
        # Criar a estrutura de pasta se necessário
        keep_object = f"{folder_path}/.keep"
        try:
            s3_client.head_object(Bucket=bucket_name, Key=keep_object)
        except ClientError as err:
            if err.response['Error']['Code'] == '404':
                logger.info(f"Criando estrutura de pasta '{folder_path}'")
                s3_client.put_object(Bucket=bucket_name, Key=keep_object, Body=b"")
        
        # Fazer o upload
        logger.info(f"Fazendo upload de '{file_name}' para '{s3_key}'")
        print(f"\n⬆️  Fazendo upload...")
        print(f"    De: {local_file_path}")
        print(f"    Para: s3://{bucket_name}/{s3_key}")
        
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        
        logger.info(f"Upload concluído com sucesso: '{file_name}'")
        print(f"✅ Upload concluído com sucesso!")
        
        # Resumo
        print(f"\n📊 Resumo da Operação:")
        print(f"   • Pasta: {folder_path}")
        print(f"   • Arquivos removidos: {deleted_count}")
        print(f"   • Arquivo enviado: {file_name}")
        print(f"   • Status: ✅ Política respeitada (apenas 1 arquivo .parquet)")
        
        return True
        
    except ClientError as err:
        logger.error(f"Erro ao fazer upload do arquivo '{file_name}': {err}")
        print(f"❌ Erro ao fazer upload de {file_name}: {err}")
        return False
    except Exception as err:
        logger.error(f"Erro inesperado ao fazer upload do arquivo: {err}")
        print(f"❌ Erro inesperado ao fazer upload: {err}")
        return False


def main():
    """Função principal para upload de arquivos parquet para AWS S3 com estrutura raw/AAAAMMDD."""
    try:
        print("🚀 SISTEMA DE UPLOAD PARQUET PARA AWS S3")
        print("=" * 60)
        print("📁 Estrutura: s3://bucket/raw/AAAAMMDD/")
        print("📋 Política: Apenas 1 arquivo .parquet por pasta de data")
        print("🔄 Comportamento: Remove arquivos antigos e mantém apenas o mais recente")
        print("=" * 60)
        
        # Conectar ao S3
        s3_client = connect_s3()
        
        # Verificar se o bucket configurado existe, se não existir, criar
        if ensure_bucket_exists(s3_client, AWS_BUCKET_NAME):
            print(f"✅ Bucket '{AWS_BUCKET_NAME}' está disponível para uso")
        else:
            print(f"❌ Falha ao garantir que o bucket '{AWS_BUCKET_NAME}' existe")
            return
        
        # Criar estrutura de pasta raw/AAAAMMDD no bucket
        folder_path = create_date_folder(s3_client, AWS_BUCKET_NAME)
        if folder_path:
            print(f"✅ Estrutura de pasta '{folder_path}' criada/verificada no bucket '{AWS_BUCKET_NAME}'")
        else:
            print("❌ Falha ao criar estrutura de pasta com data atual")
            return
        
        # Fazer upload do arquivo .parquet mais recente
        print(f"\n📤 Iniciando processo de upload...")
        uploaded_files = upload_parquet_files(s3_client, AWS_BUCKET_NAME, folder_path)
        
        if uploaded_files > 0:
            print(f"\n🎉 Processo concluído com sucesso!")
            print(f"📄 {uploaded_files} arquivo .parquet enviado para s3://{AWS_BUCKET_NAME}/{folder_path}/")
        else:
            print("\n⚠️  Nenhum arquivo foi enviado.")
        
        # Listar buckets disponíveis
        logger.info("Listando buckets disponíveis...")
        response = s3_client.list_buckets()
        print(f"\n📦 Buckets disponíveis na conta AWS:")
        for bucket in response['Buckets']:
            logger.info(f"Bucket encontrado: {bucket['Name']}")
            print(f"   • {bucket['Name']}")
        
        # Listar objetos na pasta raw para mostrar a estrutura
        print(f"\n📁 Estrutura da pasta 'raw' no bucket '{AWS_BUCKET_NAME}':")
        try:
            response = s3_client.list_objects_v2(
                Bucket=AWS_BUCKET_NAME,
                Prefix="raw/",
                Delimiter="/"
            )
            
            # Mostrar subpastas (pastas de data)
            if 'CommonPrefixes' in response:
                print("   📅 Pastas de data encontradas:")
                for prefix in response['CommonPrefixes']:
                    folder_name = prefix['Prefix'].rstrip('/')
                    print(f"      • {folder_name}")
                    
                    # Listar arquivos em cada pasta de data
                    files_response = s3_client.list_objects_v2(
                        Bucket=AWS_BUCKET_NAME,
                        Prefix=prefix['Prefix']
                    )
                    
                    if 'Contents' in files_response:
                        parquet_files = [
                            obj for obj in files_response['Contents'] 
                            if obj['Key'].endswith('.parquet')
                        ]
                        
                        if parquet_files:
                            for obj in parquet_files:
                                file_name = obj['Key'].split('/')[-1]
                                size_mb = obj['Size'] / (1024*1024)
                                modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                                print(f"         📄 {file_name} ({size_mb:.2f} MB) - {modified}")
                        else:
                            print(f"         (nenhum arquivo .parquet)")
            else:
                print("   (nenhuma pasta de data encontrada)")
                
            # Mostrar arquivos soltos na pasta raw (se houver)
            if 'Contents' in response:
                root_files = [
                    obj for obj in response['Contents'] 
                    if '/' not in obj['Key'][4:] and obj['Key'].endswith('.parquet')  # Remove 'raw/' prefix
                ]
                
                if root_files:
                    print("   📄 Arquivos na raiz da pasta 'raw':")
                    for obj in root_files:
                        file_name = obj['Key'].split('/')[-1]
                        size_mb = obj['Size'] / (1024*1024)
                        print(f"      • {file_name} ({size_mb:.2f} MB)")
                        
        except ClientError as err:
            logger.error(f"Erro ao listar estrutura da pasta 'raw': {err}")
            print(f"❌ Erro ao listar estrutura da pasta 'raw': {err}")
            
    except (NoCredentialsError, ClientError) as err:
        logger.error(f"Erro ao conectar no AWS S3: {err}")
        print(f"❌ Erro ao conectar no AWS S3: {err}")
        print("\n💡 Verifique:")
        print("   • Credenciais AWS (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
        print("   • Região AWS (AWS_DEFAULT_REGION)")
        print("   • Permissões do usuário/role para acessar S3")
    except Exception as err:
        logger.error(f"Erro inesperado: {err}")
        print(f"❌ Erro inesperado: {err}")


if __name__ == "__main__":
    main()
