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
    """Cria uma pasta no bucket com a data atual (AAAAMMDD) e retorna o nome da pasta."""
    try:
        # Gerar nome da pasta com data atual
        current_date = datetime.now()
        folder_name = current_date.strftime("%Y%m%d")
        
        # Criar um objeto vazio para simular a pasta (S3 não tem pastas reais)
        # Usamos um arquivo .keep dentro da pasta para garantir que ela exista
        object_name = f"{folder_name}/.keep"
        
        # Verificar se a pasta já existe
        try:
            s3_client.head_object(Bucket=bucket_name, Key=object_name)
            logger.info(f"Pasta '{folder_name}' já existe no bucket '{bucket_name}'")
        except ClientError as err:
            error_code = err.response['Error']['Code']
            if error_code == '404':
                # Pasta não existe, criar
                logger.info(f"Criando pasta '{folder_name}' no bucket '{bucket_name}'")
                s3_client.put_object(
                    Bucket=bucket_name, 
                    Key=object_name, 
                    Body=b""
                )
                logger.info(f"Pasta '{folder_name}' criada com sucesso")
            else:
                raise err
        
        return folder_name
        
    except ClientError as err:
        logger.error(f"Erro ao criar pasta com data no bucket '{bucket_name}': {err}")
        return ""


def upload_parquet_files(s3_client, bucket_name: str, folder_name: str, local_data_path: str = "data") -> int:
    """
    Faz upload de todos os arquivos .parquet da pasta local para a pasta do bucket.
    
    Se um arquivo já existir no S3 com o mesmo nome, ele será substituído.
    
    Args:
        s3_client: Cliente boto3 S3
        bucket_name: Nome do bucket S3
        folder_name: Nome da pasta dentro do bucket
        local_data_path: Caminho local da pasta contendo os arquivos .parquet
        
    Returns:
        int: Número de arquivos enviados com sucesso
    """
    uploaded_count = 0
    replaced_count = 0
    new_files_count = 0
    
    try:
        # Verificar se a pasta local existe
        if not os.path.exists(local_data_path):
            logger.warning(f"Pasta local '{local_data_path}' não encontrada")
            print(f"Pasta local '{local_data_path}' não encontrada")
            return 0
        
        # Buscar todos os arquivos .parquet na pasta local
        parquet_pattern = os.path.join(local_data_path, "*.parquet")
        parquet_files = glob.glob(parquet_pattern)
        
        if not parquet_files:
            logger.info(f"Nenhum arquivo .parquet encontrado em '{local_data_path}'")
            print(f"Nenhum arquivo .parquet encontrado em '{local_data_path}'")
            return 0
        
        logger.info(f"Encontrados {len(parquet_files)} arquivo(s) .parquet para upload")
        print(f"Encontrados {len(parquet_files)} arquivo(s) .parquet para upload")
        
        # Fazer upload de cada arquivo
        for local_file_path in parquet_files:
            try:
                # Obter apenas o nome do arquivo
                file_name = os.path.basename(local_file_path)
                
                # Definir o caminho no bucket (dentro da pasta da data)
                object_name = f"{folder_name}/{file_name}"
                
                # Verificar o tamanho do arquivo
                file_size = os.path.getsize(local_file_path)
                
                # Verificar se o arquivo já existe no S3
                file_exists = False
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=object_name)
                    file_exists = True
                    logger.info(f"Arquivo '{file_name}' já existe no S3. Será substituído.")
                    print(f"⚠️  Arquivo '{file_name}' já existe. Substituindo...")
                except ClientError as err:
                    if err.response['Error']['Code'] == '404':
                        # Arquivo não existe, continuar normalmente
                        pass
                    else:
                        # Outro erro, re-raise
                        raise err
                
                logger.info(f"Fazendo upload de '{file_name}' ({file_size} bytes) para '{object_name}'")
                print(f"Uploading: {file_name} ({file_size / (1024*1024):.2f} MB)")
                
                # Fazer o upload usando boto3 (sempre sobrescreve se existir)
                s3_client.upload_file(local_file_path, bucket_name, object_name)
                
                uploaded_count += 1
                if file_exists:
                    replaced_count += 1
                    logger.info(f"Arquivo substituído com sucesso: '{file_name}'")
                    print(f"✓ Arquivo substituído com sucesso: {file_name}")
                else:
                    new_files_count += 1
                    logger.info(f"Upload concluído: '{file_name}'")
                    print(f"✓ Upload concluído: {file_name}")
                
            except ClientError as err:
                logger.error(f"Erro ao fazer upload do arquivo '{file_name}': {err}")
                print(f"✗ Erro ao fazer upload de {file_name}: {err}")
            except Exception as err:
                logger.error(f"Erro inesperado ao fazer upload do arquivo '{file_name}': {err}")
                print(f"✗ Erro inesperado ao fazer upload de {file_name}: {err}")
        
        logger.info(f"Upload concluído: {uploaded_count}/{len(parquet_files)} arquivos enviados")
        print(f"\n📊 Resumo do Upload:")
        print(f"   • Total de arquivos processados: {len(parquet_files)}")
        print(f"   • Arquivos enviados com sucesso: {uploaded_count}")
        print(f"   • Arquivos novos: {new_files_count}")
        print(f"   • Arquivos substituídos: {replaced_count}")
        
        if uploaded_count == len(parquet_files):
            print(f"✅ Todos os arquivos foram enviados com sucesso!")
        else:
            failed_count = len(parquet_files) - uploaded_count
            print(f"⚠️  {failed_count} arquivo(s) falharam no upload")
        
    except Exception as err:
        logger.error(f"Erro ao processar uploads: {err}")
        print(f"Erro ao processar uploads: {err}")
    
    return uploaded_count


def upload_single_file(s3_client, bucket_name: str, local_file_path: str, s3_key: str) -> bool:
    """
    Faz upload de um único arquivo para o S3.
    
    Se o arquivo já existir no S3, ele será substituído.
    
    Args:
        s3_client: Cliente boto3 S3
        bucket_name: Nome do bucket S3
        local_file_path: Caminho local do arquivo
        s3_key: Chave (caminho) do arquivo no S3
        
    Returns:
        bool: True se o upload foi bem sucedido, False caso contrário
    """
    try:
        # Verificar se o arquivo local existe
        if not os.path.exists(local_file_path):
            logger.error(f"Arquivo local não encontrado: {local_file_path}")
            return False
        
        # Obter informações do arquivo
        file_size = os.path.getsize(local_file_path)
        file_name = os.path.basename(local_file_path)
        
        # Verificar se o arquivo já existe no S3
        file_exists = False
        try:
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            file_exists = True
            logger.info(f"Arquivo '{file_name}' já existe no S3. Será substituído.")
            print(f"⚠️  Arquivo '{file_name}' já existe no S3. Substituindo...")
        except ClientError as err:
            if err.response['Error']['Code'] == '404':
                # Arquivo não existe, continuar normalmente
                logger.info(f"Arquivo '{file_name}' não existe no S3. Fazendo novo upload.")
                print(f"📁 Novo arquivo: {file_name}")
            else:
                # Outro erro, re-raise
                raise err
        
        # Fazer o upload
        logger.info(f"Fazendo upload de '{file_name}' ({file_size} bytes) para '{s3_key}'")
        print(f"⬆️  Uploading: {file_name} ({file_size / (1024*1024):.2f} MB)")
        
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        
        if file_exists:
            logger.info(f"Arquivo substituído com sucesso: '{file_name}'")
            print(f"✅ Arquivo substituído com sucesso: {file_name}")
        else:
            logger.info(f"Upload concluído: '{file_name}'")
            print(f"✅ Upload concluído: {file_name}")
        
        return True
        
    except ClientError as err:
        logger.error(f"Erro ao fazer upload do arquivo '{file_name}': {err}")
        print(f"❌ Erro ao fazer upload de {file_name}: {err}")
        return False
    except Exception as err:
        logger.error(f"Erro inesperado ao fazer upload do arquivo '{file_name}': {err}")
        print(f"❌ Erro inesperado ao fazer upload de {file_name}: {err}")
        return False


def main():
    """Função principal para upload de arquivos parquet para AWS S3."""
    try:
        # Conectar ao S3
        s3_client = connect_s3()
        
        # Verificar se o bucket configurado existe, se não existir, criar
        if ensure_bucket_exists(s3_client, AWS_BUCKET_NAME):
            print(f"Bucket '{AWS_BUCKET_NAME}' está disponível para uso")
        else:
            print(f"Falha ao garantir que o bucket '{AWS_BUCKET_NAME}' existe")
            return
        
        # Criar pasta com data atual no bucket
        folder_name = create_date_folder(s3_client, AWS_BUCKET_NAME)
        if folder_name:
            print(f"Pasta '{folder_name}' criada/verificada no bucket '{AWS_BUCKET_NAME}'")
        else:
            print("Falha ao criar pasta com data atual")
            return
        
        # Fazer upload dos arquivos .parquet da pasta data
        print(f"\nIniciando upload de arquivos .parquet...")
        uploaded_files = upload_parquet_files(s3_client, AWS_BUCKET_NAME, folder_name)
        
        if uploaded_files > 0:
            print(f"\n{uploaded_files} arquivo(s) .parquet enviado(s) com sucesso!")
        else:
            print("\nNenhum arquivo foi enviado.")
        
        # Listar buckets disponíveis
        logger.info("Listando buckets disponíveis...")
        response = s3_client.list_buckets()
        print("\nBuckets disponíveis:")
        for bucket in response['Buckets']:
            logger.info(f"Bucket encontrado: {bucket['Name']}")
            print(f"- {bucket['Name']}")
        
        # Listar objetos no bucket para mostrar a pasta criada
        print(f"\nObjetos no bucket '{AWS_BUCKET_NAME}':")
        try:
            response = s3_client.list_objects_v2(Bucket=AWS_BUCKET_NAME)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f"- {obj['Key']}")
            else:
                print("Bucket está vazio")
        except ClientError as err:
            logger.error(f"Erro ao listar objetos do bucket: {err}")
            print(f"Erro ao listar objetos do bucket: {err}")
            
    except (NoCredentialsError, ClientError) as err:
        logger.error(f"Erro ao conectar no AWS S3: {err}")
        print(f"Erro ao conectar no AWS S3: {err}")
    except Exception as err:
        logger.error(f"Erro inesperado: {err}")
        print(f"Erro inesperado: {err}")


if __name__ == "__main__":
    main()
