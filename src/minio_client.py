from minio import Minio
from minio.error import S3Error
import os
import logging
import sys
from datetime import datetime
import glob


# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('minio_client.log')
    ]
)
logger = logging.getLogger(__name__)

endpoint = os.getenv("MINIO_URL", "localhost:9000")
access_key = os.getenv("MINIO_USER", "admin")
secret_key = os.getenv("MINIO_PASS", "admin123")
bucket_name = os.getenv("MINIO_BUCKET", "ibov")

def connect_minio(endpoint: str, access_key: str, secret_key: str) -> Minio:
    """Conecta ao servidor MinIO/S3 parametrizando URL, usuário e senha."""
    logger.info(f"Conectando ao MinIO/S3 em {endpoint} com usuário '{access_key}'")
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=endpoint.startswith("https")
    )
    logger.info("Cliente MinIO/S3 criado com sucesso")
    return client


def ensure_bucket_exists(client: Minio, bucket_name: str) -> bool:
    """Verifica se o bucket existe e o cria caso não exista."""
    try:
        if client.bucket_exists(bucket_name):
            logger.info(f"Bucket '{bucket_name}' já existe")
            return True
        else:
            logger.info(f"Bucket '{bucket_name}' não existe. Criando...")
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' criado com sucesso")
            return True
    except S3Error as err:
        logger.error(f"Erro ao verificar/criar bucket '{bucket_name}': {err}")
        return False


def create_date_folder(client: Minio, bucket_name: str) -> str:
    """Cria uma pasta no bucket com a data atual (AAAAMMDD) e retorna o nome da pasta."""
    try:
        # Gerar nome da pasta com data atual
        current_date = datetime.now()
        folder_name = current_date.strftime("%Y%m%d")
        
        # Criar um objeto vazio para simular a pasta (MinIO/S3 não tem pastas reais)
        # Usamos um arquivo .keep dentro da pasta para garantir que ela exista
        object_name = f"{folder_name}/.keep"
        
        # Verificar se a pasta já existe
        try:
            client.stat_object(bucket_name, object_name)
            logger.info(f"Pasta '{folder_name}' já existe no bucket '{bucket_name}'")
        except S3Error as err:
            if err.code == 'NoSuchKey':
                # Pasta não existe, criar
                logger.info(f"Criando pasta '{folder_name}' no bucket '{bucket_name}'")
                client.put_object(
                    bucket_name, 
                    object_name, 
                    data=b"", 
                    length=0
                )
                logger.info(f"Pasta '{folder_name}' criada com sucesso")
            else:
                raise err
        
        return folder_name
        
    except S3Error as err:
        logger.error(f"Erro ao criar pasta com data no bucket '{bucket_name}': {err}")
        return ""


def upload_parquet_files(client: Minio, bucket_name: str, folder_name: str, local_data_path: str = "data") -> int:
    """Faz upload de todos os arquivos .parquet da pasta local para a pasta do bucket."""
    uploaded_count = 0
    
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
                
                logger.info(f"Fazendo upload de '{file_name}' ({file_size} bytes) para '{object_name}'")
                print(f"Uploading: {file_name} ({file_size / (1024*1024):.2f} MB)")
                
                # Fazer o upload
                client.fput_object(bucket_name, object_name, local_file_path)
                
                uploaded_count += 1
                logger.info(f"Upload concluído: '{file_name}'")
                print(f"✓ Upload concluído: {file_name}")
                
            except S3Error as err:
                logger.error(f"Erro ao fazer upload do arquivo '{file_name}': {err}")
                print(f"✗ Erro ao fazer upload de {file_name}: {err}")
            except Exception as err:
                logger.error(f"Erro inesperado ao fazer upload do arquivo '{file_name}': {err}")
                print(f"✗ Erro inesperado ao fazer upload de {file_name}: {err}")
        
        logger.info(f"Upload concluído: {uploaded_count}/{len(parquet_files)} arquivos enviados")
        print(f"\nResumo: {uploaded_count}/{len(parquet_files)} arquivos enviados com sucesso")
        
    except Exception as err:
        logger.error(f"Erro ao processar uploads: {err}")
        print(f"Erro ao processar uploads: {err}")
    
    return uploaded_count


def main():
    # Parâmetros de conexão (pode alterar para testar)

    try:
        client = connect_minio(endpoint, access_key, secret_key)
        
        # Verificar se o bucket configurado existe, se não existir, criar
        if ensure_bucket_exists(client, bucket_name):
            print(f"Bucket '{bucket_name}' está disponível para uso")
        else:
            print(f"Falha ao garantir que o bucket '{bucket_name}' existe")
            return
        
        # Criar pasta com data atual no bucket
        folder_name = create_date_folder(client, bucket_name)
        if folder_name:
            print(f"Pasta '{folder_name}' criada/verificada no bucket '{bucket_name}'")
        else:
            print("Falha ao criar pasta com data atual")
            return
        
        # Fazer upload dos arquivos .parquet da pasta data
        print(f"\nIniciando upload de arquivos .parquet...")
        uploaded_files = upload_parquet_files(client, bucket_name, folder_name)
        
        if uploaded_files > 0:
            print(f"\n{uploaded_files} arquivo(s) .parquet enviado(s) com sucesso!")
        else:
            print("\nNenhum arquivo foi enviado.")
        
        logger.info("Listando buckets disponíveis...")
        buckets = client.list_buckets()
        print("\nBuckets disponíveis:")
        for bucket in buckets:
            logger.info(f"Bucket encontrado: {bucket.name}")
            print(f"- {bucket.name}")
        
        # Listar objetos no bucket para mostrar a pasta criada
        print(f"\nObjetos no bucket '{bucket_name}':")
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            print(f"- {obj.object_name}")
    except S3Error as err:
        logger.error(f"Erro ao conectar no MinIO/S3: {err}")
        print(f"Erro ao conectar no MinIO/S3: {err}")


if __name__ == "__main__":
    main()
