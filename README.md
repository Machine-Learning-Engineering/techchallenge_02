# techchallenge_02

## Usando Docker Compose

Para iniciar o MinIO usando docker-compose:

```bash
docker-compose up -d
```

Ou usando podman-compose:

```bash
podman-compose up -d
```

O MinIO estará disponível em:
- API: http://localhost:9000
- Console: http://localhost:9001

Credenciais:
- User: admin
- Password: admin123

## Comando Podman (alternativa ao docker-compose)

```bash
podman run \
   --replace --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=admin123 \
  -v minio-data:/data \
  quay.io/minio/minio server /data --console-address ":9001"

podman run -it --rm -v $(pwd)/data:/opt/app-root/src/data techchallenge-app bash

podman build -t techchallenge-app .
```