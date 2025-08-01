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
docker run \
   -d --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=1qaz@WSX \
  -e TZ=America/Sao_Paulo \
  -v minio-data:/data \
  quay.io/minio/minio server /data --console-address ":9001"

podman run -it --rm -v $(pwd)/data:/opt/app-root/src/data techchallenge-app bash

podman build -t techchallenge-app .
```

quay.io/parraes/techchallenge_02:v1 


sudo docker run --name scraper-b3 \
  -d \
  -e AWS_ACCESS_KEY_ID=AKIA6FEKPWUGBSSH3F6G \
  -e AWS_SECRET_ACCESS_KEY=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 \
  -e TZ=America/Sao_Paulo \
  quay.io/parraes/techchallenge_02:v1


http://ec2-54-80-29-12.compute-1.amazonaws.com:9001/login <-- Endereço s3