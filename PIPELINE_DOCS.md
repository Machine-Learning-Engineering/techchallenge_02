# Pipeline de Dados IBOV

Este conjunto de scripts automatiza o processo de coleta, processamento e armazenamento de dados do IBOV.

## Scripts Criados

### 1. `main_pipeline.py`
Script principal que executa todo o pipeline em sequência:
- 01_ibov_scraper.py (coleta de dados)
- 02_csv_to_parquet.py (conversão de formato)
- 03_minio_client.py (upload para MinIO)
- 04_cleanup_data.py (limpeza de arquivos locais)

### 2. `scheduler.py`
Agendador que executa o pipeline automaticamente de segunda a sexta-feira às 20:00.

### 3. `run_pipeline.py`
Script para teste manual do pipeline completo.

## Como Usar

### Execução Manual (Teste)
```bash
cd /home/parraes/techchallenge_02
python src/run_pipeline.py
```

### Execução do Pipeline Principal
```bash
cd /home/parraes/techchallenge_02
python src/main_pipeline.py
```

### Iniciar o Agendador (Execução Automática)
```bash
cd /home/parraes/techchallenge_02
python src/scheduler.py
```

O agendador ficará rodando em background e executará o pipeline:
- **Horário**: 20:00 (8:00 PM)
- **Dias**: Segunda a Sexta-feira
- **Logs**: Salvos em `scheduler.log`

Para parar o agendador, pressione `Ctrl+C`.

## Logs

Cada script gera seus próprios logs:
- `pipeline.log` - Log do pipeline principal
- `scheduler.log` - Log do agendador
- `ibov_scraper.log` - Log do scraper
- `minio_client.log` - Log do cliente MinIO
- `cleanup.log` - Log da limpeza

## Dependências

Certifique-se de que as seguintes bibliotecas estão instaladas:
```bash
pip install schedule minio pandas pyarrow beautifulsoup4 requests
```

## Fluxo de Execução

1. **Scraping**: Coleta dados do IBOV e salva em CSV
2. **Conversão**: Converte CSV para formato Parquet
3. **Upload**: Envia arquivos Parquet para o MinIO
4. **Limpeza**: Remove arquivos locais após upload

## Configuração do MinIO

Certifique-se de que o MinIO está rodando e configurado:
- URL: localhost:9000
- Usuário: admin
- Senha: admin123
- Bucket: ibov

## Troubleshooting

- Verifique se todos os scripts individuais estão funcionando
- Confirme se o MinIO está acessível
- Verifique os logs para identificar erros específicos
- Certifique-se de que todas as dependências estão instaladas
