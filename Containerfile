FROM python:3.11-slim-bookworm

# Definir variáveis de ambiente
ENV HOME=/opt/app-root \
    APP_ROOT=/opt/app-root/ \
    PATH=/opt/app-root/bin:/opt/app-root/bin:/usr/local/bin:$PATH \
    CHROMEDRIVER_PATH=/usr/local/bin/chromedriver \
    CHROME_BIN=/usr/bin/chromium \
    DISPLAY=:99

# Atualizar repositórios e instalar dependências
RUN apt-get update && \
    apt-get install -y \
    chromium \
    chromium-driver \
    wget \
    gnupg \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p ${APP_ROOT} \
    && chmod -R u+x ${APP_ROOT} \
    && chgrp -R 0 ${APP_ROOT} \
    && chmod -R g=u ${APP_ROOT}

WORKDIR ${APP_ROOT}

# Criar usuário não-root
RUN useradd -u 1001 -g 0 -s /bin/bash -m appuser \
    && chown -R 1001:0 /opt/app-root

# Copiar requirements e instalar dependências Python
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Mudar para usuário não-root
USER 1001

# Copiar código da aplicação
COPY src/ ./src/

# Criar diretório de dados se não existir
RUN mkdir -p data

# Executar aplicação
CMD ["python", "src/scheduler.py"]
