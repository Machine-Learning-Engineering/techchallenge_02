
FROM registry.redhat.io/ubi10/python-312-minimal

LABEL maintainer="PRODESP"
LABEL version="1.0"
LABEL description="API Vetorizar Dados no Elastic Search"
LABEL name="Vetorizar Dados"

# Definir variáveis de ambiente
ENV HOME=/opt/app-root \
    APP_ROOT=/opt/app-root/src \
    PATH=/opt/app-root/src/bin:/opt/app-root/bin:$PATH

# Criar diretórios necessários e ajustar permissões
USER 0

RUN mkdir -p ${APP_ROOT} && \
    chmod -R u+x ${APP_ROOT} && \
    chgrp -R 0 ${APP_ROOT} && \
    chmod -R g=u ${APP_ROOT}

WORKDIR ${APP_ROOT}

# Copiar arquivos e instalar dependências
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# Baixar o modelo como usuário não-root
USER 1001


# Copiar o resto dos arquivos da aplicação
COPY src/ ./src/
COPY config/ ./config/


# Executar o comando padrão
CMD ["python", "src/main.py"]