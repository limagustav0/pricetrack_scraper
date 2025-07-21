# Imagem base
FROM --platform=linux/amd64 python:3.12.2-slim

# Evita prompt do Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Diretório de trabalho dentro do container
WORKDIR /app

# Copia e instala dependências do sistema necessárias ao Playwright
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libxss1 \
    libasound2 \
    libxtst6 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements e instala pacotes Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instala Playwright e seus navegadores
RUN pip install playwright && playwright install --with-deps

# Copia o restante da aplicação
COPY . .

# Comando padrão (ajuste conforme necessário)
CMD ["python", "main.py"]
