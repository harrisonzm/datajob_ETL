# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.12.7
FROM python:${PYTHON_VERSION}-slim as base

# Instalar PowerShell y dependencias del sistema
RUN apt-get update && apt-get install -y \
    wget \
    apt-transport-https \
    software-properties-common \
    curl \
    && wget -q https://packages.microsoft.com/config/debian/11/packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && apt-get update \
    && apt-get install -y powershell \
    && rm -rf /var/lib/apt/lists/*

# Configurar Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar Poetry
RUN pip install poetry

# Configurar Poetry
ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_NO_INTERACTION=1
ENV POETRY_CACHE_DIR=/tmp/poetry_cache

# Copiar archivos de dependencias
COPY pyproject.toml poetry.lock* ./

# Instalar dependencias
RUN poetry install --no-dev && rm -rf $POETRY_CACHE_DIR

# Copiar código fuente
COPY . .

# Crear directorio de logs
RUN mkdir -p logs

# Variables de entorno para la base de datos
ENV DB_HOST=db
ENV DB_PORT=5432
ENV DB_NAME=job_posting
ENV DB_USER=postgres
ENV DB_PASSWORD=postgres

# Exponer puerto (si es necesario)
EXPOSE 8000

# Comando por defecto: ejecutar el pipeline completo
CMD ["pwsh", "-File", "evaluate.ps1"]
