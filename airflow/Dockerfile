FROM apache/airflow:2.7.1-python3.9

USER root

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        pkg-config \
        curl \
        wget \
        vim \
        git \
        nano \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Crear directorios necesarios
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/plugins

# Establecer permisos usando UID/GID directamente
RUN chown -R 50000:0 /opt/airflow/ && \
    chmod -R 775 /opt/airflow/

# Crear un archivo de configuración básico como root
RUN airflow version && \
    airflow config list > /opt/airflow/airflow_generated.cfg 2>/dev/null || true

# Establecer permisos en el archivo de configuración generado
RUN chown 50000:0 /opt/airflow/airflow_generated.cfg && \
    chmod 664 /opt/airflow/airflow_generated.cfg

USER 50000

# Copiar requirements e instalar dependencias
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Health check custom
HEALTHCHECK --interval=30s --timeout=10s --start-period=5m --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080