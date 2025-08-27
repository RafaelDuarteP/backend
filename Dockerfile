# Dockerfile.mysql
FROM mysql:8.0

# Copia script de inicialização
COPY init.sql /docker-entrypoint-initdb.d/
