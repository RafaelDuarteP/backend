# ------------------------------------------------------------
# Dockerfile
# ------------------------------------------------------------
# syntax=docker/dockerfile:1
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn[standard] SQLAlchemy pymysql pydantic
COPY app /app
EXPOSE 80
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]