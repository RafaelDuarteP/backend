1) Estrutura de pastas:

```plaintext
  .
  ├─ docker-compose.yml
  ├─ Dockerfile
  └─ app/
     └─ main.py
```

2) Subir stack:

  docker compose up --build

1) Testes rápidos:

# Criar pessoa

```bash
  curl -X POST http://localhost:8000/pessoas -H 'Content-Type: application/json' -d '{"nome":"João Silva","cpf":"123.456.789-00","data_nascimento":"1990-05-20"}'
```

# Editar com versão conhecida (ex: 1)

```bash
 curl -X PATCH http://localhost:8000/pessoas/1 -H 'Content-Type: application/json' -d '{"version":1, "nome":"João A. Silva"}'
```

# Listar

  curl http://localhost:8000/pessoas

Observações arquiteturais:

- Versionamento + event log em `pessoa_event` permitem reconstrução e replay.
- PATCH com version atrasada tenta merge via replay (LWW por campo ao reaplicar eventos).
- DELETE é soft delete e requer versão atual para simplicidade.
