# ------------------------------------------------------------
# app/main.py
# ------------------------------------------------------------
# FastAPI + SQLAlchemy (MySQL) com versionamento e event log
from datetime import datetime, date
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Path
from fastapi import Body, Query
from pydantic import BaseModel, Field
from sqlalchemy import (
    create_engine, Column, Integer, BigInteger, String, Date, DateTime,
    JSON, ForeignKey, func, Boolean
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import os

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

# ------------------ Models ------------------
class Pessoa(Base):
    __tablename__ = "pessoa"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    nome = Column(String(120), nullable=False)
    cpf = Column(String(14), nullable=False, unique=True)
    data_nascimento = Column(Date, nullable=False)
    version = Column(Integer, nullable=False, default=1)
    deleted = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    created_at = Column(DateTime, nullable=False, default=func.now())

    events = relationship("PessoaEvent", back_populates="pessoa", order_by="PessoaEvent.new_version")

class PessoaEvent(Base):
    __tablename__ = "pessoa_event"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    pessoa_id = Column(BigInteger, ForeignKey("pessoa.id"), nullable=False)
    base_version = Column(Integer, nullable=False)
    new_version = Column(Integer, nullable=False)
    # 'changes' é o delta aplicado nessa transição
    changes = Column(JSON, nullable=False)
    # 'state_after' é o snapshot completo após aplicar o evento
    state_after = Column(JSON, nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())

    pessoa = relationship("Pessoa", back_populates="events")

# ------------------ Schemas ------------------
class PessoaOut(BaseModel):
    id: int
    nome: str
    cpf: str
    data_nascimento: date
    version: int
    updated_at: datetime

    class Config:
        orm_mode = True

class PessoaCreate(BaseModel):
    nome: str = Field(..., max_length=120)
    cpf: str = Field(..., max_length=14)
    data_nascimento: date

class PessoaPatch(BaseModel):
    version: int = Field(..., description="Versão conhecida pelo cliente")
    nome: Optional[str] = Field(None, max_length=120)
    cpf: Optional[str] = Field(None, max_length=14)
    data_nascimento: Optional[date] = None

# ------------------ Helpers ------------------

def pessoa_to_dict(p: Pessoa) -> Dict[str, Any]:
    return {
        "id": p.id,
        "nome": p.nome,
        "cpf": p.cpf,
        "data_nascimento": p.data_nascimento.isoformat(),
        "version": p.version,
        "deleted": p.deleted,
        "updated_at": p.updated_at.isoformat() if p.updated_at else None,
    }

# aplica um delta (changes) em um estado base (dict) retornando novo estado
# regra: LWW por campo (valor do delta vence para os campos presentes)

def apply_changes(base_state: Dict[str, Any], changes: Dict[str, Any]) -> Dict[str, Any]:
    new_state = dict(base_state)
    for k, v in changes.items():
        new_state[k] = v
    return new_state

# reconstrói o snapshot de uma pessoa em uma versão específica usando o event log

def snapshot_at_version(sess, pessoa_id: int, target_version: int) -> Dict[str, Any]:
    ev = (
        sess.query(PessoaEvent)
        .filter(PessoaEvent.pessoa_id == pessoa_id, PessoaEvent.new_version == target_version)
        .one_or_none()
    )
    if ev is None:
        raise HTTPException(status_code=409, detail="Versão solicitada inexistente para reconstrução")
    return dict(ev.state_after)

# reaplica eventos de (from_version+1 .. to_version) sobre um estado inicial

def replay_forward(sess, pessoa_id: int, from_version: int, to_version: int, state: Dict[str, Any]) -> Dict[str, Any]:
    if to_version <= from_version:
        return state
    evs = (
        sess.query(PessoaEvent)
        .filter(
            PessoaEvent.pessoa_id == pessoa_id,
            PessoaEvent.new_version > from_version,
            PessoaEvent.new_version <= to_version,
        )
        .order_by(PessoaEvent.new_version.asc())
        .all()
    )
    s = dict(state)
    for ev in evs:
        s = apply_changes(s, ev.changes)
    return s

# cria e persiste um evento com snapshot pós-aplicação

def persist_event(sess, pessoa: Pessoa, base_version: int, changes: Dict[str, Any]):
    # construir snapshot após aplicar changes ao estado atual do ORM (já atualizado)
    state_after = {
        "id": pessoa.id,
        "nome": pessoa.nome,
        "cpf": pessoa.cpf,
        "data_nascimento": pessoa.data_nascimento.isoformat(),
        "version": pessoa.version,
        "deleted": pessoa.deleted,
        "updated_at": pessoa.updated_at.isoformat() if pessoa.updated_at else None,
    }
    ev = PessoaEvent(
        pessoa_id=pessoa.id,
        base_version=base_version,
        new_version=pessoa.version,
        changes=changes,
        state_after=state_after,
    )
    sess.add(ev)

# ------------------ App ------------------
app = FastAPI(title="Pessoas API", version="1.0.0")

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(engine)

# 1) Listar pessoas (com filtros opcionais)
@app.get("/pessoas", response_model=List[PessoaOut])
def list_pessoas(
    modified_since: Optional[datetime] = Query(None),
    include_deleted: bool = Query(False),
):
    sess = SessionLocal()
    try:
        q = sess.query(Pessoa)
        if not include_deleted:
            q = q.filter(Pessoa.deleted == False)  # noqa: E712
        if modified_since:
            q = q.filter(Pessoa.updated_at >= modified_since)
        rows = q.order_by(Pessoa.id.asc()).all()
        return rows
    finally:
        sess.close()

# 2) Obter pessoa
@app.get("/pessoas/{pessoa_id}", response_model=PessoaOut)
def get_pessoa(pessoa_id: int = Path(...)):
    sess = SessionLocal()
    try:
        p = sess.query(Pessoa).filter(Pessoa.id == pessoa_id, Pessoa.deleted == False).one_or_none()  # noqa: E712
        if not p:
            raise HTTPException(status_code=404, detail="Pessoa não encontrada")
        return p
    finally:
        sess.close()

# 3) Criar pessoa
@app.post("/pessoas", response_model=PessoaOut, status_code=201)
def create_pessoa(payload: PessoaCreate):
    sess = SessionLocal()
    try:
        # valida unicidade de CPF
        exists = sess.query(Pessoa).filter(Pessoa.cpf == payload.cpf, Pessoa.deleted == False).one_or_none()  # noqa: E712
        if exists:
            raise HTTPException(status_code=409, detail="CPF já cadastrado")
        p = Pessoa(
            nome=payload.nome,
            cpf=payload.cpf,
            data_nascimento=payload.data_nascimento,
            version=1,
            deleted=False,
        )
        sess.add(p)
        sess.flush()  # para obter ID
        # evento de criação: base 0 -> new 1
        changes = {
            "nome": p.nome,
            "cpf": p.cpf,
            "data_nascimento": p.data_nascimento.isoformat(),
            "deleted": False,
        }
        persist_event(sess, p, base_version=0, changes=changes)
        sess.commit()
        sess.refresh(p)
        return p
    finally:
        sess.close()

# 4) Editar (PATCH) com OCC + replay via log
@app.patch("/pessoas/{pessoa_id}", response_model=PessoaOut)
def patch_pessoa(pessoa_id: int, payload: PessoaPatch = Body(...)):
    sess = SessionLocal()
    try:
        p = sess.query(Pessoa).filter(Pessoa.id == pessoa_id, Pessoa.deleted == False).one_or_none()  # noqa: E712
        if not p:
            raise HTTPException(status_code=404, detail="Pessoa não encontrada")

        current_version = p.version
        client_version = payload.version
        if client_version <= 0:
            raise HTTPException(status_code=400, detail="Versão inválida")

        # definir changes enviados pelo cliente
        provided_changes: Dict[str, Any] = {}
        if payload.nome is not None:
            provided_changes["nome"] = payload.nome
        if payload.cpf is not None:
            # opcional: revalidar unicidade se cpf alterar
            other = (
                sess.query(Pessoa)
                .filter(Pessoa.cpf == payload.cpf, Pessoa.id != pessoa_id, Pessoa.deleted == False)
                .one_or_none()
            )
            if other:
                raise HTTPException(status_code=409, detail="CPF já utilizado por outra pessoa")
            provided_changes["cpf"] = payload.cpf
        if payload.data_nascimento is not None:
            provided_changes["data_nascimento"] = payload.data_nascimento.isoformat()

        if not provided_changes:
            # nada a alterar, retorna atual
            return p

        if client_version == current_version:
            # caminho simples: aplica direto
            base_version = current_version
            # aplicar no ORM
            if "nome" in provided_changes:
                p.nome = provided_changes["nome"]
            if "cpf" in provided_changes:
                p.cpf = provided_changes["cpf"]
            if "data_nascimento" in provided_changes:
                p.data_nascimento = date.fromisoformat(provided_changes["data_nascimento"])
            p.version = current_version + 1
            sess.flush()
            persist_event(sess, p, base_version=base_version, changes=provided_changes)
            sess.commit()
            sess.refresh(p)
            return p

        if client_version > current_version:
            # cliente está à frente? inválido
            raise HTTPException(status_code=409, detail="Versão do cliente adiantada em relação ao servidor")

        # caminho de replay: cliente está atrasado (ex: v3 num servidor v5)
        # 1) reconstruir snapshot em client_version
        base_snapshot = snapshot_at_version(sess, pessoa_id, client_version)
        # 2) aplicar mudanças do cliente
        merged = apply_changes(base_snapshot, provided_changes)
        # 3) re-aplicar eventos até a versão atual
        final_state = replay_forward(sess, pessoa_id, client_version, current_version, merged)

        # 4) persistir como nova versão (current_version + 1)
        base_version = current_version
        # aplicar no ORM com final_state
        p.nome = final_state["nome"]
        p.cpf = final_state["cpf"]
        p.data_nascimento = date.fromisoformat(final_state["data_nascimento"])
        p.version = current_version + 1
        sess.flush()

        # changes registrados devem ser o delta em relação ao estado antes da escrita (estado atual antes do patch)
        prev_state = pessoa_to_dict(p)
        # prev_state já reflete p.version incrementada; ajustamos para comparar com estado antes (current_version)
        # Para manter simples, registramos as alterações que efetivamente foram aplicadas agora:
        changes_now = {
            "nome": p.nome,
            "cpf": p.cpf,
            "data_nascimento": p.data_nascimento.isoformat(),
        }
        persist_event(sess, p, base_version=base_version, changes=changes_now)
        sess.commit()
        sess.refresh(p)
        return p
    finally:
        sess.close()

# 5) Remover (soft delete) com OCC
@app.delete("/pessoas/{pessoa_id}", response_model=PessoaOut)
def delete_pessoa(pessoa_id: int, version: int = Query(..., description="Versão conhecida pelo cliente")):
    sess = SessionLocal()
    try:
        p = sess.query(Pessoa).filter(Pessoa.id == pessoa_id, Pessoa.deleted == False).one_or_none()  # noqa: E712
        if not p:
            raise HTTPException(status_code=404, detail="Pessoa não encontrada")
        if version < p.version:
            # podemos optar por replay para deletar mesmo com cliente atrasado
            # aqui, rejeitamos para deixar claro
            raise HTTPException(status_code=409, detail="Versão desatualizada para DELETE")

        base_version = p.version
        p.deleted = True
        p.version = p.version + 1
        sess.flush()
        changes = {"deleted": True}
        persist_event(sess, p, base_version=base_version, changes=changes)
        sess.commit()
        sess.refresh(p)
        return p
    finally:
        sess.close()

# Health
@app.get("/health")
def health():
    return {"status": "ok"}