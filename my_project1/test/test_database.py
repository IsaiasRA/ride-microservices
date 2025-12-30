from contextlib import contextmanager, closing
import sqlite3
import pytest


@contextmanager
def fake_conexao():
    with closing(sqlite3.connect('test.db')) as con:
        cursor = con.cursor()
        try:
            yield cursor
            con.commit()
        except Exception:
            con.rollback()
            raise

@pytest.fixture(scope='session')
def setup_fake_db():
    with fake_conexao() as cursor:
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS usuarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    usuario TEXT NOT NULL UNIQUE,
                    senha_hash TEXT NOT NULL,
                    criado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                ''')
        
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS passageiros (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT NOT NULL,
                    total_viagem REAL NOT NULL CHECK(total_viagem > 0),
                    metodo_pagamento TEXT NOT NULL CHECK(
                        metodo_pagamento IN ('pix', 'credito', 'debito', 'boleto')),
                    pagamento TEXT NOT NULL CHECK(
                    pagamento IN ('pago', 'pendente', 'cancelado'))
                    );
                ''')
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS motoristas (
                    id INTEGER AUTOINCREMENT PRIMARY KEY,
                    nome TEXT NOT NULL,
                    valor_passagem REAL NOT NULL CHECK(valor_passagem > 0),
                    status TEXT NOT NULL CHECK(
                    status IN ('ativo', 'suspenso', 'bloqueado'))
                    );
                ''')
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS viagens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_passageiro INTEGER NOT NULL,
                    id_motorista INTEGER NOT NULL,
                    nome_passageiro TEXT NOT NULL,
                    nome_motorista TEXT NOT NULL,
                    valor_passagem REAL NOT NULL CHECK(valor_passagem > 0),
                    total_viagem REAL NOT NULL CHECK(total_viagem > 0),
                    metodo_pagamento TEXT NOT NULL CHECK(
                    metodo_pagamento IN ('pix', 'credito', 'debito', 'boleto')),
                    pagamento TEXT NOT NULL CHECK(
                    pagamento IN ('pago', 'pendente', 'cancelado')),
                    status TEXT NOT NULL CHECK(status IN ('confirmada', 'cancelada')),
                    criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                    atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            ''')
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS registros_pagamento (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome_passageiro TEXT NOT NULL,
                    nome_motorista TEXT NOT NULL,
                    total_viagem REAL NOT NULL CHECK(total_viagem > 0),
                    metodo_pagamento TEXT NOT NULL CHECK(
                    metodo_pagamento IN ('pix', 'credito', 'debito', 'boleto')),
                    pagamento TEXT NOT NULL CHECK(
                    pagamento IN ('pago', 'pendente', 'cancelado')),
                    status TEXT NOT NULL CHECK(
                    status IN ('confirmado', 'cancelado'))
                );
            ''')