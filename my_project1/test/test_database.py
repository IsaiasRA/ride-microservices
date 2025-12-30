from contextlib import contextmanager, closing
import sqlite3


@contextmanager
def fake_conexao():
    with closing(sqlite3.connect('test.db')) as con:
        con.row_factory = sqlite3.Row
        cursor = con.cursor()
        try:
            yield cursor
            con.commit()
        except:
            con.rollback()
            raise


with fake_conexao() as cursor:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT,
            senha_hash TEXT
        );
    """)

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                token_hash TEXT,
                criado_em DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS passageiros (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT,
            telefone TEXT,
            cpf TEXT,
            valor REAL,
            endereco_rua TEXT,
            endereco_numero TEXT,
            endereco_bairro TEXT,
            endereco_cidade TEXT,
            endereco_estado TEXT,
            endereco_cep TEXT,
            km INTEGER,
            metodo_pagamento TEXT,
            pagamento TEXT,
            atualizado_em TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS motoristas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT,
            status TEXT,
            telefone TEXT,
            cnh TEXT,
            placa TEXT,
            modelo_carro TEXT,
            ano_carro INTEGER,
            atualizado_em TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS viagens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_passageiro INTEGER,
            id_motorista INTEGER,
            nome_passageiro TEXT,
            nome_motorista TEXT,
            endereco_rua TEXT,
            endereco_numero TEXT,
            endereco_bairro TEXT,
            endereco_cidade TEXT,
            endereco_estado TEXT,
            endereco_cep TEXT,
            valor_por_km REAL,
            total_viagem REAL,
            metodo_pagamento TEXT,
            pagamento TEXT,
            status TEXT,
            criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
            atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS registros_pagamento (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_viagem INTEGER,
            remetente TEXT,
            recebedor TEXT,
            metodo_pagamento TEXT
            pagamento TEXT,
            status TEXT,
            valor_viagem REAL,
            criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
            atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)
