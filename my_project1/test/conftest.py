import pytest
from test.test_database import init_test_db, criar_tabelas, fake_conexao
from main import (app1,
                   app2,
                    app3,
                     app4)


@pytest.fixture(autouse=True)
def limpar_banco():
    with fake_conexao() as cursor:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute('DELETE FROM refresh_tokens')
        cursor.execute('DELETE FROM usuarios')
        cursor.execute('DELETE FROM passageiros')
        cursor.execute('DELETE FROM motoristas')
        cursor.execute('DELETE FROM viagens')
        cursor.execute('DELETE FROM registros_pagamento')
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        


@pytest.fixture(scope='session', autouse=True)
def setup_test_db():
    init_test_db()
    criar_tabelas()


@pytest.fixture
def client_app1():
    app1.config['TESTING'] = True
    with app1.app_context():
        with app1.test_client() as client:
            yield client


@pytest.fixture
def client_app2():
    app2.config['TESTING'] = True
    with app2.app_context():
        with app2.test_client() as client:
            yield client


@pytest.fixture
def client_app3():
    app3.config['TESTING'] = True
    with app3.app_context():
        with app3.test_client() as client:
            yield client


@pytest.fixture
def client_app4():
    app4.config['TESTING'] = True
    with app4.app_context():
        with app4.test_client() as client:
            yield client


@pytest.fixture
def db_conexao():
    return fake_conexao


@pytest.fixture
def auth_headers():
    return {'Authorization': 'Bearer token_valido'}
