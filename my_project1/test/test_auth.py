from unittest.mock import patch
import bcrypt


def test_register_integracao(client_app1, db_conexao):
    with patch('main.conexao', db_conexao):
        resp = client_app1.post('/register', json={
            'usuario': 'integracao',
            'senha': '123456'
        })

    assert resp.status_code == 201


def test_register_usuario_duplicado(client_app1, db_conexao):
    with db_conexao() as cursor:
        cursor.execute(
            'INSERT INTO usuarios (usuario, senha_hash) VALUES (%s, %s)',
            ('duplicado', 'hash')
        )

    with patch('main.conexao', db_conexao):
        resp = client_app1.post('/register', json={
            'usuario': 'duplicado',
            'senha': '123456'
        })

    assert resp.status_code in (400, 409)


def test_login_integracao(client_app1, db_conexao):
    senha = '123456'
    hash_ = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()

    with db_conexao() as cursor:
        cursor.execute(
            'INSERT INTO usuarios (usuario, senha_hash) VALUES (%s, %s)',
            ('Isaias', hash_)
        )

    with patch('main.conexao', db_conexao), \
         patch('main.ip_bloqueado', return_value=False):

        resp = client_app1.post('/login', json={
            'usuario': 'Isaias',
            'senha': senha
        })

    assert resp.status_code == 200
    assert 'access_token' in resp.json


def test_login_senha_invalida(client_app1, db_conexao):
    senha = '123456'
    hash_ = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()

    with db_conexao() as cursor:
        cursor.execute(
            'INSERT INTO usuarios (usuario, senha_hash) VALUES (%s, %s)',
            ('Isaias', hash_)
        )

    with patch('main.conexao', db_conexao), \
         patch('main.ip_bloqueado', return_value=False):

        resp = client_app1.post('/login', json={
            'usuario': 'Isaias',
            'senha': 'senha_errada'
        })

    assert resp.status_code == 401


def test_refresh_sem_cookie(client_app1):
    resp = client_app1.post('/refresh')

    assert resp.status_code == 401


def test_refresh_integracao(client_app1, db_conexao):
    with patch('main.conexao', db_conexao), \
         patch('main.validar_token', return_value=({'sub': 1}, 200)), \
         patch('main.gerar_tokens', return_value=({
             'access_token': 'novo',
             'refresh_token': 'novo_refresh',
             'refresh_exp': '2026-01-01'
         }, 200)):

        client_app1.set_cookie(
            key='refresh_token',
            value='token',
            domain='localhost'
        )

        resp = client_app1.post('/refresh')

    assert resp.status_code in (200, 401)


def test_logout_integracao(client_app1):
    client_app1.set_cookie(
        key='refresh_token',
        value='token_valido',
        domain='localhost'
    )

    resp = client_app1.post('/logout')

    assert resp.status_code == 200
    assert 'mensagem' in resp.json

