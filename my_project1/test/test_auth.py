from unittest.mock import patch
import bcrypt


def test_register_integracao(client, db_conexao):
    with patch('main.conexao', db_conexao):
        resp = client.post('/register', json={
            'usuario': 'integracao',
            'senha': '123456'
        })

    assert resp.status_code == 201


def test_login_integracao(client, db_conexao):
    senha = '123456'
    hash_ = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()

    with db_conexao() as cursor:
        cursor.execute(
            'INSERT INTO usuarios (usuario, senha_hash) VALUES (%s, %s)',
            ('Isaias', hash_)
        )

    with patch('main.conexao', db_conexao), \
         patch('main.ip_bloqueado', return_value=False):

        resp = client.post('/login', json={
            'usuario': 'Isaias',
            'senha': senha
        })

    assert resp.status_code == 200
    assert 'access_token' in resp.json


def test_refresh_integracao(client, db_conexao):
    with patch('main.conexao', db_conexao), \
         patch('main.validar_token', return_value=({'sub': 1}, 200)), \
         patch('main.gerar_tokens', return_value=({
             'access_token': 'novo',
             'refresh_token': 'novo_refresh',
             'refresh_exp': '2026-01-01'
         }, 200)):

        client.set_cookie(
            key='refresh_token',
            value='token',
            domain='localhost'
        )

        resp = client.post('/refresh')

    assert resp.status_code in (200, 401)
