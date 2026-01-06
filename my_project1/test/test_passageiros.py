from unittest.mock import patch
from test.test_database import fake_conexao



def test_listar_passageiros_sem_token(client_app1):
    resp = client_app1.get('/passageiros')

    assert resp.status_code in (401, 403)


def test_listar_passageiros(client_app1, db_conexao, auth_headers):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO passageiros (
                nome, cpf, telefone, saldo, endereco_rua, endereco_numero,
                endereco_bairro, endereco_cidade, endereco_estado,
                endereco_cep, km, metodo_pagamento
            ) VALUES (
                'Maria', '98765432100', '11999999999', 50,
                'Rua B', '45', 'Bairro', 'Cidade',
                'RJ', '22000000', 8, 'pix'
            )
        """)

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.get(
            '/passageiros',
            headers=auth_headers
        )

    assert resp.status_code == 200
    assert len(resp.json) == 1


def test_listar_passageiros_vazio(client_app1, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.get('/passageiros', headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json == []


def test_buscar_passageiro_por_id(client_app1, db_conexao, auth_headers):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO passageiros (
                nome, cpf, telefone, saldo, endereco_rua, endereco_numero,
                endereco_bairro, endereco_cidade, endereco_estado,
                endereco_cep, km, metodo_pagamento
            ) VALUES (
                'Carlos', '11122233344', '11988887777', 30,
                'Rua C', '10', 'Centro', 'Cidade',
                'MG', '30000000', 5, 'debito'
            )
        """)
        novo_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.get(
            f'/passageiros/{novo_id}',
            headers=auth_headers
        )

    assert resp.status_code == 200
    assert resp.json['nome'] == 'Carlos'


def test_buscar_passageiro_por_id_inexistente(client_app1, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.get('/passageiros/99999', headers=auth_headers)

    assert resp.status_code == 404
    assert 'erro' in resp.json


def test_adicionar_passageiro(client_app1, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.post(
            '/passageiros',
             headers=auth_headers,
             json={
            'nome': 'Joao Silva',
            'cpf': '12345678909',
            'telefone': '11988887777',
            'saldo': 25.50,
            'endereco_rua': 'Rua A',
            'endereco_numero': '123',
            'endereco_bairro': 'Centro',
            'endereco_cidade': 'Sao Paulo',
            'endereco_estado': 'SP',
            'endereco_cep': '01001000',
            'km': 10.5,
            'metodo_pagamento': 'pix'
        })

    assert resp.status_code == 201
    assert 'id' in resp.json


def test_adicionar_passageiro_campo_faltando(client_app1, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.post(
            '/passageiros',
            headers=auth_headers,
            json={
                'cpf': '12345678900',
                'telefone': '11999999999'
            }
        )

    assert resp.status_code == 400
    assert 'erro' in resp.json


def test_adicionar_passageiro_valor_negativo(client_app1, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.post(
            '/passageiros',
            headers=auth_headers,
            json={
                'nome': 'Teste',
                'cpf': '12345678900',
                'telefone': '11999999999',
                'saldo': -10,
                'endereco_rua': 'Rua X',
                'endereco_numero': '1',
                'endereco_bairro': 'Centro',
                'endereco_cidade': 'SP',
                'endereco_estado': 'SP',
                'endereco_cep': '00000000',
                'km': 5,
                'metodo_pagamento': 'pix'
            }
        )

    assert resp.status_code == 400
    assert 'erro' in resp.json


def test_atualizar_passageiro(client_app1, db_conexao, auth_headers):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO passageiros (
                nome, cpf, telefone, saldo, endereco_rua, endereco_numero,
                endereco_bairro, endereco_cidade, endereco_estado,
                endereco_cep, km, metodo_pagamento
            ) VALUES (
                'Ana', '43315690866', '11977776666', 40,
                'Rua D', '20', 'Bairro', 'Cidade',
                'SP', '04000000', 7, 'credito'
            )
        """)
        novo_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.put(
            f'/passageiros/{novo_id}',
            headers=auth_headers,
            json={
            'saldo': 45.00
        })

    assert resp.status_code == 200
    assert 'saldo' in resp.json['atualizado']


def test_atualizar_passageiro_inexistente(client_app1, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.put(
            '/passageiros/99999',
            headers=auth_headers,
            json={
                'saldo': 30.00
            }
        )

    assert resp.status_code == 404
    assert 'erro' in resp.json


def test_deletar_passageiro(client_app1, db_conexao, auth_headers):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO passageiros (
                nome, cpf, telefone, saldo, endereco_rua, endereco_numero,
                endereco_bairro, endereco_cidade, endereco_estado,
                endereco_cep, km, metodo_pagamento
            ) VALUES (
                'Pedro', '99988877766', '11966665555', 20,
                'Rua E', '5', 'Centro', 'Cidade',
                'BA', '40000000', 3, 'pix'
            )
        """)
        novo_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.delete(
            f'/passageiros/{novo_id}',
            headers=auth_headers
        )

    assert resp.status_code == 204


def test_deletar_passageiro_inexistente(client_app1, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app1.delete(
            '/passageiros/99999',
            headers=auth_headers
        )

    assert resp.status_code == 404
