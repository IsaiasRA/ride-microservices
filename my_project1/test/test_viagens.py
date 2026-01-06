from unittest.mock import patch
from test.test_database import fake_conexao



def inserir_passageiro_e_motorista():
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO passageiros
            (nome, cpf, telefone, saldo, endereco_rua, endereco_numero,
             endereco_bairro, endereco_cidade, endereco_estado,
             endereco_cep, km, metodo_pagamento)
            VALUES
            ('Maria', '77463529090', '11988809753', 150,
             'Rua A', '10', 'Centro', 'SP', 'SP',
             '01000000', 5, 'pix')
        """)
        id_passageiro = cursor.lastrowid

        cursor.execute("""
            INSERT INTO motoristas
            (nome, cnh, telefone, categoria_cnh, placa, modelo_carro,
            ano_carro, status, valor_passagem, quantia)
            VALUES 
            ('João', '85858720591', '21999290284', 'A',
            'PKJ2I10', 'Nissan Barra', 2018, 'ativo', 2.50, 700)
        """)
        id_motorista = cursor.lastrowid

    return id_passageiro, id_motorista


def inserir_viagem():
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO viagens
            (id_passageiro, id_motorista, nome_passageiro, nome_motorista,
             endereco_rua, endereco_numero, endereco_bairro,
             endereco_cidade, endereco_estado, endereco_cep,
             valor_por_km, total_viagem, metodo_pagamento, status)
            VALUES
            (%s, %s, 'Maria', 'João',
             'Rua A', '10', 'Centro',
             'SP', 'SP', '01000000',
             2.50, 12.50, 'pix', 'confirmada')
        """, (id_passageiro, id_motorista))

        return cursor.lastrowid


def test_listar_viagens_sem_token(client_app3):
    resp = client_app3.get('/viagens')

    assert resp.status_code in (401, 403)


def test_listar_viagens(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO viagens
            (id_passageiro, id_motorista, nome_passageiro, nome_motorista,
             endereco_rua, endereco_numero, endereco_bairro,
             endereco_cidade, endereco_estado, endereco_cep,
             valor_por_km, total_viagem, metodo_pagamento, status)
            VALUES
            (%s, %s, 'Maria', 'João',
             'Rua A', '10', 'Centro',
             'SP', 'SP', '01000000',
             2.50, 12.50, 'pix', 'confirmada')
        """, (id_passageiro, id_motorista))

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.get('/viagens', headers=auth_headers)

    assert resp.status_code == 200
    assert isinstance(resp.json, list)
    assert len(resp.json) == 1
    assert resp.json[0]['nome_passageiro'] == 'Maria'


def test_listar_viagens_vazio(client_app3, db_conexao, auth_headers):

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.get('/viagens', headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json == []


def test_buscar_viagem_por_id(client_app3, db_conexao, auth_headers):
    id_viagem = inserir_viagem()

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.get(f'/viagens/{id_viagem}', headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json['id'] == id_viagem


def test_buscar_viagem_inexistente(client_app3, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.get('/viagens/99999', headers=auth_headers)

    assert resp.status_code == 404
    assert 'erro' in resp.json


def test_adicionar_viagem(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.post(
            '/viagens',
            headers=auth_headers,
            json={
                'id_passageiro': id_passageiro,
                'id_motorista': id_motorista
            }
        )

    assert resp.status_code == 201
    assert 'id' in resp.json


def test_adicionar_viagem_passageiro_inexistente(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.post(
            '/viagens',
            headers=auth_headers,
            json={
                'id_passageiro': 999,
                'id_motorista': id_motorista
            }
        )

    assert resp.status_code in (400, 404)
    assert 'erro' in resp.json


def test_adicionar_viagem_motorista_inexistente(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.post(
            '/viagens',
            headers=auth_headers,
            json={
                'id_passageiro': id_passageiro,
                'id_motorista': 999
            }
        )

    assert resp.status_code in (400, 404)
    assert 'erro' in resp.json


def test_adicionar_viagem_campo_faltando(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.post(
            '/viagens',
            headers=auth_headers,
            json={
                'id_passageiro': id_passageiro
            }
        )

    assert resp.status_code == 400
    assert 'erro' in resp.json


def test_cancelar_viagem(client_app3, db_conexao, auth_headers):
    id_viagem = inserir_viagem()

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.patch(
            f'/viagens/{id_viagem}/cancelar',
            headers=auth_headers
        )

    assert resp.status_code == 204


def test_cancelar_viagem_duas_vezes(client_app3, db_conexao, auth_headers):
    id_viagem = inserir_viagem()

    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp1 = client_app3.patch(
            f'/viagens/{id_viagem}/cancelar',
            headers=auth_headers
        )
        resp2 = client_app3.patch(
            f'/viagens/{id_viagem}/cancelar',
            headers=auth_headers
        )

    assert resp1.status_code == 204
    assert resp2.status_code == 409


def test_cancelar_viagem_inexistente(client_app3, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.patch(
            '/viagens/99999/cancelar',
            headers=auth_headers
        )

    assert resp.status_code == 404
    assert 'erro' in resp.json
