from unittest.mock import patch, MagicMock
from test.test_database import fake_conexao



def inserir_passageiro_e_motorista():
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO passageiros
            (nome, cpf, telefone, valor, endereco_rua, endereco_numero,
             endereco_bairro, endereco_cidade, endereco_estado,
             endereco_cep, km, metodo_pagamento, pagamento)
            VALUES
            ('Maria', '77463529090', '11988809753', 150,
             'Rua A', '10', 'Centro', 'SP', 'SP',
             '01000000', 5, 'pix', 'pago')
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


def mock_requests_passageiro_motorista(id_passageiro, id_motorista):
    resp_passa = MagicMock()
    resp_passa.json.return_value = {
        'nome': 'Maria',
        'cpf': '77463529090',
        'telefone': '11988809753',
        'valor': 150,
        'endereco_rua': 'Rua A',
        'endereco_numero': '10',
        'endereco_bairro': 'Centro',
        'endereco_cidade': 'SP',
        'endereco_estado': 'SP',
        'endereco_cep': '01000000',
        'km': 5,
        'metodo_pagamento': 'pix',
        'pagamento': 'pago'
    }
    resp_passa.raise_for_status.return_value = None

    resp_moto = MagicMock()
    resp_moto.json.return_value = {
        'nome': 'João',
        'cnh': '85858720591',
        'telefone': '21999290284',
        'categoria_cnh': 'A',
        'placa': 'PKJ2I10',
        'modelo_carro': 'Nissan Barra',
        'ano_carro': 2018,
        'status': 'ativo',
        'valor_passagem': 5,
        'quantia': 700
    }
    resp_moto.raise_for_status.return_value = None

    return [resp_passa, resp_moto]


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
             valor_por_km, total_viagem, metodo_pagamento, pagamento)
            VALUES
            (%s, %s, 'Maria', 'João',
             'Rua A', '10', 'Centro',
             'SP', 'SP', '01000000',
             5, 50, 'pix', 'pago')
        """, (id_passageiro, id_motorista))

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.get('/viagens', headers=auth_headers)

    assert resp.status_code == 200
    assert isinstance(resp.json, list)
    assert len(resp.json) == 1
    assert resp.json[0]['nome_passageiro'] == 'Maria'


def test_listar_viagens_vazio(client_app3, db_conexao, auth_headers):

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.get('/viagens', headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json == []


def test_buscar_viagem(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO viagens
            (id_passageiro, id_motorista, nome_passageiro, nome_motorista,
             endereco_rua, endereco_numero, endereco_bairro,
             endereco_cidade, endereco_estado, endereco_cep,
             valor_por_km, total_viagem, metodo_pagamento, pagamento)
            VALUES
            (%s, %s, 'Maria', 'João',
             'Rua A', '10', 'Centro',
             'SP', 'SP', '01000000',
             5, 50, 'pix', 'pago')
        """, (id_passageiro, id_motorista))
        viagem_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.get(f'/viagens/{viagem_id}', headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json['id'] == viagem_id


def test_buscar_viagem_inexistente(client_app3, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.get('/viagens/99999', headers=auth_headers)

    assert resp.status_code == 404
    assert 'erro' in resp.json


def test_adicionar_viagem(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()
    mocks = mock_requests_passageiro_motorista(id_passageiro, id_motorista)

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)), \
         patch('requests.get', side_effect=mocks):

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
    resp_mock = MagicMock()
    resp_mock.raise_for_status.side_effect = Exception('Passageiro não encontrado')

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)), \
         patch('requests.get', return_value=resp_mock):

        resp = client_app3.post(
            '/viagens',
            headers=auth_headers,
            json={
                'id_passageiro': 999,
                'id_motorista': 1
            }
        )

    assert resp.status_code == 404


def test_adicionar_viagem_motorista_inexistente(client_app3, db_conexao, auth_headers):
    resp_passa = MagicMock()
    resp_passa.json.return_value = {'nome': 'Maria'}
    resp_passa.raise_for_status.return_value = None

    resp_moto = MagicMock()
    resp_moto.raise_for_status.side_effect = Exception('Motorista não encontrado')

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)), \
         patch('requests.get', side_effect=[resp_passa, resp_moto]):

        resp = client_app3.post(
            '/viagens',
            headers=auth_headers,
            json={
                'id_passageiro': 1,
                'id_motorista': 999
            }
        )

    assert resp.status_code == 404


def test_atualizar_viagem(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO viagens
            (id_passageiro, id_motorista, nome_passageiro, nome_motorista,
             endereco_rua, endereco_numero, endereco_bairro,
             endereco_cidade, endereco_estado, endereco_cep,
             valor_por_km, total_viagem, metodo_pagamento, pagamento)
            VALUES
            (%s, %s, 'Maria', 'João',
             'Rua A', '10', 'Centro',
             'SP', 'SP', '01000000',
             5, 50, 'pix', 'pago')
        """, (id_passageiro, id_motorista))
        viagem_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.put(
            f'/viagens/{viagem_id}',
            headers=auth_headers,
            json={
                'status': 'cancelada'
            }
        )

    assert resp.status_code == 200
    assert resp.json['atualizado']['status'] == 'cancelada'


def test_atualizar_viagem_inexistente(client_app3, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.put(
            '/viagens/99999',
            headers=auth_headers,
            json={
                'status': 'cancelada'
            }
        )

    assert resp.status_code == 404


def test_deletar_viagem(client_app3, db_conexao, auth_headers):
    id_passageiro, id_motorista = inserir_passageiro_e_motorista()

    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO viagens
            (id_passageiro, id_motorista, nome_passageiro, nome_motorista,
             endereco_rua, endereco_numero, endereco_bairro,
             endereco_cidade, endereco_estado, endereco_cep,
             valor_por_km, total_viagem, metodo_pagamento, pagamento)
            VALUES
            (%s, %s, 'Maria', 'João',
             'Rua A', '10', 'Centro',
             'SP', 'SP', '01000000',
             5, 50, 'pix', 'pago')
        """, (id_passageiro, id_motorista))
        viagem_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.delete(
            f'/viagens/{viagem_id}',
            headers=auth_headers
        )

    assert resp.status_code == 204


def test_deletar_viagem_inexistente(client_app3, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app3.delete(
            '/viagens/99999',
            headers=auth_headers
        )

    assert resp.status_code == 404
