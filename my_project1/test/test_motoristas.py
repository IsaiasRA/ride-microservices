from unittest.mock import patch
from decimal import Decimal
from test.test_database import fake_conexao



def test_listar_motoristas_sem_token(client_app2):
    resp = client_app2.get('/motoristas')

    assert resp.status_code in (401, 403)


def test_listar_motoristas(client_app2, db_conexao, auth_headers):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO motoristas (
                nome, cnh, telefone, categoria_cnh, placa,
                modelo_carro, ano_carro, status, valor_passagem, quantia
            ) VALUES (
                'Antonio', '89754290214', '21998397321', 'D',
                'TY6PO20', 'Corolla Xei', 2021, 'ativo', 30, 150
            )
        """)

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.get('/motoristas', headers=auth_headers)

        assert resp.status_code == 200
        assert len(resp.json) == 1


def test_listar_motoristas_vazio(client_app2, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.get('/motoristas', headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json == []


def test_buscar_motoristas(client_app2, db_conexao, auth_headers):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO motoristas (
                nome, cnh, telefone, categoria_cnh, placa,
                modelo_carro, ano_carro, status, valor_passagem, quantia
            ) VALUES (
                'Caio', '78509479383', '71999832820', 'A',
                'O9PUI12', 'Corolla Cross', 2022, 'ativo', 25, 120
            )
        """)
        novo_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.get(f'/motoristas/{novo_id}', headers=auth_headers)

        assert resp.status_code == 200
        assert resp.json['nome'] == 'Caio'


def test_buscar_motorista_inexistente(client_app2, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.get('/motoristas/99999', headers=auth_headers)

    assert resp.status_code == 404
    assert 'erro' in resp.json


def test_adicionar_motoristas(client_app2, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.post(
            '/motoristas',
            headers=auth_headers,
            json={
                'nome': 'Caio Santos',
                'cnh': '00567844290',
                'telefone': '71988892456',
                'categoria_cnh': 'B',
                'placa': 'MRC8C12',
                'modelo_carro': 'Corolla Xei',
                'ano_carro': 2022,
                'status': 'bloqueado',
                'valor_passagem': 30,
                'quantia': 200
            }
        )

        assert resp.status_code == 201
        assert 'id' in resp.json


def test_adicionar_motorista_campo_faltando(client_app2, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.post(
            '/motoristas',
            headers=auth_headers,
            json={
                'nome': 'Teste',
                'telefone': '11999999999'
            }
        )

    assert resp.status_code == 400
    assert 'erro' in resp.json


def test_adicionar_motorista_valor_negativo(client_app2, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.post(
            '/motoristas',
            headers=auth_headers,
            json={
                'nome': 'Teste',
                'cnh': '12345678901',
                'telefone': '11999999999',
                'categoria_cnh': 'B',
                'placa': 'XYZ9A99',
                'modelo_carro': 'Gol',
                'ano_carro': 2020,
                'status': 'ativo',
                'valor_passagem': -10,
                'quantia': 100
            }
        )

    assert resp.status_code == 201
    assert 'id' in resp.json
    

def test_atualizar_motorista(client_app2, db_conexao, auth_headers):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO motoristas (
                nome, cnh, telefone, categoria_cnh, placa,
                modelo_carro, ano_carro, status, valor_passagem, quantia
            ) VALUES (
                'Pedro', '74356353541', '6599452621', 'A',
                '0PO1UI9', 'Ford Ranger', 2023, 'ativo', 40, 400
            )
        """)
        novo_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.put(
            f'/motoristas/{novo_id}',
            headers=auth_headers,
            json={
                'nome': 'Pedro Silva',
                'valor_passagem': 35
            }
        )

        assert resp.status_code == 200
    

def test_atualizar_motorista_inexistente(client_app2, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.put(
            '/motoristas/99999',
            headers=auth_headers,
            json={
                'status': 'ativo'
            }
        )

    assert resp.status_code == 404


def test_deletar_motorista(client_app2, db_conexao, auth_headers):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO motoristas (
                nome, cnh, telefone, categoria_cnh, placa,
                modelo_carro, ano_carro, status, valor_passagem, quantia
            ) VALUES (
                'Gustavo', '95958585981', '11999280192', 'B',
                'ABC1D23', 'Nissan Versa', 2020, 'suspenso', 35, 200
            )
        """)
        novo_id = cursor.lastrowid

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.delete(
            f'/motoristas/{novo_id}',
            headers=auth_headers
        )

        assert resp.status_code == 204


def test_deletar_motorista_inexistente(client_app2, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app2.delete('/motoristas/99999', headers=auth_headers)

    assert resp.status_code == 404
