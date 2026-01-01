from unittest.mock import patch, MagicMock
from decimal import Decimal
from test.test_database import fake_conexao


def inserir_passageiro():
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO passageiros
            (nome, cpf, telefone, valor, endereco_rua, endereco_numero,
            endereco_bairro, endereco_cidade, endereco_estado,
            endereco_cep, km, metodo_pagamento, pagamento
            )VALUES
            ('Maria', '96049202451', '11999206789', 150, 'Rua A',
             '10', 'Centro', 'SP', 'SP', '01000000',
             5, 'pix', 'pago'
        )
    """)
        return cursor.lastrowid


def inserir_motorista():
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO motoristas
            (nome, cnh, telefone, categoria_cnh, placa,
            modelo_carro, ano_carro, status, valor_passagem, quantia
            ) VALUES
            ('João', '96902285291', '71988809295', 'D', 'HT1YO19',
             'Ford Ranger', 2024, 'ativo', 5, 200
        )
    """)
        return cursor.lastrowid


def inserir_viagem():
    id_passageiro = inserir_passageiro()
    id_motorista = inserir_motorista()

    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO viagens
            (id_passageiro, id_motorista, nome_passageiro, nome_motorista,
             endereco_rua, endereco_numero, endereco_bairro,
             endereco_cidade, endereco_estado, endereco_cep,
             valor_por_km, total_viagem, metodo_pagamento, pagamento, status)
            VALUES
            (%s, %s, 'Maria', 'João',
             'Rua A', '10', 'Centro',
             'SP', 'SP', '01000000',
             5, 50, 'pix', 'pago', 'concluida')
        """, (id_passageiro, id_motorista))
        return cursor.lastrowid


def inserir_registro_pagamento(id_viagem):
    with fake_conexao() as cursor:
        cursor.execute("""
            INSERT INTO registros_pagamento
            (id_viagem, remetente, recebedor, metodo_pagamento,
             pagamento, status, valor_viagem)
            VALUES
            (%s, 'Maria', 'João', 'pix', 'pago', 'concluido', 50)
        """, (id_viagem,))
        return cursor.lastrowid


def mock_viagem_api():
    resp = MagicMock()
    resp.json.return_value = {
        'nome_passageiro': 'Maria',
        'nome_motorista': 'João',
        'endereco_rua': 'Rua A',
        'endereco_numero': '10',
        'endereco_bairro': 'Centro',
        'endereco_cidade': 'SP',
        'endereco_estado': 'SP',
        'endereco_cep': '01000000',
        'valor_por_km': 5,
        'total_viagem': 50,
        'metodo_pagamento': 'pix',
        'pagamento': 'pago',
        'status': 'concluida'
    }
    resp.raise_for_status.return_value = None
    return resp


def test_listar_registros_pagamento_sem_token(client_app4):
    resp = client_app4.get('/registros-pagamento')

    assert resp.status_code in (401, 403)


def test_listar_registros_pagamento(client_app4, db_conexao, auth_headers):
    id_viagem = inserir_viagem()
    inserir_registro_pagamento(id_viagem)

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.get('/registros-pagamento', headers=auth_headers)

    assert resp.status_code == 200
    assert len(resp.json) == 1


def test_listar_registros_pagamento_vazio(client_app4, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.get('/registros-pagamento', headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json == []


def test_buscar_registro_pagamento(client_app4, db_conexao, auth_headers):
    id_viagem = inserir_viagem()
    registro_id = inserir_registro_pagamento(id_viagem)

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.get(
            f'/registros-pagamento/{registro_id}',
            headers=auth_headers
        )

    assert resp.status_code == 200
    assert resp.json['id'] == registro_id


def test_buscar_registro_pagamento_inexistente(client_app4, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.get(
            '/registros-pagamento/9999',
            headers=auth_headers
        )

    assert resp.status_code == 404
    assert 'erro' in resp.json


def test_adicionar_registro_pagamento(client_app4, db_conexao, auth_headers):
    id_viagem = inserir_viagem()

    with patch('main.conexao', db_conexao), \
         patch('requests.get', return_value=mock_viagem_api()), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.post(
            '/registros-pagamento',
            headers=auth_headers,
            json={
                'id_viagem': id_viagem
            }
        )

    assert resp.status_code == 201
    assert 'sucesso' in resp.json['mensagem'].lower()


def test_adicionar_registro_pagamento_inexistente(client_app4, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('requests.get', side_effect=Exception("Viagem não encontrada")), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.post(
            '/registros-pagamento',
            headers=auth_headers,
            json={
                'id_viagem': 99999
            }
        )

    assert resp.status_code in (400, 404)
    assert 'erro' in resp.json


def test_atualizar_registro_pagamento(client_app4, db_conexao, auth_headers):
    id_viagem = inserir_viagem()
    registro_id = inserir_registro_pagamento(id_viagem)

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.put(
            f'/registros-pagamento/{registro_id}',
            headers=auth_headers,
            json={
                'remetente': 'Maria Silva',
                'valor_viagem': str(Decimal('60.00'))
            }
        )

    assert resp.status_code == 200
    assert resp.json['atualizado']['remetente'] == 'Maria Silva'


def test_atualizar_registro_pagamento_inexistente(client_app4, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.put(
            '/registros-pagamento/99999',
            headers=auth_headers,
            json={
                'remetente': 'Teste'
            }
        )

    assert resp.status_code == 404
    assert 'erro' in resp.json


def test_deletar_registro_pagamento(client_app4, db_conexao, auth_headers):
    id_viagem = inserir_viagem()
    registro_id = inserir_registro_pagamento(id_viagem)

    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.delete(
            f'/registros-pagamento/{registro_id}',
            headers=auth_headers
        )

    assert resp.status_code == 204


def test_deletar_registro_pagamento_inexistente(client_app4, db_conexao, auth_headers):
    with patch('main.conexao', db_conexao), \
         patch('app1.auth.validar_token', return_value=({'sub': 1}, 200)):

        resp = client_app4.delete('/registros-pagamento/99999', headers=auth_headers)

    assert resp.status_code == 404
    assert 'erro' in resp.json
