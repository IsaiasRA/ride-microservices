def criar_motorista(client, auth_header):
    return client.post(
        '/motoristas',
        headers=auth_header,
        json={
            'nome': 'Carlos',
            'status': 'ativo',
            'cnh': '12345678900',
            'telefone': '11999999999',
            'categoria_cnh': 'B',
            'placa': 'ABC1234',
            'modelo_carro': 'Onix',
            'ano_carro': 2020,
            'atualizado_em': 'now'
        }
    )


def test_buscar_motorista_por_id_sucesso(client_app2, auth_header):
    criar_motorista(client_app2, auth_header)

    resp = client_app2.get('/motoristas/1', headers=auth_header)
    assert resp.status_code == 200
    assert 'id' in resp.json


def test_atualizar_motorista_sucesso(client_app2, auth_header):
    criar_motorista(client_app2, auth_header)

    resp = client_app2.put(
        '/motoristas/1',
        headers=auth_header,
        json={'status': 'suspenso', 'atualizado_em': 'now'}
    )

    assert resp.status_code == 200


def test_deletar_motorista_sucesso(client_app2, auth_header):
    criar_motorista(client_app2, auth_header)

    resp = client_app2.delete('/motoristas/1', headers=auth_header)
    assert resp.status_code in (200, 204)
