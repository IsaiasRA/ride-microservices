def criar_viagem(client, auth_header):
    return client.post(
        '/viagens',
        headers=auth_header,
        json={
            'id_passageiro': 1,
            'id_motorista': 1,
            'nome_passageiro': 'João',
            'nome_motorista': 'Carlos',
            'total_viagem': 50,
            'valor_por_km': 2.50,
            'status': 'confirmada'
        }
    )


def test_buscar_viagem_por_id_sucesso(client_app3, auth_header):
    criar_viagem(client_app3, auth_header)

    resp = client_app3.get('/viagens/1', headers=auth_header)
    assert resp.status_code == 200
