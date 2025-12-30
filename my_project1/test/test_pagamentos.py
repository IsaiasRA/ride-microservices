def criar_pagamento(client, auth_header):
    return client.post(
        '/registros-pagamento',
        headers=auth_header,
        json={
            'id_viagem': 1,
            'remetente': 'João',
            'recebedor': 'Carlos',
            'status': 'confirmado'
        }
    )


def test_buscar_pagamento_por_id_sucesso(client_app4, auth_header):
    criar_pagamento(client_app4, auth_header)

    resp = client_app4.get('/registros-pagamento/1', headers=auth_header)
    assert resp.status_code == 200
