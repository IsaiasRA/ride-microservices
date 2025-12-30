def criar_passageiro(client, auth_header):
    return client.post(
        '/passageiros',
        headers=auth_header,
        json={
            'nome': 'João',
            'telefone': '9999',
            'cpf': '12345678909',
            'valor': 100,
            'endereco_rua': 'Rua X',
            'endereco_numero': '1',
            'endereco_bairro': 'Centro',
            'endereco_cidade': 'SP',
            'endereco_estado': 'SP',
            'endereco_cep': '00000',
            'km': 10,
            'metodo_pagamento': 'pix',
            'pagamento': 'pago'
        }
    )


def test_buscar_passageiro_por_id_sucesso(client_app1, auth_header):
    criar_passageiro(client_app1, auth_header)

    resp = client_app1.get('/passageiros/1', headers=auth_header)
    assert resp.status_code == 200
