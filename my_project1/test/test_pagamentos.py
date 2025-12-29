from unittest.mock import Mock


def test_pagamento_sem_token(client_app4):
    resp = client_app4.get(
        '/registros-pagamento'
    )

    assert resp.status_code == 401


def test_listar_pagamentos(client_app4, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchall(self):
                return [(1, 'João', 'Carlos',
                        'pix', 'pago', 'confirmada', 20)]
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app4.get(
        '/registros-pagamento',
        headers=auth_header
    )

    assert resp.status_code == 200
    assert resp.json[0]['metodo_pagamento'] == 'pix'
    assert resp.json[0]['pagamento'] == 'pago'
    assert hasattr(cursor_holder['cursor'], 'called')


def test_buscar_pagamento_por_id_sucesso(client_app4, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self):
                return (1, 'João', 'Carlos',
                        'pix', 'pago', 'confirmada', 20)
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app4.get(
        '/registros-pagamento/1',
        headers=auth_header
    )

    assert resp.status_code == 200
    assert resp.json['id'] == 1
    assert hasattr(cursor_holder['cursor'], 'called')


def test_buscar_pagamento_por_id_inexistente(client_app4, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self):
                return None
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor

    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app4.get(
        '/registros-pagamento/999',
        headers=auth_header
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'],'called')


def test_adicionar_pagamento_sucesso(client_app4, auth_header, monkeypatch):
    fake_resp = Mock()
    fake_resp.json.return_value = {
        'nome_passageiro': 'João',
        'nome_motorista': 'Carlos',
        'metodo_pagamento': 'pix',
        'pagamento': 'pago',
        'status': 'confirmada',
        'total_viagem': 20
    }
    fake_resp.raise_for_status = lambda: None

    monkeypatch.setattr('app1.main.requests.get', lambda *args, **kwargs: fake_resp)

    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self): return (1,)
            lastrowid = 1
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app4.post(
        '/registros-pagamento',
        headers=auth_header,
        json={'id_viagem': 1}
    )

    assert resp.status_code == 201
    assert 'mensagem' in resp.json or 'id' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_atualizar_pagamento_sucesso(client_app4, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            rowcount = 1
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor

    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app4.put(
        '/registros-pagamento/1',
        headers=auth_header,
        json={
            'status': 'cancelado'
        }
    )

    assert resp.status_code == 200
    assert 'mensagem' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_atualizar_pagamento_inexistente(client_app4, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            rowcount = 0
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app4.put(
        '/registros-pagamento/999',
        headers=auth_header,
        json={
            'status': 'cancelado'
        }
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_deletar_pagamento_sucesso(client_app4, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            rowcount = 1
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app4.delete(
        '/registros-pagamento/1',
        headers=auth_header
    )

    assert resp.status_code in (200, 204)
    if resp.status_code == 200:
        assert 'mensagem' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_deletar_pagamento_inexistente(client_app4, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            rowcount = 0
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app4.delete(
        '/registros-pagamento/999',
        headers=auth_header
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')
