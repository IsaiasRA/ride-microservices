
def test_passageiros_sem_token(client_app1):
    resp = client_app1.get('/passageiros')
    assert resp.status_code == 401


def test_listar_passageiros_vazio(client_app1, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args): 
                self.called = True
            def fetchall(self): return []
    
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app1.get('/passageiros', headers=auth_header)
    assert resp.status_code == 200
    assert resp.json == []
    assert hasattr(cursor_holder['cursor'], 'called')


def test_buscar_passageiro_por_id_sucesso(client_app1, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self):
                return (1, 'João', 30, 'M')
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor

    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app1.get(
        '/passageiros/1',
        headers=auth_header
    )

    assert resp.status_code == 200
    assert resp.json == {
        'id': 1,
        'nome': 'João',
        'idade': 30,
        'sexo': 'M'
    }
    assert hasattr(cursor_holder['cursor'], 'called')


def test_buscar_passageiro_por_id_inexistente(client_app1, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self): return None
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app1.get(
        '/passageiros/999',
        headers=auth_header
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_adicionar_passageiro_sucesso(client_app1, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            lastrowid = 1
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app1.post(
        '/passageiros',
        headers=auth_header,
        json={
            'nome': 'João',
            'idade': 30,
            'sexo': 'M'
        }
    )

    assert resp.status_code in (200, 201)
    assert 'mensagem' in resp.json or 'id' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_adicionar_passageiro_sem_json(client_app1, auth_header):
    resp = client_app1.post(
        '/passageiros',
        headers=auth_header
    )

    assert resp.status_code == 400
    assert 'erro' in resp.json


def test_atualizar_passageiro_sucesso(client_app1, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            rowcount = 1
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app1.put(
        '/passageiros/1',
        headers=auth_header,
        json={
            'nome': 'João atualizado',
            'idade': 31,
            'sexo': 'M'
        }
    )

    assert resp.status_code == 200
    assert 'mensagem' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_atualizar_passageiro_inexistente(client_app1, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            rowcount = 0
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app1.put(
        '/passageiros/999',
        headers=auth_header,
        json={
            'nome': 'X',
            'idade': 20,
            'sexo': 'M'
        }
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_deletar_passageiro_sucesso(client_app1, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            rowcount = 1
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app1.delete(
        '/passageiros/1',
        headers=auth_header
    )

    assert resp.status_code in (200, 204)
    if resp.status_code == 200:
        assert 'mensagem' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_deletar_passageiro_inexistente(client_app1, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            rowcount = 0
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('main.conexao', fake_conexao)

    resp = client_app1.delete(
        '/passageiros/999',
        headers=auth_header
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')
