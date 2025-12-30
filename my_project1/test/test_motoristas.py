
def test_motoristas_sem_token(client_app2):
    resp = client_app2.get('/motoristas')
    assert resp.status_code == 401


def test_listar_motoristas_vazio(client_app2, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchall(self): return []
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app2.get('/motoristas', headers=auth_header)
    assert resp.status_code == 200
    assert resp.json == []
    assert hasattr(cursor_holder['cursor'], 'called')


def test_buscar_motoristas_por_id_sucesso(client_app2, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self):
                return (1, 'Carlos', 2.50, 'ativo')
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor

    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app2.get(
        '/motoristas/1',
        headers=auth_header
    )
    assert resp.status_code == 200
    assert resp.json == {
        'id': 1,
        'nome': 'Carlos',
        'valor_passagem': 2.50,
        'status': 'ativo'
    }
    assert hasattr(cursor_holder['cursor'], 'called')


def test_buscar_motoristas_por_id_inexistente(client_app2, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self): return None
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor

    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app2.get(
        '/motoristas/999',
        headers=auth_header
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_adicionar_motorista_sucesso(client_app2, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            lastrowid = 1
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor

    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app2.post(
        '/motoristas',
        headers=auth_header,
        json={
            'nome': 'Carlos',
            'valor_passagem': 2.50,
            'status': 'ativo'
        }
    )

    assert resp.status_code in (200, 201)
    assert 'mensagem' in resp.json or 'id' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_adicionar_motorista_sem_json(client_app2, auth_header):
    resp = client_app2.post(
        '/motoristas',
        headers=auth_header
    )

    assert resp.status_code == 400
    assert 'erro' in resp.json


def test_atualizar_motorista_sucesso(client_app2, auth_header, monkeypatch):
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

    resp = client_app2.put(
        '/motoristas/1',
        headers=auth_header,
        json={
            'nome': 'Carlos atualizado',
            'valor_passagem': 3.50,
            'status': 'ativo'
        }
    )

    assert resp.status_code == 200
    assert 'mensagem' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_atualizar_motorista_inexistente(client_app2, auth_header, monkeypatch):
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

    resp = client_app2.put(
        '/motoristas/999',
        headers=auth_header,
        json={
            'nome': 'X',
            'valor_passagem': 3.50,
            'status': 'ativo'
        }
    )
    
    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_deletar_motorista_sucesso(client_app2, auth_header, monkeypatch):
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

    resp = client_app2.delete(
        '/motoristas/1',
        headers=auth_header
    )

    assert resp.status_code in (200, 204)
    if resp.status_code == 200:
        assert 'mensagem' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_deletar_motorista_inexistente(client_app2, auth_header, monkeypatch):
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

    resp = client_app2.delete(
        '/motoristas/999',
        headers=auth_header
    )

    assert resp.status_code == 404
    assert hasattr(cursor_holder['cursor'], 'called')
