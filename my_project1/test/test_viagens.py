from unittest.mock import Mock
from decimal import Decimal


def test_viagens_sem_token(client_app3):
    resp = client_app3.get(
        '/viagens'
    )
    assert resp.status_code == 401


def test_listar_viagens(client_app3, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchall(self):
                return [(1, 1, 1, 'João', 10, 'pix', 'pago',
                         'confirmada', 'Carlos', 2.50, 'now', 'now')]
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app3.get(
        '/viagens',
         headers=auth_header
    )

    assert resp.status_code == 200
    assert len(resp.json) == 1
    assert hasattr(cursor_holder['cursor'], 'called')


def test_buscar_viagens_por_id_sucesso(client_app3, auth_header, monkeypatch):
    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self):
                return (1, 1, 1, 'João', 10, 'pix', 'pago',
                         'confirmada', 'Carlos', 2.50, 'now', 'now')
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app3.get(
        '/viagens/1',
        headers=auth_header
    )

    assert resp.status_code == 200
    assert resp.json['id'] == 1
    assert hasattr(cursor_holder['cursor'], 'called')


def test_buscar_viagens_por_id_inexistente(client_app3, auth_header, monkeypatch):
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

    resp = client_app3.get(
        '/viagens/999',
        headers=auth_header
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')
    

def test_adicionar_viagem_sucesso(client_app3, auth_header, monkeypatch):
    fake_pass = Mock()
    fake_pass.json.return_value = {
        'nome': 'João',
        'total_viagem': 20,
        'metodo_pagamento': 'pix',
        'pagamento': 'pago',
        'status': 'confirmada'

    }

    fake_pass.raise_for_status = lambda: None

    fake_moto = Mock()
    fake_moto.json.return_value = {
        'nome': 'Carlos',
        'valor_total': 2.50,
        'criado_em': 'now',
        'atualizado_em': 'now'
    }
    fake_moto.raise_for_status = lambda: None

    monkeypatch.setattr(
        'app1.main.requests.get',
        lambda url, **k: fake_pass if 'passageiros' in url else fake_moto
    )

    cursor_holder = {}
    def fake_conexao():
        class Cursor:
            def execute(self, *args):
                self.called = True
            def fetchone(self): return (Decimal('100'),)
            lastrowid = 1
        cursor = Cursor()
        cursor_holder['cursor'] = cursor
        yield cursor
    
    monkeypatch.setattr('app1.main.conexao', fake_conexao)

    resp = client_app3.post(
        '/viagens',
        headers=auth_header,
        json={
            'id_passageiro': 1,
            'id_motorista': 1
        }
    )

    assert resp.status_code == 201
    assert 'mensagem' in resp.json or 'id' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_atualizar_viagem_sucesso(client_app3, auth_header, monkeypatch):
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

    resp = client_app3.put(
        '/viagens/1',
        headers=auth_header,
        json={
            'status': 'cancelada'
        }
    )

    assert resp.status_code == 200
    assert 'mensagem' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_atualizar_viagem_inexistente(client_app3, auth_header, monkeypatch):
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

    resp = client_app3.put(
        '/viagens/999',
        headers=auth_header,
        json={
            'status': 'cancelada'
        }
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_deletar_viagem_sucesso(client_app3, auth_header, monkeypatch):
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

    resp = client_app3.delete(
        '/viagens/1',
        headers=auth_header
    )

    assert resp.status_code in (200, 204)
    if resp.status_code == 200:
        assert 'mensagem' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')


def test_deletar_viagem_inexistente(client_app3, auth_header, monkeypatch):
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

    resp = client_app3.delete(
        '/viagens/999',
        headers=auth_header
    )

    assert resp.status_code == 404
    assert 'erro' in resp.json
    assert hasattr(cursor_holder['cursor'], 'called')
