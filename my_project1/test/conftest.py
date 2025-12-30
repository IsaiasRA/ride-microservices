import pytest
from unittest.mock import patch
from main import (app1, app2,
                        app3, app4)
from test.test_database import fake_conexao



@pytest.fixture
def client_app1():
    return app1.test_client()


@pytest.fixture
def client_app2():
    return app2.test_client()


@pytest.fixture
def client_app3():
    return app3.test_client()


@pytest.fixture
def client_app4():
    return app4.test_client()


@pytest.fixture
def auth_header():
    return {'Authorization': 'Bearer fake_token'}


@pytest.fixture(autouse=True)
def mock_db(monkeypatch):
    monkeypatch.setattr(
        'main.conexao',
        fake_conexao
    )


@pytest.fixture(autouse=True)
def mock_jwt():
    with patch('main.validar_token') as mock:
        mock.return_value = (
            {'sub': 1,
            'type': 'access',
            'iat': 123,
            'exp': 999999
            }, 200
        )
        yield
