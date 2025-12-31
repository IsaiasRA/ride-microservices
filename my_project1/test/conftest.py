import pytest
from test.test_database import init_test_db, criar_tabelas, fake_conexao
from main import app1


@pytest.fixture(scope='session', autouse=True)
def setup_test_db():
    init_test_db()
    criar_tabelas()


@pytest.fixture
def client():
    app1.config['TESTING'] = True
    with app1.test_client() as client:
        yield client


@pytest.fixture
def db_conexao():
    return fake_conexao
