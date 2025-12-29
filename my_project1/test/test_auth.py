
def test_register_sucesso(client_app1):
    resp = client_app1.post(
        '/register/1',
        json={
            'usuario': 'Isaias',
            'senha': '2006I$aia$110'
        }
    )

    assert resp.status_code in (200, 201)
    assert 'mensagem' in resp.json or 'id' in resp.json


def test_register_sem_json(client_app1):
    resp = client_app1.post('/register/1')

    assert resp.status_code == 400
    assert 'erro' in resp.json


def test_login_sucesso(client_app1):
    resp = client_app1.post(
        '/login/1',
        json={
            'usuario': 'Isaias',
            'senha': '2006I$aia$110'
        }
    )

    assert resp.status_code == 200
    assert 'access' in resp.json or 'access_token' in resp.json
    assert resp.headers.get('Set-Cookie')


def test_login_credenciais_invalidas(client_app1):
    resp = client_app1.post(
        '/login/1',
        json={
            'usuario': 'Isaias',
            'senha': 'senha_errada'
        }
    )

    assert resp.status_code == 401
    assert 'erro' in resp.json


def test_refresh_sem_cookie(client_app1):
    resp = client_app1.post('/refresh')

    assert resp.status_code == 401
    assert 'erro' in resp.json


def test_refresh_com_cookie(client_app1):
    login = client_app1.post(
        '/login/1',
        json={
            'usuario': 'Isaias',
            'senha': '2006I$aia$110'
        }
    )
    cookies = login.headers.getlist('Set-Cookie')

    resp = client_app1.post(
        '/refresh',
        headers={'Cookie': '; '.join(cookies)}
    )

    assert resp.status_code == 200
    assert 'access' in resp.json or 'access_token' in resp.json


def test_logout_sucesso(client_app1):
    login = client_app1.post(
        '/login/1',
        json={
            'usuario': 'Isaias',
            'senha': '2006I$aia$110'
        }
    )

    cookies = login.headers.getlist('Set-Cookie')

    resp = client_app1.post(
        '/logout',
        headers={'Cookie': '; '.join(cookies)}
    )

    assert resp.status_code == 200
    assert 'mensagem' in resp.json

    set_cookie = resp.headers.get('Set-Cookie')
    assert set_cookie is not None
    assert 'expires=' in set_cookie.lower() or 'max-age=0' in set_cookie.lower()
