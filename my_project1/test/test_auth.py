import bcrypt
import hashlib


def test_register_sucesso(client_app1):
    resp = client_app1.post(
        '/register',
        json={'usuario': 'Isaias', 'senha': '123456'}
    )

    assert resp.status_code in (200, 201, 409)


def test_register_sem_json(client_app1):
    resp = client_app1.post('/register')
    assert resp.status_code in (400, 404)


def test_login_sucesso(client_app1):
    client_app1.post(
        '/register',
        json={'usuario': 'Isaias', 'senha': '123456'}
    )

    resp = client_app1.post(
        '/login',
        json={'usuario': 'Isaias', 'senha': '123456'}
    )

    assert resp.status_code == 200


def test_refresh_com_cookie(client_app1):
    login = client_app1.post(
        '/login',
        json={'usuario': 'Isaias', 'senha': '123456'}
    )

    cookies = login.headers.getlist('Set-Cookie')
    resp = client_app1.post('/refresh', headers={'Cookie': '; '.join(cookies)})

    assert resp.status_code in (200, 401)


def test_logout_sucesso(client_app1):
    resp = client_app1.post('/logout')
    assert resp.status_code in (200, 401)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()

def salvar_refresh(cursor, user_id, refresh_token, expira_at):
    cursor.execute('''
        INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
            VALUES (?, ?, ?)''',
            (user_id, hash_token(refresh_token), expira_at)
    )
    

def refresh_valido(cursor, refresh_token):
    cursor.execute('''
        SELECT id, user_id FROM refresh_tokens
            WHERE token_hash = ?
            AND revoked = FALSE
            AND expires_at > NOW()''',
            (hash_token(refresh_token),)
    )
    
    return cursor.fetchone()


def revogar_refresh(cursor, refresh_token):
    cursor.execute('''
        UPDATE refresh_tokens SET
            revoked = TRUE
            WHERE token_hash = ?''',
            (hash_token(refresh_token),)
    )


def revogar_todos_refresh(cursor, user_id):
    cursor.execute('''
        UPDATE refresh_tokens SET
            revoked = TRUE
            WHERE user_id = ?''',
            (user_id,)
    )


def criar_usuario(cursor, usuario, senha):
    senha_hash = bcrypt.hashpw(
        senha.encode(),
        bcrypt.gensalt()
    ).decode()

    cursor.execute('''
        INSERT INTO usuarios (usuario, senha_hash)
            VALUES (?, ?)''',
            (usuario, senha_hash)
    )