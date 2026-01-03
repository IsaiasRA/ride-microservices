import bcrypt
import hashlib

 

def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def salvar_refresh(cursor, user_id, refresh_token, expira_at):
    cursor.execute('''
        INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
            VALUES (%s, %s, %s)''',
            (user_id, hash_token(refresh_token), expira_at)
    )
    

def refresh_valido(cursor, refresh_token):
    cursor.execute('''
        SELECT id, user_id FROM refresh_tokens
            WHERE token_hash = %s
            AND revoked = FALSE
            AND expires_at > NOW()''',
            (hash_token(refresh_token),)
    )
    
    return cursor.fetchone()


def revogar_refresh(cursor, refresh_token):
    cursor.execute('''
        UPDATE refresh_tokens SET
            revoked = TRUE
            WHERE token_hash = %s''',
            (hash_token(refresh_token),)
    )


def revogar_todos_refresh(cursor, user_id):
    cursor.execute('''
        UPDATE refresh_tokens SET
            revoked = TRUE
            WHERE user_id = %s''',
            (user_id,)
    )


def criar_usuario(cursor, usuario, senha):
    senha_hash = bcrypt.hashpw(
        senha.encode(),
        bcrypt.gensalt()
    ).decode()

    cursor.execute('''
        INSERT INTO usuarios (usuario, senha_hash)
            VALUES (%s, %s)''',
            (usuario, senha_hash)
    )
