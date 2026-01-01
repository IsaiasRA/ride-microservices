from datetime import datetime, timedelta, timezone
from app1.log import configurar_logging
from flask import jsonify, request, g
from functools import wraps
import jwt
import hashlib
import bcrypt
import logging



configurar_logging()
logger = logging.getLogger(__name__)


SECRET_KEY = "20060305_i$aia$10"
ALGORITHM = "HS256"
ACCESS_EXPIRES_MIN = 30
REFRESH_EXPIRES_DAYS = 7


br = timezone(timedelta(hours=-3))


def gerar_tokens(id_usuario: str) -> str:
    agora = datetime.now(br)
    try:
        access_payload = {
            'sub': str(id_usuario),
            'type': 'access',
            'iat': int(agora.timestamp()),
            'exp': int((agora + timedelta(minutes=ACCESS_EXPIRES_MIN)).timestamp())
        }

        refresh_payload = {
            'sub': str(id_usuario),
            'type': 'refresh',
            'iat': int(agora.timestamp()),
            'exp': int((agora + timedelta(days=REFRESH_EXPIRES_DAYS)).timestamp())
        }

        acess_token = jwt.encode(access_payload, SECRET_KEY, algorithm=ALGORITHM)
        refresh_token = jwt.encode(refresh_payload, SECRET_KEY, algorithm=ALGORITHM)

        logger.info('Access e Refresh tokens gerados com sucesso.')
        return {
            'access_token': acess_token,
            'refresh_token': refresh_token,
            'refresh_exp': agora + timedelta(days=REFRESH_EXPIRES_DAYS)
        }, 200
    
    except jwt.InvalidKeyError:
        logger.error(f'Chave SECRET_KEY inválida ao gerar token.')
        return {'erro': 'Chave SECRET_KEY inválida!'}, 500
    
    except jwt.InvalidAlgorithmError:
        logger.error(f'Algoritmo JWT inválido ao gerar token.')
        return {'erro': 'Algoritmo JWT inválido ao gerar token!'}, 500
    
    except jwt.PyJWTError as erro:
        logger.error(f'Erro inesperado ao gerar token: {str(erro)}')
        return {'erro': 'Erro inesperado ao gerar token!'}, 500
    

def validar_token(token: str, token_type: str = 'access'):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=ALGORITHM)

        if payload.get('type') != token_type:
            logger.warning('Tipo de token inválido.')
            return {'erro': 'Tipo de token inváldo!'}, 401

        return payload, 200
    
    except jwt.ExpiredSignatureError:
        logger.warning('Token expirou.')
        return {'erro': 'Token expirou!'}, 401
    
    except jwt.InvalidIssuedAtError:
        logger.warning("Campo 'iat' inválido no token.")
        return {'erro': "Campo 'iat' inválido no token!"}, 401
    
    except jwt.InvalidAlgorithmError:
        logger.warning('Algoritmo JWT inválido no token.')
        return {'erro': 'Algoritmo JWT inválido no token!'}, 401
    
    except jwt.InvalidSignatureError:
        logger.warning('Assinatura inválido no token.')
        return {'erro': 'Assinatura inválida no token!'}, 401
    
    except jwt.DecodeError:
        logger.warning('Token malformado.')
        return {'erro': 'Token malformado!'}, 401
    
    except jwt.InvalidTokenError:
        logger.warning('Token inválido.')
        return {'erro': 'Token inválido!'}, 401
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao válidar token: {str(erro)}')
        return {'erro': 'Erro inesperado ao válidar token!'}, 500
    

def rota_protegida(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        auth = request.headers.get('Authorization')

        if not auth:
            logger.warning('Token não enviado.')
            return jsonify({'erro': 'Token não enviado!'}), 401
        
        partes = auth.split()

        if len(partes) != 2 or partes[0].lower() != 'bearer':
            logger.warning('JSON malformado. Use Bearer <token>')
            return jsonify({'erro': 'JSON malformado! Use Bearer <token>'}), 401
        
        payload, status = validar_token(partes[1], token_type='access')

        if status != 200:
            return jsonify(payload), status
        
        g.id_usuario = payload.get('sub')
        return func(*args, **kwargs)
    return wrapper
 

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
    