from contextlib import contextmanager, closing
from app1.error import tratamento_erro_mysql
import mysql.connector


@contextmanager
def criar_banco_fake():
    with closing(mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password=''
    )) as con:
        try:
            cursor = con.cursor()
            yield cursor
            con.commit()
        except Exception as erro:
            con.rollback()
            tratamento_erro_mysql(erro)
            raise


def init_test_db():
    with criar_banco_fake() as cursor:
        cursor.execute('''
            CREATE DATABASE IF NOT EXISTS test
                DEFAULT CHARSET utf8mb4
                DEFAULT COLLATE utf8mb4_unicode_ci;
        ''')


@contextmanager
def fake_conexao():
    with closing(mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='',
        autocommit=False,
        database='test'
    )) as con:
        try:
            cursor = con.cursor()
            yield cursor
            con.commit()
        except Exception as erro:
            con.rollback()
            tratamento_erro_mysql(erro)
            raise


def criar_tabelas():
    with fake_conexao() as cursor:
        cursor.execute('DELETE FROM refresh_tokens')
        cursor.execute('DELETE FROM usuarios')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS usuarios (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                usuario VARCHAR(100) NOT NULL UNIQUE,
                senha_hash VARCHAR(255) NOT NULL,
                criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_usuario_u (usuario)
        ) ENGINE=InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                user_id INT UNSIGNED NOT NULL,
                token_hash CHAR(64) NOT NULL UNIQUE,
                expires_at DATETIME NOT NULL,
                revoked BOOLEAN DEFAULT FALSE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_refresh_user_id
                    FOREIGN KEY (user_id)
                    REFERENCES usuarios(id)
                    ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')
