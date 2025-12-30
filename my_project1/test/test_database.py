from contextlib import contextmanager, closing
from app1.error import tratamento_erro_mysql
import sqlite3


@contextmanager
def fake_conexao():
    with closing(sqlite3.connect('banco2.db')) as con:
        cursor = con.cursor()
        try:
            yield cursor
            con.commit()
        except Exception as erro:
            con.rollback()
            tratamento_erro_mysql(erro)
            raise


with fake_conexao() as cursor:
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS passageiros (
                id INT AUTOINCREMENT PRIMARY KEY,
                nome TEXT NOT NULL,
                idade INT NOT NULL,
                sexo TEXT NOT NULL CHECK(sexo IN ('M', 'F'))
                );
            ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS motoristas (
                   id INT AUTOINCREMENT PRIMARY KEY,
                   nome TEXT NOT NULL,
                   idade INT NOT NULL,
                   sexo INT NOT NULL,
                   status TEXT DEFAULT 'ativo'
                   );
                ''')
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS viagens (
                id INT AUTOINCREMENT PRIMARY KEY,
                id_passageiro INT NOT NULL,
                id_motorista INT NOT NULL,
                nome_passageiro TEXT NOT NULL,
                nome_motorista TEXT NOT NULL,
                
                   
                )''')