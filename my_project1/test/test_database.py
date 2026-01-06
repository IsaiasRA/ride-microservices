from contextlib import contextmanager, closing
from app.error import tratamento_erro_mysql
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
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS usuarios (
                    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                    usuario VARCHAR(100) NOT NULL UNIQUE,
                    senha_hash VARCHAR(255) NOT NULL,
                    criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                    
                    INDEX idx_usuario_u (usuario)
                ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
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
                    ON UPDATE RESTRICT,

                    INDEX idx_refresh_user_id (user_id),
                    INDEX idx_refresh_token (token_hash),
                    INDEX idx_refresh_expires (expires_at)
                ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
            ''')


        cursor.execute('''
                CREATE TABLE IF NOT EXISTS passageiros (
                    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                    nome VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(nome)) > 0),
                    cpf CHAR(11) NOT NULL UNIQUE CHECK(LENGTH(TRIM(cpf)) = 11),
                    telefone VARCHAR(20) NOT NULL UNIQUE CHECK(LENGTH(TRIM(telefone)) >= 8),
                    saldo DECIMAL(10, 2) DEFAULT 0 CHECK(saldo >= 0),
                    endereco_rua VARCHAR(100) NOT NULL,
                    endereco_numero VARCHAR(10) NOT NULL,
                    endereco_bairro VARCHAR(50) NOT NULL,
                    endereco_cidade VARCHAR(50) NOT NULL,
                    endereco_estado CHAR(2) NOT NULL CHECK(LENGTH(TRIM(endereco_estado)) = 2),
                    endereco_cep VARCHAR(10) NOT NULL CHECK(LENGTH(TRIM(endereco_cep)) >= 8),
                    km DECIMAL(6, 2) NOT NULL CHECK(km > 0),
                    metodo_pagamento ENUM(
                    'pix', 'credito', 'debito', 'boleto') NOT NULL,
                    criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                    atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE = InnoDB DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
            ''')

        cursor.execute(
            "SHOW INDEX FROM passageiros WHERE Key_name = 'idx_passa_nome'")
        if not cursor.fetchone():
            cursor.execute(
                'CREATE INDEX idx_passa_nome ON passageiros(nome);')


        cursor.execute('''
                CREATE TABLE IF NOT EXISTS motoristas (
                    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                    nome VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(nome)) > 0),
                    cnh CHAR(11) NOT NULL UNIQUE CHECK(LENGTH(TRIM(cnh)) = 11),
                    telefone VARCHAR(20) NOT NULL UNIQUE CHECK(LENGTH(TRIM(telefone)) >= 8),
                    categoria_cnh ENUM('A', 'B', 'C', 'D', 'E') NOT NULL,
                    placa CHAR(7) NOT NULL UNIQUE CHECK(LENGTH(TRIM(placa)) = 7),
                    modelo_carro VARCHAR(50) NOT NULL,
                    ano_carro INT UNSIGNED NOT NULL CHECK(ano_carro >= 1980),
                    status ENUM('ativo', 'suspenso', 'bloqueado') DEFAULT 'ativo',
                    valor_passagem DECIMAL(5, 2) UNSIGNED NOT NULL,
                    quantia DECIMAL(10, 2) UNSIGNED DEFAULT 0 CHECK(quantia >= 0),
                    criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                    atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
            ''')

        cursor.execute(
            "SHOW INDEX FROM motoristas WHERE Key_name = 'idx_moto_nome'")
        if not cursor.fetchone():
            cursor.execute('CREATE INDEX idx_moto_nome ON motoristas(nome)')

        cursor.execute(
            "SHOW INDEX FROM motoristas WHERE Key_name = 'idx_moto_ano_carro'")
        if not cursor.fetchone():
            cursor.execute(
                'CREATE INDEX idx_moto_ano_carro ON motoristas(ano_carro)')


        cursor.execute('''
                CREATE TABLE IF NOT EXISTS viagens (
                    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                    id_passageiro INT UNSIGNED NOT NULL,
                    id_motorista INT UNSIGNED NOT NULL,
                    nome_passageiro VARCHAR(100) NOT NULL,
                        CHECK(LENGTH(TRIM(nome_passageiro)) > 0),
                    nome_motorista VARCHAR(100) NOT NULL
                        CHECK(LENGTH(TRIM(nome_motorista)) > 0),
                    endereco_rua VARCHAR(100) NOT NULL,
                    endereco_numero VARCHAR(10) NOT NULL,
                    endereco_bairro VARCHAR(50) NOT NULL,
                    endereco_cidade VARCHAR(50) NOT NULL,
                    endereco_estado CHAR(2) NOT NULL
                        CHECK(LENGTH(TRIM(endereco_estado)) = 2),
                    endereco_cep VARCHAR(10) NOT NULL
                        CHECK(LENGTH(TRIM(endereco_cep)) >= 8),
                    valor_por_km DECIMAL(5, 2) NOT NULL CHECK(valor_por_km > 0),
                    total_viagem DECIMAL(10, 2) NOT NULL CHECK(total_viagem > 0),
                    metodo_pagamento ENUM(
                    'pix', 'credito', 'debito', 'boleto') NOT NULL,
                    status ENUM('confirmada', 'cancelada') DEFAULT 'confirmada',
                    criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                    atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT fk_viagens_passageiro
                    FOREIGN KEY (id_passageiro)
                    REFERENCES passageiros(id)
                    ON DELETE RESTRICT ON UPDATE RESTRICT,

                    CONSTRAINT fk_viagens_motoristas
                        FOREIGN KEY (id_motorista)
                        REFERENCES motoristas(id)
                        ON DELETE RESTRICT ON UPDATE RESTRICT,

                    INDEX idx_viagens_passageiro (id_passageiro),
                    INDEX idx_viagens_motorista (id_motorista)
                ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_unicode_ci;
            ''')

        cursor.execute(
            "SHOW INDEX FROM viagens WHERE Key_name = 'idx_viagens_nome_passa'")
        if not cursor.fetchone():
            cursor.execute(
                'CREATE INDEX idx_viagens_nome_passa ON viagens(nome_passageiro)')

        cursor.execute(
            "SHOW INDEX FROM viagens WHERE Key_name = 'idx_viagens_nome_moto'")
        if not cursor.fetchone():
            cursor.execute(
                'CREATE INDEX idx_viagens_nome_moto ON viagens(nome_motorista)')


        cursor.execute('''
                CREATE TABLE IF NOT EXISTS registros_pagamento (
                    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                    id_viagem INT UNSIGNED,
                    remetente VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(remetente)) > 0),
                    recebedor VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(recebedor)) > 0),
                    metodo_pagamento ENUM('pix', 'credito', 'debito', 'boleto') NOT NULL,
                    pagamento ENUM('pago', 'cancelado', 'pendente') DEFAULT 'pago',
                    status ENUM('concluido', 'cancelado') DEFAULT 'concluido',
                    valor_viagem DECIMAL(10, 2) NOT NULL CHECK(valor_viagem > 0),
                    criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                    atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                    CONSTRAINT fk_registro_viagem
                    FOREIGN KEY (id_viagem)
                    REFERENCES viagens(id)
                    ON DELETE SET NULL ON UPDATE RESTRICT,
                    INDEX idx_registro_viagem (id_viagem)
                ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
            ''')

        cursor.execute(
            "SHOW INDEX FROM registros_pagamento WHERE Key_name = 'idx_registro_remetente'")
        if not cursor.fetchone():
            cursor.execute(
                'CREATE INDEX idx_registro_remetente ON registros_pagamento(remetente)')

        cursor.execute(
            "SHOW INDEX FROM registros_pagamento WHERE Key_name = 'idx_registro_recebedor'")
        if not cursor.fetchone():
            cursor.execute(
                'CREATE INDEX idx_registro_recebedor ON registros_pagamento(recebedor)')
