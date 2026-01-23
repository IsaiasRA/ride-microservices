from flask import Flask, jsonify, request
from contextlib import closing, contextmanager
from werkzeug.exceptions import (BadRequest,
                                  NotFound,
                                  Conflict)
from app.error import (tratamento_erro_mysql,
                         register_erro_handlers)
from app.log import configurar_logging
from app.validation import validar_json, formatar_nome
from app.auth import (gerar_tokens,
                        rota_protegida,
                          validar_token)
from app.refresh_tokens import (criar_usuario,
                                  refresh_valido,
                                   salvar_refresh,
                                    revogar_refresh,
                                     revogar_todos_refresh)
from app.brute_force import (ip_bloqueado,
                               registrar_falha,
                                 limpar_falhas,
                                  limiter)
from app.gerador_qr_code import gerar_qr_pix
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta
import threading
import logging
import bcrypt
import re
import mysql.connector
import string
import random
import uuid


configurar_logging()
logger = logging.getLogger(__name__)


app1 = Flask('API1')


limiter.init_app(app1)


@contextmanager
def criar_banco():
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


@contextmanager
def conexao():
    with closing(mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='',
        autocommit=False,
        database='meubanco',
        pool_name='mypool',
        pool_size=5
    )) as con:
        try:
            cursor = con.cursor(dictionary=False)
            yield cursor
            con.commit()
        except Exception as erro:
            con.rollback()
            tratamento_erro_mysql(erro)
            raise


with criar_banco() as cursor:
    cursor.execute('''
            CREATE DATABASE IF NOT EXISTS meubanco
                DEFAULT CHARSET utf8mb4
                DEFAULT COLLATE utf8mb4_unicode_ci;''')
    

with conexao() as cursor:
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS usuarios (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                usuario VARCHAR(100) NOT NULL UNIQUE,
                senha_hash VARCHAR(255) NOT NULL,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
        ''')
    
    cursor.execute("SHOW INDEX FROM usuarios WHERE Key_name = 'idx_usuario_u'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_usuario_u ON usuarios(usuario)')
    
    cursor.execute("SHOW INDEX FROM usuarios WHERE Key_name = 'idx_usuario_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_usuario_data ON usuarios(criado_em)')
    
    
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                id_usuario INT UNSIGNED NOT NULL,
                hash_token CHAR(64) NOT NULL,
                expira_em DATETIME NOT NULL,
                revogado BOOLEAN DEFAULT FALSE,
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_refresh_usuario
                   FOREIGN KEY (id_usuario)
                   REFERENCES usuarios(id)
                   ON DELETE CASCADE
                   ON UPDATE RESTRICT,
                
                UNIQUE KEY uk_refresh_token (hash_token)
            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
        ''')
    
    cursor.execute(
        "SHOW INDEX FROM refresh_tokens WHERE Key_name = 'idx_refresh_usuario'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_refresh_usuario ON refresh_tokens(id_usuario)')

    cursor.execute(
        "SHOW INDEX FROM refresh_tokens WHERE Key_name = 'idx_refresh_token'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_refresh_token ON refresh_tokens(hash_token)')
    
    cursor.execute(
        "SHOW INDEX FROM refresh_tokens WHERE Key_name = 'idx_refresh_expira'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_refresh_expira ON refresh_tokens(expira_em)')

    cursor.execute(
        "SHOW INDEX FROM refresh_tokens WHERE Key_name = 'idx_refresh_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_refresh_data ON refresh_tokens(criado_em)')


    cursor.execute('''
            CREATE TABLE IF NOT EXISTS passageiros (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                nome VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(nome)) > 0),
                cpf CHAR(11) NOT NULL CHECK(LENGTH(TRIM(cpf)) = 11),
                telefone VARCHAR(20) NOT NULL CHECK(LENGTH(TRIM(telefone)) >= 8),
                status ENUM('ativo', 'bloqueado') NOT NULL DEFAULT 'ativo',
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,
                
                UNIQUE KEY uk_passa_cpf (cpf),
                UNIQUE KEY uk_passa_telefone (telefone)
            ) ENGINE = InnoDB DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
        ''')
    
    cursor.execute(
        "SHOW INDEX FROM passageiros WHERE Key_name = 'idx_passa_nome'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_passa_nome ON passageiros(nome);')
        
    cursor.execute("SHOW INDEX FROM passageiros WHERE Key_name = 'idx_passa_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_passa_data ON passageiros(criado_em)')

    cursor.execute("SHOW INDEX FROM passageiros WHERE Key_name = 'idx_passa_status_data'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_passa_status_data ON passageiros(status, criado_em)')

    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS contas_bancarias (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            banco_codigo CHAR(3) NOT NULL,
            banco_nome VARCHAR(100) NOT NULL,
            agencia VARCHAR(10) NOT NULL,
            agencia_digito CHAR(1) NULL,
            conta VARCHAR(20) NOT NULL,
            conta_digito CHAR(1) NULL,
            tipo_conta ENUM('corrente', 'poupanca',
                'salario', 'pagamento') NOT NULL,
            titular_nome VARCHAR(150) NOT NULL,
            titular_documento VARCHAR(20) NOT NULL,
            status ENUM('ativa', 'bloqueada', 'encerrada') NOT NULL DEFAULT 'ativa',
            principal BOOLEAN DEFAULT FALSE,
            principal_ativa BOOLEAN
                GENERATED ALWAYS AS (
                        CASE
                            WHEN principal = TRUE AND status = 'ativa' THEN TRUE
                            ELSE NULL
                        END
                ) STORED,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            
            UNIQUE KEY uk_conta (banco_codigo, agencia, conta,
                    conta_digito, titular_documento),
            
            CONSTRAINT fk_conta_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT ON UPDATE CASCADE
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM contas_bancarias WHERE Key_name = 'uq_principal_ativa'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE UNIQUE INDEX uq_principal_ativa' \
            ' ON contas_bancarias(id_passageiro, principal_ativa)')

    cursor.execute(
        "SHOW INDEX FROM contas_bancarias WHERE Key_name = 'idx_conta_passageiro'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_conta_passageiro ON contas_bancarias(id_passageiro)')
    
    cursor.execute(
        "SHOW INDEX FROM contas_bancarias WHERE Key_name = 'idx_conta_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_conta_data ON contas_bancarias(criado_em)')

    cursor.execute(
        "SHOW INDEX FROM contas_bancarias WHERE Key_name = 'idx_conta_banco'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_conta_banco ON contas_bancarias(banco_codigo, agencia)')
    
    cursor.execute(
        "SHOW INDEX FROM contas_bancarias WHERE Key_name = 'idx_conta_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_conta_passa_status ON contas_bancarias(id_passageiro, status)')
    

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS contas_plataforma (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            id_conta_bancaria INT UNSIGNED NOT NULL,
            tipo ENUM(
                   'recebimento',
                   'repasse',
                   'comissao',
                   'custodia',
                   'taxas'
                   ) NOT NULL,
            saldo_atual DECIMAL(14, 2) NOT NULL DEFAULT 0.00 CHECK(saldo_atual >= 0),
            saldo_disponivel DECIMAL(14, 2) NOT NULL
                    DEFAULT 0.00 CHECK(saldo_disponivel >= 0),
            saldo_bloqueado DECIMAL(14, 2) NOT NULL
                    DEFAULT 0.00 CHECK(saldo_bloqueado >= 0),
            status ENUM('ativa', 'bloqueada', 'encerrada') NOT NULL DEFAULT 'ativa',
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_plataforma_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT UPDATE CASCADE,
            
            CONSTRAINT fk_plataforma_conta_bancaria
                FOREIGN KEY (id_conta_bancaria)
                REFERENCES contas_bancarias(id)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            
            UNIQUE KEY uk_plataforma_tipo_passageiro (id_passageiro, tipo)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM contas_plataforma WHERE Key_name = 'idx_plataforma_passa'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_plataforma_passa' \
            ' ON contas_plataforma(id_passageiro)')

    cursor.execute(
        "SHOW INDEX FROM contas_plataforma WHERE Key_name = 'idx_plataforma_conta'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_plataforma_conta' \
            ' ON contas_plataforma(id_conta_bancaria)')
        
    cursor.execute(
        "SHOW INDEX FROM contas_plataforma WHERE Key_name = 'idx_plataforma_tipo'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_plataforma_tipo ON contas_plataforma(tipo)')

    cursor.execute(
        "SHOW INDEX FROM contas_plataforma WHERE Key_name = 'idx_plataforma_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_plataforma_status ON contas_plataforma(status)')
    
    cursor.execute(
        "SHOW INDEX FROM contas_plataforma WHERE Key_name = 'idx_plataforma_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_plataforma_data ON contas_plataforma(criado_em)')

    cursor.execute(
        "SHOW INDEX FROM contas_plataforma WHERE Key_name = 'idx_plataforma_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_plataforma_passa_status' \
            ' ON contas_plataforma(id_passageiro, status)')
    
    cursor.execute(
        "SHOW INDEX FROM contas_plataforma WHERE Key_name = 'idx_plataforma_conta_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_plataforma_conta_status' \
            ' ON contas_plataforma(id_conta_bancaria, status)')
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pagamentos_pix (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            txid VARCHAR(35) NOT NULL,
            idempotency_key VARCHAR(64) NOT NULL,
            valor DECIMAL(14, 2) NOT NULL CHECK(valor > 0),
            qr_code_payload TEXT NOT NULL,
            status ENUM('pendente', 'confirmado', 'cancelado')
                NOT NULL DEFAULT 'pendente',
            liquidado_em DATETIME NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_pix_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            
            CONSTRAINT chk_pix_status_liquidado
                CHECK(
                   (status = 'confirmado' AND liquidado_em IS NOT NULL)
                   OR status <> 'confirmado'),
            
            CONSTRAINT chk_pix_criado_liquidado
                CHECK(liquidado_em IS NULL OR liquidado_em >= criado_em),
            
            UNIQUE KEY uk_pix_txid_passageiro (txid, id_passageiro),
            UNIQUE KEY uk_pix_idempotency_key (idempotency_key)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute("SHOW INDEX FROM pagamentos_pix WHERE Key_name = 'idx_pix_passa'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_pix_passa ON pagamentos_pix(id_passageiro)')

    cursor.execute("SHOW INDEX FROM pagamentos_pix WHERE Key_name = 'idx_pix_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_pix_status ON pagamentos_pix(status)')

    cursor.execute("SHOW INDEX FROM pagamentos_pix WHERE Key_name = 'idx_pix_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_pix_data ON pagamentos_pix(criado_em)')

    cursor.execute(
        "SHOW INDEX FROM pagamentos_pix WHERE Key_name = 'idx_pix_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_pix_passa_status ON pagamentos_pix(id_passageiro, status)')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chaves_pix (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            tipo ENUM('cpf', 'cnpj', 'email', 'telefone', 'aleatoria') NOT NULL,
            chave VARCHAR(140) NOT NULL,
            banco_codigo CHAR(3) NOT NULL,
            status ENUM('ativa', 'removida') NOT NULL DEFAULT 'ativa',
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   
            CONSTRAINT fk_chave_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT ON UPDATE CASCADE,
                   
            UNIQUE KEY uk_chave_pix (chave),
            UNIQUE KEY uk_chave_passageiro_tipo_status (id_passageiro, tipo, status)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;''')
    
    cursor.execute("SHOW INDEX FROM chaves_pix WHERE Key_name = 'idx_chave_passa'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_chave_passa ON chaves_pix(id_passageiro)')

    cursor.execute("SHOW INDEX FROM chaves_pix WHERE Key_name = 'idx_chave_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_chave_status ON chaves_pix(status)')

    cursor.execute("SHOW INDEX FROM chaves_pix WHERE Key_name = 'idx_chave_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_chave_data ON chaves_pix(criado_em)')

    cursor.execute(
        "SHOW INDEX FROM chaves_pix WHERE Key_name = 'idx_chave_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_chave_passa_status ON chaves_pix(id_passageiro, status)')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS configuracoes_pix (
            id TINYINT PRIMARY KEY DEFAULT 1 CHECK(id = 1),
            chave_pix VARCHAR(140) NOT NULL,
            tipo ENUM('cpf', 'cnpj', 'email', 'telefone', 'aleatoria') NOT NULL,
            banco_codigo CHAR(3) NOT NULL,
            nome_recebedor VARCHAR(25) NOT NULL,
            cidade VARCHAR(15) NOT NULL,
            status ENUM('ativa', 'removida') NOT NULL DEFAULT 'ativa',
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM configuracoes_pix WHERE Key_name = 'idx_config_id'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_config_id ON configuracoes_pix(id)')

    cursor.execute(
        "SHOW INDEX FROM configuracoes_pix WHERE Key_name = 'idx_config_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_config_status ON configuracoes_pix(status)')

    cursor.execute(
        "SHOW INDEX FROM configuracoes_pix WHERE Key_name = 'idx_config_id_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_config_id_status ON configuracoes_pix(id, status)')


    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pagamentos_debito (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            autorizacao VARCHAR(64) NOT NULL,
            idempotency_key VARCHAR(64) NOT NULL UNIQUE,
            valor DECIMAL(14, 2) NOT NULL CHECK(valor > 0),
            status ENUM('pendente', 'aprovado', 'negado', 'estornado')
                NOT NULL DEFAULT 'pendente',
            liquidado_em DATETIME NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_debito_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            
            CONSTRAINT chk_debito_status_liquidado
                CHECK((
                   status = 'aprovado' AND liquidado_em IS NOT NULL)
                   OR status <> 'aprovado'),
            
            UNIQUE KEY uk_debito_autorizacao (autorizacao)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM pagamentos_debito WHERE Key_name = 'idx_debito_passa'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_debito_passa ON pagamentos_debito(id_passageiro)')
        
    cursor.execute(
        "SHOW INDEX FROM pagamentos_debito WHERE Key_name = 'idx_debito_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_debito_status ON pagamentos_debito(status)')

    cursor.execute(
        "SHOW INDEX FROM pagamentos_debito WHERE Key_name = 'idx_debito_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_debito_data ON pagamentos_debito(criado_em)')
    
    cursor.execute(
        "SHOW INDEX FROM pagamentos_debito WHERE Key_name = 'idx_debito_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_debito_passa_status' \
            ' ON pagamentos_debito(id_passageiro, status)')
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS credito_passageiro (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            limite_total DECIMAL(14, 2) NOT NULL CHECK(limite_total > 0),
            limite_utilizado DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
            status ENUM('ativo', 'bloqueado') NOT NULL DEFAULT 'ativo',
            idempotency_key VARCHAR(64) NOT NULL UNIQUE,
                   
            CONSTRAINT fk_credito_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT UPDATE CASCADE,
                
            CONSTRAINT chk_credito_utilizado_total
                CHECK(limite_utilizado <= limite_total),
            
            UNIQUE KEY uk_credito_passageiro (id_passageiro)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM credito_passageiro WHERE Key_name = 'idx_credito_passa'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_credito_passa ON credito_passageiro(id_passageiro)')

    cursor.execute(
        "SHOW INDEX FROM credito_passageiro WHERE Key_name = 'idx_credito_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_credito_status ON credito_passageiro(status)')

    cursor.execute(
        "SHOW INDEX FROM credito_passageiro WHERE Key_name = 'idx_credito_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_credito_passa_status ON' \
            ' credito_passageiro(id_passageiro, status)')


    cursor.execute('''
        CREATE TABLE IF NOT EXISTS faturas_credito (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            vencimento DATE NOT NULL,
            valor_total DECIMAL(14, 2) NOT NULL CHECK(valor_total > 0),
            status ENUM('aberta', 'paga', 'fechada', 'atrasada', 'cancelada')
                    NOT NULL DEFAULT 'aberta',
            pago_em DATETIME NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_fatura_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT UPDATE CASCADE,
                   
            CONSTRAINT chk_fatura_status_paga
                CHECK(
                   (status = 'paga' AND pago_em IS NOT NULL)
                   OR status <> 'paga'),
            
            UNIQUE KEY uk_fatura_passa_vencimento (id_passageiro, vencimento)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute("SHOW INDEX FROM faturas_credito WHERE Key_name = 'idx_fatura_passa'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_fatura_passa ON faturas_credito(id_passageiro)')

    cursor.execute(
        "SHOW INDEX FROM faturas_credito WHERE Key_name = 'idx_fatura_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_fatura_status ON faturas_credito(status)')

    cursor.execute("SHOW INDEX FROM faturas_credito WHERE Key_name = 'idx_fatura_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_fatura_data ON faturas_credito(criado_em)')

    cursor.execute(
        "SHOW INDEX FROM faturas_credito WHERE Key_name = 'idx_fatura_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_fatura_passa_status' \
            ' ON faturas_credito(id_passageiro, status)')
        
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS parcelas_credito (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_fatura BIGINT UNSIGNED NOT NULL,
            parcelas TINYINT UNSIGNED NOT NULL CHECK(parcelas BETWEEN 1 AND 12),
            vencimento DATE NULL,
            valor DECIMAL(14, 2) NOT NULL CHECK(valor > 0),
            status ENUM('pendente', 'paga', 'cancelada')
                NOT NULL DEFAULT 'pendente',
            pago_em DATETIME NULL,
            
            CONSTRAINT fk_parcela_fatura
                FOREIGN KEY (id_fatura)
                REFERENCES faturas_credito(id),
                   
            CONSTRAINT chk_parcela_status_paga
                CHECK(
                   (status = 'paga' AND pago_em IS NOT NULL)
                   OR status <> 'paga'),
            
            UNIQUE KEY uk_fatura_parcela (id_fatura, parcelas)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM parcelas_credito WHERE Key_name = 'idx_parcela_fatura'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_parcela_fatura ON parcelas_credito(id_fatura)')

    cursor.execute(
        "SHOW INDEX FROM parcelas_credito WHERE Key_name = 'idx_parcela_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_parcela_status ON parcelas_credito(status)')
    
    cursor.execute(
        "SHOW INDEX FROM parcelas_credito WHERE Key_name = 'idx_parcela_fatura_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_parcela_fatura_status' \
            ' ON parcelas_credito(id_fatura, status)')
        

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pagamentos_credito_transacoes (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            id_fatura BIGINT UNSIGNED NOT NULL,
            valor DECIMAL(14, 2) NOT NULL CHECK(valor > 0),
            parcelas TINYINT UNSIGNED NOT NULL CHECK(parcelas BETWEEN 1 AND 12),
            status ENUM('pendente', 'pago', 'cancelado')
                    NOT NULL DEFAULT 'pendente',
            autorizado_em DATETIME NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_transacoes_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            
            CONSTRAINT fk_transacoes_fatura
                FOREIGN KEY (id_fatura)
                REFERENCES faturas_credito(id)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            
            CONSTRAINT chk_transacoes_status_autorizado
                CHECK(
                   (status = 'pago' AND autorizado_em IS NOT NULL)
                   OR status <> 'pago')
            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
        ''')
    
    cursor.execute(
        "SHOW INDEX FROM pagamentos_credito_transacoes" \
        " WHERE Key_name = 'idx_transacao_passa'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_transacao_passa ON' \
            ' pagamentos_credito_transacoes (id_passageiro)')
        
    cursor.execute(
        "SHOW INDEX FROM pagamentos_credito_transacoes" \
        " WHERE Key_name = 'idx_transacao_fatura'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_transacao_fatura' \
            ' ON pagamentos_credito_transacoes(id_fatura)')
        
    cursor.execute(
        "SHOW INDEX FROM pagamentos_credito_transacoes" \
        " WHERE Key_name = 'idx_transacao_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_transacao_status' \
            ' ON pagamentos_credito_transacoes(status)')
        
    cursor.execute(
        "SHOW INDEX FROM pagamentos_credito_transacoes" \
        " WHERE Key_name = 'idx_transacao_data'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_transacao_data' \
            ' ON pagamentos_credito_transacoes(criado_em)')
        
    cursor.execute(
        "SHOW INDEX FROM pagamentos_credito_transacoes" \
        " WHERE Key_name = 'idx_transacao_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_transacao_passa_status' \
            ' ON pagamentos_credito_transacoes(id_passageiro, status)')
    
    cursor.execute(
        "SHOW INDEX FROM pagamentos_credito_transacoes" \
        " WHERE Key_name = 'idx_transacao_fatura_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_transacao_fatura_status' \
            ' ON pagamentos_credito_transacoes(id_fatura, status)')
        
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pagamentos_boleto (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_passageiro INT UNSIGNED NOT NULL,
            codigo_barras VARCHAR(100) NOT NULL,
            idempotency_key VARCHAR(64) NOT NULL UNIQUE,
            valor DECIMAL(14, 2) NOT NULL CHECK(valor > 0),
            vencimento DATE NOT NULL,
            status ENUM(
                   'emitido',
                   'pago',
                   'vencido',
                   'cancelado'
                   ) NOT NULL,
            pago_em DATETIME NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_boleto_passageiro
                FOREIGN KEY (id_passageiro)
                REFERENCES passageiros(id)
                ON DELETE RESTRICT UPDATE CASCADE,
            
            CONSTRAINT chk_boleto_status_pago
                CHECK(
                   (status = 'pago' AND pago_em IS NOT NULL)
                   OR status <> 'pago'),
            
            CONSTRAINT chk_pago_data
                CHECK(
                   (pago_em IS NULL OR pago_em >= criado_em)
                   ),
            
            UNIQUE KEY uk_boleto_codigo (codigo_barras)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM pagamentos_boleto WHERE Key_name = 'idx_boleto_passa'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_boleto_passa ON pagamentos_boleto(id_passageiro)')
        
    cursor.execute(
        "SHOW INDEX FROM pagamentos_boleto WHERE Key_name = 'idx_boleto_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_boleto_status ON pagamentos_boleto(status)')

    cursor.execute(
        "SHOW INDEX FROM pagamentos_boleto WHERE Key_name = 'idx_boleto_passa_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_boleto_passa_status' \
            ' ON pagamentos_boleto(id_passageiro, status)')


    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movimentacoes_financeiras (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_conta_plataforma INT UNSIGNED NOT NULL,
            tipo ENUM(
                   'credito',
                   'debito',
                   'estorno',
                   'ajuste'
                   ) NOT NULL,
            origem ENUM(
                   'pix',
                   'credito',
                   'debito',
                   'boleto',
                   'manual',
                   'sistema'
                   ) NOT NULL,
            status ENUM('pendente', 'confirmado', 'cancelado')
                   NOT NULL DEFAULT 'pendente',
            valor DECIMAL(14, 2) NOT NULL CHECK(valor > 0),
            impacto_saldo ENUM('disponivel', 'bloqueado') NOT NULL,
            descricao VARCHAR(255) NOT NULL,
            referencia_externa VARCHAR(100) NULL,
            liquidado_em DATETIME NULL,
            data_vencimento DATE NULL,
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_mov_conta
                FOREIGN KEY (id_conta_plataforma)
                REFERENCES contas_plataforma(id)
                ON DELETE RESTRICT ON UPDATE CASCADE,
            
            CONSTRAINT chk_mov_status_liquidacao
                CHECK((status = 'confirmado' AND liquidado_em IS NOT NULL)
                OR status <> 'confirmado'),
            
            CONSTRAINT chk_mov_impacto
                CHECK(
                   (status = 'pendente' AND impacto_saldo = 'bloqueado')
                   OR status <> 'pendente')
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM movimentacoes_financeiras WHERE Key_name = 'idx_mov_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_mov_status ON movimentacoes_financeiras(status)')
        
    cursor.execute(
        "SHOW INDEX FROM movimentacoes_financeiras WHERE Key_name = 'idx_mov_tipo'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_mov_tipo ON movimentacoes_financeiras(tipo)')
        
    cursor.execute(
        "SHOW INDEX FROM movimentacoes_financeiras WHERE Key_name = 'idx_mov_origem'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_mov_origem ON movimentacoes_financeiras(origem)')
        
    cursor.execute(
        "SHOW INDEX FROM movimentacoes_financeiras" \
        " WHERE Key_name = 'idx_mov_referencia_externa'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_mov_referencia_externa' \
            ' ON movimentacoes_financeiras(referencia_externa)')
        
    cursor.execute(
        "SHOW INDEX FROM movimentacoes_financeiras WHERE Key_name = 'idx_mov_tipo_origem'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_mov_tipo_origem ON movimentacoes_financeiras(tipo, origem)')
        
    cursor.execute(
        "SHOW INDEX FROM movimentacoes_financeiras WHERE Key_name = 'idx_mov_conta'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_mov_conta ON movimentacoes_financeiras'\
            '(id_conta_plataforma, criado_em)')
        
    cursor.execute(
        "SHOW INDEX FROM movimentacoes_financeiras" \
        " WHERE Key_name = 'idx_mov_status_vencimento'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_mov_status_vencimento' \
            ' ON movimentacoes_financeiras(status, data_vencimento)')
    
    cursor.execute(
        "SHOW INDEX FROM movimentacoes_financeiras" \
        " WHERE Key_name = 'idx_mov_conta_status_data'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_mov_conta_status_data' \
            ' ON movimentacoes_financeiras(id_conta_plataforma, status, criado_em)')
        
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movimentacoes_referencias (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_movimentacao BIGINT UNSIGNED NOT NULL,
            tipo_origem ENUM('pix', 'credito', 'debito', 'boleto'),
            id_origem BIGINT UNSIGNED NOT NULL,
            
            CONSTRAINT fk_referencias_mov
                FOREIGN KEY (id_movimentacao)
                REFERENCES movimentacoes_financeiras(id)
                ON DELETE RESTRICT ON UPDATE CASCADE,
                   
            UNIQUE KEY uk_mov (id_movimentacao),
            UNIQUE KEY uk_tipo_origem (tipo_origem, id_origem)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
    ''')

    cursor.execute(
        "SHOW INDEX FROM movimentacoes_referencias WHERE Key_name = 'idx_refe_mov'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_refe_mov ON movimentacoes_referencias(id_movimentacao)')
        

    cursor.execute('''
            CREATE TABLE IF NOT EXISTS motoristas (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                nome VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(nome)) > 0),
                cnh CHAR(11) NOT NULL CHECK(LENGTH(TRIM(cnh)) = 11),
                telefone VARCHAR(20) NOT NULL UNIQUE CHECK(LENGTH(TRIM(telefone)) >= 8),
                categoria_cnh ENUM('A', 'B', 'C', 'D', 'E') NOT NULL,
                placa CHAR(7) NOT NULL CHECK(LENGTH(TRIM(placa)) = 7),
                modelo_carro VARCHAR(50) NOT NULL,
                ano_carro INT UNSIGNED NOT NULL CHECK(ano_carro >= 1980),
                status ENUM('ativo', 'bloqueado')NOT NULL DEFAULT 'ativo',
                quantia DECIMAL(10, 2) UNSIGNED DEFAULT 0 CHECK(quantia >= 0),
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,

                UNIQUE KEY uk_moto_cnh (cnh),
                UNIQUE KEY uk_moto_telefone (telefone),
                UNIQUE KEY uk_moto_placa (placa)
            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
        ''')

    cursor.execute(
        "SHOW INDEX FROM motoristas WHERE Key_name = 'idx_moto_nome'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_moto_nome ON motoristas(nome)')

    cursor.execute("SHOW INDEX FROM motoristas WHERE Key_name = 'idx_moto_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_moto_status ON motoristas(status)')

    cursor.execute(
        "SHOW INDEX FROM motoristas WHERE Key_name = 'idx_moto_ano_carro'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_moto_ano_carro ON motoristas(ano_carro)')
        
    cursor.execute("SHOW INDEX FROM motoristas WHERE Key_name = 'idx_moto_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_moto_data ON motoristas(criado_em)')


    cursor.execute('''
            CREATE TABLE IF NOT EXISTS viagens (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                id_passageiro INT UNSIGNED NOT NULL,
                id_motorista INT UNSIGNED NOT NULL,
                km DECIMAL(6, 2) NOT NULL CHECK(km > 0),
                valor_por_km DECIMAL(10, 2) NOT NULL CHECK(valor_por_km > 0),
                total DECIMAL(10, 2) NOT NULL CHECK(total > 0),
                status ENUM('criada', 'em_andamento',
                   'finalizada', 'cancelada') NOT NULL DEFAULT 'criada',
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                   
                CONSTRAINT fk_viagens_passageiro
                   FOREIGN KEY (id_passageiro)
                   REFERENCES passageiros(id)
                   ON DELETE RESTRICT ON UPDATE RESTRICT,

                CONSTRAINT fk_viagens_motoristas
                    FOREIGN KEY (id_motorista)
                    REFERENCES motoristas(id)
                    ON DELETE RESTRICT ON UPDATE RESTRICT
            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_unicode_ci;
        ''')
    
    cursor.execute("SHOW INDEX FROM viagens WHERE Key_name = 'idx_viagens_id_passa'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_viagens_id_passa ON viagens(id_passageiro)')

    cursor.execute("SHOW INDEX FROM viagens WHERE Key_name = 'idx_viagens_id_moto'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_viagens_id_moto ON viagens(id_motorista)')

    cursor.execute("SHOW INDEX FROM viagens WHERE Key_name = 'idx_viagens_status'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_viagens_status ON viagens(status)')

    cursor.execute("SHOW INDEX FROM viagens WHERE Key_name = 'idx_viagens_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_viagens_data ON viagens(criado_em)')

    cursor.execute("SHOW INDEX FROM viagens WHERE Key_name = 'idx_viagens_status_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_viagens_status_data ON viagens(status, criado_em)')


    cursor.execute('''
        CREATE TABLE IF NOT EXISTS viagens_enderecos (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            id_viagem INT UNSIGNED NOT NULL,
            tipo ENUM('origem', 'destino') NOT NULL,
            rua VARCHAR(150) NOT NULL,
            numero VARCHAR(20) NOT NULL,
            bairro VARCHAR(100) NOT NULL,
            cidade VARCHAR(100) NOT NULL,
            estado CHAR(2) NOT NULL CHECK(LENGTH(TRIM(estado)) = 2),
            criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_endereco_viagem
                FOREIGN KEY (id_viagem)
                REFERENCES viagens(id)
                ON DELETE RESTRICT ON UPDATE RESTRICT,
            
            UNIQUE KEY uk_viagem_tipo (id_viagem, tipo)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;''')
    

    cursor.execute(
        "SHOW INDEX FROM viagens_enderecos WHERE Key_name = 'idx_endereco_viagem'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_endereco_viagem ON viagens_enderecos(id_viagem)')


    cursor.execute('''
            CREATE TABLE IF NOT EXISTS registros_pagamento (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                id_viagem INT UNSIGNED NOT NULL,
                remetente VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(remetente)) > 0),
                recebedor VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(recebedor)) > 0),
                metodo_pagamento ENUM('pix', 'credito', 'debito', 'boleto') NOT NULL,
                parcelas TINYINT UNSIGNED NULL CHECK(parcelas BETWEEN 1 AND 12),
                valor_parcela DECIMAL(10, 2) NULL CHECK(valor_parcela > 0),
                valor_total DECIMAL(10, 2) NOT NULL CHECK(valor_total > 0),
                status ENUM('pendente', 'pago', 'cancelado',
                    'estornado') NOT NULL DEFAULT 'pendente',
                criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,

                CONSTRAINT fk_registro_viagem
                   FOREIGN KEY (id_viagem)
                   REFERENCES viagens(id)
                   ON DELETE RESTRICT ON UPDATE RESTRICT,

                CONSTRAINT chk_metodo_parcelas
                    CHECK(
                        (metodo_pagamento IN ('pix', 'debito')
                            AND parcelas IS NULL
                            AND valor_parcela IS NULL)
                    OR (metodo_pagamento IN ('credito', 'boleto')
                            AND parcelas BETWEEN 1 AND 12
                            AND valor_parcela IS NOT NULL)),
                
                UNIQUE KEY uk_pagamento_viagem (id_viagem)
            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
        ''')

    cursor.execute(
        "SHOW INDEX FROM registros_pagamento WHERE Key_name = 'idx_pagamento_remetente'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_pagamento_remetente ON registros_pagamento(remetente)')

    cursor.execute(
        "SHOW INDEX FROM registros_pagamento WHERE Key_name = 'idx_pagamento_recebedor'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_pagamento_recebedor ON registros_pagamento(recebedor)')

    cursor.execute(
        "SHOW INDEX FROM registros_pagamento WHERE Key_name = 'idx_pagamento_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_pagamento_data ON registros_pagamento(criado_em)')

    cursor.execute(
        "SHOW INDEX FROM registros_pagamento WHERE Key_name = 'idx_pagamento_viagem_status'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_pagamento_viagem_status ON' \
            ' registros_pagamento(id_viagem, status)')


@app1.route('/admin/passageiros', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def listar_passageiros_admin():
    try:
        logger.info('Listando passageiros...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, nome, telefone, saldo,
                       status, criado_em, atualizado_em
                    FROM passageiros ORDER BY criado_em DESC''')

            dados = [{
                'id': p[0],
                'nome': p[1],
                'telefone': p[2],
                'saldo': p[3],
                'status': p[4],
                'criado_em': p[5],
                'atualizado_em': p[6]
            } for p in cursor.fetchall()]

            if not dados:
                logger.warning(f'Nenhum passageiro cadastrado ainda.')
                return jsonify([]), 200

            logger.info('Listagem de passageiros bem-sucedida.')
            return jsonify(dados), 200
        
    except Exception as erro:
        logger.error(f'Erro inesperado ao listar passageiros: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao listar passageiros!'}), 500


@app1.route('/admin/passageiros/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def buscar_passageiro_admin(id):
    try:
        logger.info(f'Buscando passageiro com id={id}...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, nome, telefone, saldo,
                       status, criado_em, atualizado_em
                    FROM passageiros WHERE id = %s''', (id,))
            dado = cursor.fetchone()

            if not dado:
                logger.warning(f'Passageiro {id} não encontrado.')
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404

            logger.info(f'Busca de passageiro bem-sucedida.')
            return jsonify({
                'id': dado[0],
                'nome': dado[1],
                'telefone': dado[2],
                'saldo': dado[3],
                'status': dado[4],
                'criado_em': dado[5],
                'atualizado_em': dado[6]
            }), 200
        
    except Exception as erro:
        logger.error(f'Erro inesperado ao buscar passageiros: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar passageiros!'}), 500


@app1.route('/register', methods=['POST'])
@limiter.limit('3 per minute')
def register():
    try:
        logger.info('Registrando usuário...')

        dados = validar_json()

        REGRAS = {
            'usuario': lambda v: isinstance(v, str) and v.strip() != '',
            'senha': lambda v: isinstance(v, str) and v.strip() != ''
        }

        faltando = [c for c in REGRAS if c not in dados]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400
        
        for campo, regra in REGRAS.items():
            try:
                valor = str(dados[campo]).strip()

                if not regra(valor):
                    raise ValueError
                
                dados[campo] = valor
            except Exception:
                logger.warning(f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400
            
        with conexao() as cursor:
            cursor.execute('SELECT id FROM usuarios WHERE usuario = %s',
                           (dados['usuario'],))
            
            if cursor.fetchone():
                logger.warning(f'Usuário existente.')
                return jsonify({'erro': 'Usuário já existe!'}), 409
            
            criar_usuario(
                cursor,
                dados['usuario'],
                dados['senha']
            )
            
            logger.info('Usuário criado.')
            return jsonify({'mensagem': 'Usuário criado com sucesso.'}), 201
        
    except Exception as erro:
        logger.error(f'Erro inesperado ao registrar usuário: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao registrar usuário!'}), 500


@app1.route('/login', methods=['POST'])
@limiter.limit('5 per minute')
def login():
    try:
        logger.info('Gerando tokens...')

        ip = request.headers.get('X-Forwarded-For', request.remote_addr)

        if ip_bloqueado(ip):
            logger.warning(f'IP bloqueado por brute force.')
            return jsonify(
                {'erro': 'Muitas tentativas de login. Tente novamente mais tarde'
            }), 429

        dados = validar_json()

        REGRAS = {
            'usuario': lambda v: isinstance(v, str) and v.strip() != '',
            'senha': lambda v: isinstance(v, str) and v.strip() != ''
        }

        faltando = [c for c in REGRAS if c not in dados]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = str(dados[campo])

                if not regra(valor):
                    raise ValueError

                dados[campo] = valor

            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, senha_hash FROM usuarios
                    WHERE usuario = %s''',
                    (dados['usuario'],))
            
            usuario = cursor.fetchone()

            if not usuario:
                logger.warning(f'Usuário e/ou senha inválido.')
                registrar_falha(ip)
                return jsonify({'erro': 'Usuário e/ou senha inválido!'}), 401
            
            id_usuario_db, senha_hash = usuario

            if not bcrypt.checkpw(
                dados['senha'].encode(),
                senha_hash.encode()
            ):
                logger.warning('Usuario e/ou senha inválida.')
                registrar_falha(ip)
                return jsonify({'erro':'Usuário e/ou senha inválido!'}), 401
            
            limpar_falhas(ip)

            tokens, status = gerar_tokens(id_usuario_db)
            if status != 200:
                return jsonify(tokens), status

            salvar_refresh(
                cursor,
                id_usuario_db,
                tokens['refresh_token'],
                tokens['refresh_exp']
            )

            response = jsonify({
                'access_token': tokens['access_token']
            })

            response.set_cookie(
                'refresh_token',
                tokens['refresh_token'],
                httponly=True,
                secure=True,
                samesite='Lax',
                max_age=60 * 60 * 24 * 7
            )

            return response, 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao gerar token: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao gerar token!'}), 500


@app1.route('/refresh', methods=['POST'])
@limiter.limit('10 per minute')
def refresh():
    try:
        refresh_token = request.cookies.get('refresh_token')

        if not refresh_token:
            logger.warning('Refresh token não enviado.')
            return jsonify({'erro': 'Refresh token não enviado!'}), 401

        payload, status = validar_token(refresh_token, token_type='refresh')
        if status != 200:
            response = jsonify(payload)
            response.set_cookie(
                'refresh_token',
                '',
                expires=0,
                httponly=True,
                secure=True,
                samesite='Lax'
            )
            return response, status
        

        with conexao() as cursor:
            registro = refresh_valido(cursor, refresh_token)

            if not registro:
                logger.warning(f'Refresh token revogado ou inexistente.')
                response = jsonify({'erro': 'Refresh token inválido ao revogar!'})

                response.set_cookie(
                    'refresh_token',
                    '',
                    expires=0,
                    httponly=True,
                    secure=True,
                    samesite='Lax'
                )

                return response, 401
            
            revogar_refresh(cursor, refresh_token)

            novos_tokens, _ = gerar_tokens(payload['sub'])

            salvar_refresh(
                cursor,
                payload['sub'],
                novos_tokens['refresh_token'],
                novos_tokens['refresh_exp']
            )

            response = jsonify({
                'access_token': novos_tokens['access_token']
            })

            response.set_cookie(
                'refresh_token',
                novos_tokens['refresh_token'],
                httponly=True,
                secure=True,
                samesite='Lax',
                max_age=60 * 60 * 24 * 7
            )

            return response, 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao renovar token: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao renovar token!'}), 500


@app1.route('/logout', methods=['POST'])
@limiter.limit('10 per minute')
def logout():
    try:
        refresh_token = request.cookies.get('refresh_token')
        
        if not refresh_token:
            logger.warning('Refresh token não enviado.')
            return jsonify({'erro':'Refresh token não enviado!'}), 401

        payload, status =  validar_token(refresh_token, token_type='refresh')

        if status == 200:
            with conexao() as cursor:
                revogar_todos_refresh(cursor, payload['sub'])
            
        response = jsonify({'mensagem': 'Logout realizado com sucesso!'})

        response.set_cookie(
            'refresh_token',
            '',
            expires=0,
            httponly=True,
            secure=True,
            samesite='Lax'
        )

        return response, 200
    
    except Exception as erro:
        logger.error(f'Erro inesperado no logout: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado no logout!'}), 500


@app1.route('/admin/passageiros', methods=['POST'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def adicionar_passageiro_admin():
    try:
        logger.info('Adicionando passageiro...')

        dados = validar_json()

        NORMALIZACOES = {
            'nome': lambda v: formatar_nome(v),
            'cpf': lambda v: str(v).strip(),
            'telefone': lambda v: str(v).strip(),
            'saldo': lambda v: Decimal(str(v)).quantize(Decimal('0.01')),
        }

        REGRAS = {
            'nome': lambda v: v != '',
            'cpf': lambda v: re.fullmatch(r'\d{11}', v) is not None,
            'telefone': lambda v: re.fullmatch(r'\d{10,11}', v) is not None,
            'saldo': lambda v: v >= 0
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = dados[campo]
                valor = NORMALIZACOES[campo](valor)

                if not regra(valor):
                    raise ValueError

                dados[campo] = valor

            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}'}), 400

        with conexao() as cursor:
            cursor.execute('''
                INSERT INTO passageiros (
                    nome, cpf, telefone, saldo)
                    VALUES (%s, %s, %s, %s)
                ''', (dados['nome'], dados['cpf'],
                     dados['telefone'], dados['saldo'])
                )
            
            novo_id = cursor.lastrowid
            logger.info(f'Passageiro id={novo_id} adicionado com sucesso.')
            return jsonify({'mensagem': 'Passageiro adicionado com sucesso!',
                            'id': novo_id}), 201

    except Exception as erro:
        logger.error(f'Erro inesperado ao adicionar passageiro: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao adicionar passageiro!'}), 500


@app1.route('/admin/passageiros/<int:id>', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def atualizar_passageiro_admin(id):
    try:
        logger.info(f'Atualizando passageiro com id={id}...')

        dados = validar_json()

        NORMALIZACOES = {
            'nome': lambda v: formatar_nome(v),
            'telefone': lambda v: str(v).strip(),
            'saldo': lambda v: Decimal(str(v)).quantize(Decimal('0.01'))
        }

        REGRAS = {
            'nome': lambda v: v != '',
            'telefone': lambda v: re.fullmatch(r'\d{10,11}', v) is not None,
            'saldo': lambda v: v >= 0
        }

        enviados = {k: v for k, v in dados.items() if k in REGRAS and v is not None}

        if not enviados:
            logger.warning('Nenhum campo enviado para a atualização.')
            return jsonify({'erro': 'Envie ao menos um campo para atualizar!'}), 400

        for campo, valor in enviados.items():
            try:
                valor = NORMALIZACOES[campo](valor)

                if not REGRAS[campo](valor):
                    raise ValueError

                enviados[campo] = valor

            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        set_sql = ", ".join(f"{campo} = %s" for campo in enviados.keys())
        valores = list(enviados.values())
        valores.append(id)

        query = f"UPDATE passageiros SET {set_sql} WHERE id = %s AND status != 'bloqueado'"
        with conexao() as cursor:
            cursor.execute(query, valores)

            if cursor.rowcount == 0:
                cursor.execute('SELECT id FROM passageiros WHERE id = %s', (id,))
                if not cursor.fetchone():
                    logger.warning(f'Passageiro id={id} não encontrado.')
                    return jsonify({'erro': 'Passageiro não encontrado!'}), 404
                logger.warning('Passageiro bloqueado.')
                return jsonify(
                    {'erro': 'Passageiro bloqueado não pode ser atualizado!'}), 409

            logger.info(f'Passageiro id={id} atualizado com sucesso.')
            return '', 204

    except Exception as erro:
        logger.error(f'Erro inesperado ao atualizar passageiro: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao atualizar passageiro!'}), 500


@app1.route('/admin/passageiros/<int:id>/bloquear', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def bloquear_passageiro_admin(id):
    try:
        logger.info(f'Bloqueando passageiro com id={id}...')
        with conexao() as cursor:
            cursor.execute(
                'SELECT status FROM passageiros WHERE id = %s', (id,))
            
            passageiro = cursor.fetchone()

            if not passageiro:
                logger.warning(f'Passageiro id={id} não encontrado.')
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404
            
            if passageiro[0] == 'bloqueado':
                logger.warning(f'Passageiro id={id} já foi bloqueado.')
                return jsonify({'erro': 'Passageiro já foi bloqueado!'}), 409
            
            cursor.execute(
                "UPDATE passageiros SET status = 'bloqueado' WHERE id = %s",
                (id,))
            logger.info('Passageiro bloqueado com sucesso.')
            return '', 204
        
    except Exception as erro:
        logger.error(f'Erro inesperado ao bloquear passageiro: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao bloquear passageiro!'}), 500


@app1.route('/admin/passageiros/<int:id>/reativar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def reativar_passageiro_admin(id):
    try:
        logger.info(f'Reativando passageiro com id={id}...')
        with conexao() as cursor:
            cursor.execute(
                'SELECT status FROM passageiros WHERE id = %s', (id,))
            
            passageiro = cursor.fetchone()

            if not passageiro:
                logger.warning(f'Passageiro id={id} não encontrado.')
                return jsonify({'erro': 'Passsageiro não encontrado!'}), 404
            
            if passageiro[0] == 'ativo':
                logger.warning(f'Passageiro id={id} já foi ativado.')
                return jsonify({'erro': 'Passageiro já foi ativado!'}), 409
            
            cursor.execute(
                "UPDATE passageiros SET status = 'ativo' WHERE id = %s",
                (id,))
            
            logger.info('Passageiro reativado com sucesso.')
            return '', 204

    except Exception as erro:
        logger.error(f'Erro inesperado ao reativar passageiro: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao reativar passageiro!'}), 500


register_erro_handlers(app1)


app2 = Flask('API2')


@app2.route('/admin/motoristas', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def listar_motoristas_admin():
    try:
        logger.info('Listando motoristas...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, nome, cnh, telefone, categoria_cnh,
                       placa, modelo_carro, ano_carro, status,
                       quantia, criado_em, atualizado_em
                    FROM motoristas ORDER BY criado_em DESC''')
            dados = [{
                'id': m[0],
                'nome': m[1],
                'cnh': m[2],
                'telefone': m[3],
                'categoria_cnh': m[4],
                'placa': m[5],
                'modelo_carro': m[6],
                'ano_carro': m[7],
                'status': m[8],
                'quantia': m[9],
                'criado_em': m[10],
                'atualizado_em': m[11]
            } for m in cursor.fetchall()]

            if not dados:
                logger.warning('Nenhum motorista registrado ainda.')
                return jsonify([]), 200

            logger.info('Listagem de motoristas bem-sucedida.')
            return jsonify(dados), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao listar motoristas: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao listar motoristas!'}), 500


@app2.route('/admin/motoristas/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def buscar_motorista_admin(id):
    try:
        logger.info(f'Buscando motorista com id={id}...')
        with conexao() as cursor:
            cursor.execute('''
                SELECT id, nome, cnh, telefone, categoria_cnh,
                       placa, modelo_carro, ano_carro, status,
                       quantia, criado_em, atualizado_em
                    FROM motoristas WHERE id = %s''', (id,))
            dado = cursor.fetchone()

            if not dado:
                logger.warning(f'Motorista {id} não encontrado.')
                return jsonify({'erro': 'Motorista não encontrado!'}), 404

            logger.info('Busca de motorista bem-sucedida.')
            return jsonify({
                'id': dado[0],
                'nome': dado[1],
                'cnh': dado[2],
                'telefone': dado[3],
                'categoria_cnh': dado[4],
                'placa': dado[5],
                'modelo_carro': dado[6],
                'ano_carro': dado[7],
                'status': dado[8],
                'quantia': dado[9],
                'criado_em': dado[10],
                'atualizado_em': dado[11]
            }), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao buscar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar motorista!'}), 500


@app2.route('/admin/motoristas', methods=['POST'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def adicionar_motorista_admin():
    try:
        logger.info('Adicionando motorista...')

        dados = validar_json()

        NORMALIZACOES = {
            'nome': lambda v: formatar_nome(v),
            'cnh': lambda v: str(v).strip(),
            'telefone': lambda v: str(v).strip(),
            'categoria_cnh': lambda v: str(v).strip().upper(),
            'placa': lambda v: str(v).strip().upper(),
            'modelo_carro': lambda v: str(v).strip(),
            'ano_carro': lambda v: int(v)
        }

        REGRAS = {
            'nome': lambda v: v != '',
            'cnh': lambda v: re.fullmatch(r'\d{11}', v) is not None,
            'telefone': lambda v: re.fullmatch(r'\d{10,11}', v) is not None,
            'categoria_cnh': lambda v: v in ('A', 'B', 'C', 'D', 'E'),
            'placa': lambda v: re.fullmatch(
                r'[A-Z]{3}-?\d{4}|[A-Z]{3}\d[A-Z]\d{2}', v) is not None,
            'modelo_carro': lambda v: v != '',
            'ano_carro': lambda v: v >= 1980
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = dados[campo]
                valor = NORMALIZACOES[campo](valor)

                if not regra(valor):
                    raise ValueError

                dados[campo] = valor

            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        with conexao() as cursor:
            cursor.execute('''
                INSERT INTO motoristas
                    (nome, cnh, telefone, categoria_cnh,
                     placa, modelo_carro, ano_carro)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                           (dados['nome'], dados['cnh'], dados['telefone'],
                            dados['categoria_cnh'], dados['placa'],
                            dados['modelo_carro'], dados['ano_carro']))

            novo_id = cursor.lastrowid
            logger.info(f'Motorista id={novo_id} adicionado com sucesso.')
            return jsonify({'mensagem': 'Motorista adicionado!',
                            'id': novo_id}), 201

    except Exception as erro:
        logger.error(f'Erro inesperado ao adicionar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao adicionar motorista!'}), 500


@app2.route('/admin/motoristas/<int:id>', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def atualizar_motorista_admin(id):
    try:
        logger.info(f'Atualizando motorista com id={id}...')

        dados = validar_json()

        NORMALIZACOES = {
            'nome': lambda v: formatar_nome(v),
            'telefone': lambda v: str(v).strip(),
            'categoria_cnh': lambda v: str(v).strip().upper(),
            'placa': lambda v: str(v).strip().upper(),
            'modelo_carro': lambda v: str(v).strip(),
            'ano_carro': lambda v: int(v)
        }

        REGRAS = {
            'nome': lambda v: v != '',
            'telefone': lambda v: re.fullmatch(r'\d{10,11}', v) is not None,
            'categoria_cnh': lambda v: v in ('A', 'B', 'C', 'D', 'E'),
            'placa': lambda v: re.fullmatch(
                r'[A-Z]{3}-?\d{4}|[A-Z]{3}\d[A-Z]\d{2}', v) is not None,
            'modelo_carro': lambda v: v != '',
            'ano_carro': lambda v: v >= 1980
        }

        enviados = {k: v for k, v in dados.items() if k in REGRAS and v is not None}

        if not enviados:
            logger.warning('Nenhum campo enviado para a atualização.')
            return jsonify({'erro': 'Envie ao menos um campo para atualizar!'}), 400

        for campo, valor in enviados.items():
            try:
                valor = NORMALIZACOES[campo](valor)

                if not REGRAS[campo](valor):
                    raise ValueError

                enviados[campo] = valor

            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400


        set_sql = ", ".join(f"{campo} = %s" for campo in enviados.keys())
        valores = list(enviados.values())
        valores.append(id)

        query = f"UPDATE motoristas SET {set_sql} WHERE id = %s AND status != 'bloqueado'"
        with conexao() as cursor:
            cursor.execute(query, valores)

            if cursor.rowcount == 0:
                cursor.execute('SELECT id FROM motoristas WHERE id = %s', (id,))
                if not cursor.fetchone():
                    logger.warning(f'Motorista id={id} não encontrado.')
                    return jsonify({'erro': 'Motorista não encontrado!'}), 404
                logger.warning(f'Motorista id={id} bloqueado.')
                return jsonify(
                    {'erro': 'Motorista bloqueado não pode ser atualizado.'}), 409

            logger.info(f'Motorista id={id} atualizado com sucesso.')
            return '', 204

    except Exception as erro:
        logger.error(f'Erro inesperado ao atualizar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao atualizar motorista!'}), 500


@app2.route('/admin/motoristas/<int:id>/bloquear', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def bloquear_motorista_admin(id):
    try:
        logger.info(f'Bloqueando motorista com id={id}...')
        with conexao() as cursor:
            cursor.execute('SELECT status FROM motoristas WHERE id = %s', (id,))
            motorista = cursor.fetchone()

            if not motorista:
                logger.warning(f'Motorista id={id} não encontrado.')
                return jsonify({'erro': 'Motorista não encontrado!'}), 404
            
            if motorista[0] == 'bloqueado':
                logger.warning(f'Motorista id={id} já está bloqueado.')
                return jsonify(
                    {'erro': 'Motorista bloqueado já está bloqueado!'}), 409
            
            cursor.execute('''
                UPDATE motoristas SET
                     status = 'bloqueado' WHERE id = %s''',
                     (id,))

            logger.info('Motorista bloqueado com sucesso.')
            return '', 204
        
    except Exception as erro:
        logger.error(f'Erro inesperado ao bloquear motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao bloquear motorista!'}), 500


@app2.route('/admin/motoristas/<int:id>/reativar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def reativar_motorista_admin(id):
    try:
        logger.info(f'Reativando motorista com id={id}...')

        with conexao() as cursor:
            cursor.execute(
                'SELECT status FROM motoristas WHERE id = %s', (id,))
            
            motorista = cursor.fetchone()

            if not motorista:
                logger.warning(f'Motorista id={id} não encontrado.')
                return jsonify({'erro': 'Motorista não encontrado!'}), 404
            
            if motorista[0] == 'ativo':
                logger.warning(f'Motorista id={id} já está ativado.')
                return jsonify({'erro': 'Motorista já está ativado!'}), 409
            
            cursor.execute(
                "UPDATE motoristas SET status = 'ativo' WHERE id = %s",
                (id,))
            
            logger.info('Motorista reativado com sucesso.')
            return '', 204
        
    except Exception as erro:
        logger.error(f'Erro inesperado ao reativar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao reativar motorista!'}), 500


register_erro_handlers(app2)


app3 = Flask('API3')


@app3.route('/admin/viagens', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def listar_viagens_admin():
    try:
        logger.info('Listando viagens...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, id_passageiro, id_motorista, km, valor_por_km,
                       total, status, criado_em, atualizado_em
                    FROM viagens ORDER BY criado_em DESC''')
            
            dados = [{
                'id': v[0],
                'id_passageiro': v[1],
                'id_motorista': v[2],
                'km': v[3],
                'valor_por_km': v[4],
                'total': v[5],
                'status': v[6],
                'criado_em':  v[7],
                'atualizado_em': v[8]
            } for v in cursor.fetchall()]

            if not dados:
                logger.warning(f'Nenhum viagem encontrada.')
                return jsonify([]), 200

            logger.info(f'Listagem de viagens bem-sucedida.')
            return jsonify(dados), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao listar viagens: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao listar viagens!'}), 500


@app3.route('/admin/viagens/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def buscar_viagem_admin(id):
    try:
        logger.info(f'Buscando viagem com id={id}...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, id_passageiro, id_motorista, km, valor_por_km,
                       total, status, criado_em, atualizado_em
                    FROM viagens WHERE id = %s''', (id,))

            dado = cursor.fetchone()

            if not dado:
                logger.warning(f'Viagem id={id} não encontrada.')
                return jsonify({'erro': 'Viagem não encontrado!'}), 404

            logger.info(f'Busca de viagem bem-sucedida.')
            return jsonify({
                'id': dado[0],
                'id_passageiro': dado[1],
                'id_motorista': dado[2],
                'km': dado[3],
                'valor_por_km': dado[4],
                'total': dado[5],
                'status': dado[6],
                'criado_em': dado[7],
                'atualizado_em': dado[8]
            }), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao buscar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar viagem!'}), 500
    

@app3.route('/admin/viagens', methods=['POST'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def criar_viagem_admin():
    try:
        logger.info('Criando viagem...')

        dados = validar_json()

        if 'enderecos' not in dados or not dados['enderecos']:
            logger.warning('Endereços não enviado.')
            return jsonify({'erro': 'Viagens deve ter enderecos!'}), 400

        if not isinstance(dados['enderecos'], list):
            logger.warning('Endereços deve ser uma lista.')
            return jsonify({'erro': 'Endereços deve ser uma lista!'}), 400
        
        tipo = [str(e.get('tipo')).strip().lower() for e in dados['enderecos']]
        if tipo.count('origem') != 1 or tipo.count('destino') != 1:
            logger.warning('Viagem deve ter exatamente uma origem e um destino.')
            return jsonify({'erro':
                'Viagem deve ter exatamente uma origem e um destino!'}), 400

        NORMALZACOES = {
            'tipo': lambda v: str(v).strip().lower(),
            'rua': lambda v: str(v).strip(),
            'numero': lambda v: str(v).strip(),
            'bairro': lambda v: str(v).strip(),
            'cidade': lambda v: formatar_nome(v),
            'estado': lambda v: str(v).strip().upper()
        }

        REGRAS_ENDERECOS = {
            'tipo': lambda v: v in ('origem', 'destino'),
            'rua': lambda v: v != '',
            'numero': lambda v: v != '',
            'bairro': lambda v: v != '',
            'cidade': lambda v: v != '',
            'estado': lambda v: re.fullmatch(r'[A-Z]{2}', v) is not None
        }

        REGRAS_VIAGENS = {
            'id_passageiro': lambda v: v > 0,
            'id_motorista': lambda v: v > 0,
            'km': lambda v: v > 0,
            'valor_por_km': lambda v: v > 0
        }

        faltando = [c for c in REGRAS_VIAGENS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS_VIAGENS.items():
            try:
                valor = dados[campo]

                if campo in ('id_passageiro', 'id_motorista'):
                    valor = int(valor)
                
                elif campo in ('km', 'valor_por_km'):
                    try:
                        valor = Decimal(str(valor)).quantize(Decimal('0.01'))
                    except InvalidOperation:
                        raise ValueError
        
                if not regra(valor):
                    raise ValueError

                dados[campo] = valor

            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        with conexao() as cursor:
            try:
                cursor.execute('START TRANSACTION')

                cursor.execute('''
                    SELECT status FROM passageiros WHERE id = %s''',
                            (dados['id_passageiro'],))

                passageiro = cursor.fetchone()

                if not passageiro:
                    logger.warning(
                        f"Passageiro {dados['id_passageiro']} não encontrado.")
                    raise NotFound('Passageiro não encontrado!')
                
                if passageiro[0] == 'bloqueado':
                    logger.warning(f"Passageiro id={dados['id_passageiro']} bloqueado.")
                    raise Conflict('Passageiro inativo para viagem!')
                
                cursor.execute('''
                    SELECT id FROM viagens WHERE id_passageiro = %s
                        AND status IN ('criada', 'em_andamento')
                        FOR UPDATE''',
                    (dados['id_passageiro'],))
                
                if cursor.fetchone():
                    logger.warning(
                        f'''Viagem de passageiro id={dados['id_passageiro']}
                          já foi criada ou está em andamento.''')
                    raise BadRequest(
                        'Viagem de passageiro já foi criada ou está em andamento!')

                cursor.execute('''
                    SELECT status
                        FROM motoristas WHERE id = %s''',
                        (dados['id_motorista'],))
                
                motorista = cursor.fetchone()

                if not motorista:
                    logger.warning(
                        f"Motorista {dados['id_motorista']} não encontrado.")
                    raise NotFound('Motorista não encontrado!')
                
                if motorista[0] == 'bloqueado':
                    logger.warning(f"Motorista id={dados['id_motorista']} bloqueado.")
                    raise Conflict('Motorista inativo para viagem!')
                
                cursor.execute('''
                    SELECT id FROM viagens WHERE id_motorista = %s
                        AND status IN ('criada', 'em_andamento')
                        FOR UPDATE''',
                        (dados['id_motorista'],))
                
                if cursor.fetchone():
                    logger.warning('Motorista já está em viagem.')
                    raise BadRequest('Motorista já está em viagem!')

                try:
                    km = Decimal(str(dados['km'])).quantize(Decimal('0.01'))
                    valor_por_km = Decimal(str(dados['valor_por_km'])).quantize(
                        Decimal('0.01'))
                except InvalidOperation as erro:
                    logger.warning(f'Erro ao coletar dados: {str(erro)}')
                    raise BadRequest('Erro ao coletar dados!')

                total = (valor_por_km * km).quantize(Decimal('0.01'))

                cursor.execute('''
                        INSERT INTO viagens 
                            (id_passageiro, id_motorista, km,
                            valor_por_km, total, status) 
                            VALUES (%s, %s, %s, %s, %s, 'criada')
                        ''', (dados['id_passageiro'], dados['id_motorista'], km,
                            valor_por_km, total))
                
                id_viagem = cursor.lastrowid

                for endereco in dados['enderecos']:
                    faltando = [c for c in REGRAS_ENDERECOS if c not in endereco
                                or endereco[c] is None]
                    
                    if faltando:
                        logger.warning(
                            f"Campos obrigatórios no endereço: {', '.join(faltando)}")
                        raise BadRequest(f'''Campos obrigatórios no endereço:
                                        {', '.join(faltando)}''')
                    
                    for campo, regra in REGRAS_ENDERECOS.items():
                        try:
                            valor = endereco[campo]
                            valor = NORMALZACOES[campo](valor)

                            if not regra(valor):
                                raise ValueError
                            
                            endereco[campo] = valor
                        
                        except Exception as erro:
                            logger.warning(
                                f'Valor inválido para {campo}: {endereco.get(campo)}')
                            raise BadRequest(f'Valor inválido para {campo}!')

                    cursor.execute('''
                        INSERT INTO viagens_enderecos (id_viagem, tipo,
                            rua, numero, bairro, cidade, estado)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                            (id_viagem,
                            endereco['tipo'],
                            endereco['rua'],
                            endereco['numero'],
                            endereco['bairro'],
                            endereco['cidade'],
                            endereco['estado']))
                    
                cursor.execute('COMMIT')

                logger.info(f'Viagem id={id_viagem} criada com sucesso.')
                return jsonify({
                    'mensagem': 'Viagem criada com sucesso!',
                    'id': id_viagem,
                    'total': total
                }), 201
            
            except Exception:
                cursor.execute('ROLLBACK')
                raise

    except (BadRequest, NotFound, Conflict) as erro:
        logger.warning(f'Erro de válidação: {erro.description}')
        return jsonify({'erro': erro.description}), erro.code

    except Exception as erro:
        logger.error(f'Erro inesperado ao criar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao criar viagem!'}), 500


@app3.route('/admin/viagens/<int:id>/cancelar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def cancelar_viagem_admin(id):
    try:
        logger.info(f'Cancelar viagem com id={id}...')

        with conexao() as cursor:
            cursor.execute(
                "UPDATE viagens SET status = 'cancelada' WHERE id = %s" \
                "   AND status = 'criada'", (id,))
            
            if cursor.rowcount == 0:
                cursor.execute('SELECT id FROM viagens WHERE id = %s', (id,))
                if not cursor.fetchone():
                    logger.warning(f'Viagem id={id} não encontrada.')
                    return jsonify({'erro': 'Viagem não encontrada!'}), 404
                logger.warning(
                    f'''Viagem id={id} já cancelada ou finalizada
                      ou iniciada não pode ser cancelada.''')
                return jsonify(
                    {'erro': '''Viagem já cancelada ou finalizada
                      ou iniciada não pode ser cancelada!'''}), 409
            
            logger.info(f'Viagem cancelada com sucesso.')
            return '', 204
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao cancelar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao cancelar viagem!'}), 500


@app3.route('/admin/viagens/<int:id>/iniciar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def iniciar_viagem_admin(id):
    try:
        logger.info(f'Iniciando viagem com id={id}...')

        with conexao() as cursor:
            cursor.execute(
                "UPDATE viagens SET status = 'em_andamento' WHERE id = %s"\
                    "AND status = 'criada'",
                (id,))
            
            if cursor.rowcount == 0:
                cursor.execute('SELECT id FROM viagens WHERE id = %s', (id,))
                if not cursor.fetchone():
                    logger.warning(f'Viagem id={id} não encontrada.')
                    return jsonify({'erro': 'Viagem não encontrada!'}), 404
                logger.warning(f'Viagem id={id} precisar estar criada.')
                return jsonify(
                    {'erro': 'Viagem precisar estar criada para ser iniciada!'}), 409
            
            logger.info('Viagem iniciada com sucesso.')
            return '', 204
            
    except Exception as erro:
        logger.error(f'Erro inesperado ao iniciar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao iniciar viagem!'}), 500
    

@app3.route('/admin/viagens/<int:id>/finalizar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def finalizar_viagens_admin(id):
    try:
        logger.info(f'Finalizando viagem com id={id}...')

        with conexao() as cursor:
            cursor.execute(
                "UPDATE viagens v INNER JOIN registros_pagamento p" \
                "   ON p.id_viagem = v.id SET v.status = 'finalizada'"\
                "   WHERE v.id = %s AND v.status = 'em_andamento'"\
                "   AND p.pagamento = 'pago'", (id,))
            
            if cursor.rowcount == 0:
                cursor.execute('SELECT id FROM viagens WHERE id = %s', (id,))
                if not cursor.fetchone():
                    logger.warning(f'Viagem id={id} não encontrada.')
                    return jsonify({'erro': 'Viagem não encontrada!'}), 404
                logger.warning(
                    f'Viagem id={id} não está em andamento ou pagamento não foi confirmado')
                return jsonify({'erro': 'Viagem não está em andamento'\
                                 ' ou pagamento não foi confirmado!'}), 409
            
            logger.info('Viagem finalizada com sucesso.')
            return '', 204
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao finalizar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao finalizar viagem!'}), 500


register_erro_handlers(app3)


app4 = Flask('API4')


@app4.route('/admin/registros-pagamento', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def listar_registros_pagamento_admin():
    try:
        logger.info('Listando registros de pagamentos...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, id_viagem, remetente, recebedor,
                       metodo_pagamento, pagamento, valor_viagem,
                       parcelas, criado_em, atualizado_em
                    FROM registros_pagamento ORDER BY criado_em DESC''')
            
            dados = [{
                'id': rg[0],
                'id_viagem': rg[1],
                'remetente': rg[2],
                'recebedor': rg[3],
                'metodo_pagamento': rg[4],
                'pagamento': rg[5],
                'valor_viagem': rg[6],
                'parcelas': rg[7],
                'criado_em': rg[8],
                'atualizado_em': rg[9]
            } for rg in cursor.fetchall()]

            if not dados:
                logger.warning('Nenhum registro de pagamento registrado ainda.')
                return jsonify([]), 200

            logger.info('Listagem de registros de pagamento bem-sucedida.')
            return jsonify(dados), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao listar registros de pagamento: {str(erro)}')
        return jsonify(
            {'erro': 'Erro inesperado ao listar registros de pagamentos!'}), 500


@app4.route('/admin/registros-pagamento/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def buscar_registro_pagamento_admin(id):
    try:
        logger.info(f'Buscando registro de pagamento com id={id}...')
        with conexao() as cursor:
            cursor.execute('''
                SELECT id, id_viagem, remetente, recebedor,
                       metodo_pagamento, pagamento, valor_viagem,
                       parcelas, criado_em, atualizado_em
                    FROM registros_pagamento WHERE id = %s''',
                    (id,))
            dado = cursor.fetchone()

            if not dado:
                logger.warning(f'Registro de pagamento id={id} não encontrado.')
                return jsonify({'erro': 'Registro de pagamento não encontrado!'}), 404

            logger.info('Busca de registro de pagamento bem-sucedida.')
            return jsonify({
                'id': dado[0],
                'id_viagem': dado[1],
                'remetente': dado[2],
                'recebedor': dado[3],
                'metodo_pagamento': dado[4],
                'pagamento': dado[5],
                'valor_viagem': dado[6],
                'parcelas': dado[7],
                'criado_em': dado[8],
                'atualizado_em': dado[9]
            }), 200

    except Exception as erro:
        logger.error(
            f'Erro inesperado ao buscar registro de pagamento: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar registro de pagamento!'}), 500


@app4.route('/admin/registros-pagamento', methods=['POST'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def criar_pagamento_admin():
    try:
        logger.info('Adicionando registro de pagamentos...')

        dados = validar_json()

        REGRAS = {
            'id_viagem': lambda v: v > 0,
            'metodo_pagamento': lambda v: v in ('pix', 'credito', 'debito', 'boleto')
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campo obrigatório: {', '.join(faltando)}")
            return jsonify({'erro': f"Campo obrigatório: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = dados[campo]

                if campo == 'id_viagem':
                    valor = int(valor)

                elif campo == 'metodo_pagamento':
                    valor = str(valor).strip().lower()

                if not regra(valor):
                    raise ValueError

                dados[campo] = valor

            except Exception:
                logger.warning(f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        with conexao() as cursor:
            try:
                cursor.execute('START TRANSACTION')

                cursor.execute(
                    'SELECT id FROM registros_pagamento' \
                    ' WHERE id_viagem = %s FOR UPDATE',
                    (dados['id_viagem'],))
                
                if cursor.fetchone():
                    logger.warning(
                        f"Pagamento para id={dados['id_viagem']} já foi registrado.")
                    raise Conflict('Pagamento já foi registrado!')

                cursor.execute('''
                    SELECT id_passageiro, id_motorista, total
                        FROM viagens WHERE id = %s
                        AND status = 'em_andamento'
                            FOR UPDATE''',
                    (dados['id_viagem'],))

                viagem = cursor.fetchone()

                if not viagem:
                    logger.warning(
                        f'''Viagem id={dados['id_viagem']} não encontrado
                        ou não está em andamento.''')
                    raise BadRequest('Viagem não encontrado ou não está em andamento!')
                
                cursor.execute(
                    "SELECT nome FROM passageiros WHERE id = %s" \
                    "   AND status = 'ativo'", (viagem[0],))
                
                passageiro = cursor.fetchone()

                if not passageiro:
                    logger.warning(
                        f'Passageiro id={viagem[0]} não encontrado ou bloqueado.')
                    raise BadRequest('Passageiro não encontrado ou bloqueado!')
                
                cursor.execute(
                    "SELECT nome FROM motoristas WHERE id = %s" \
                    "   AND status = 'ativo'", (viagem[1],))
                
                motorista = cursor.fetchone()

                if not motorista:
                    logger.warning(
                        f'Motorista id={viagem[1]} não encontrado ou bloqueado.')
                    raise BadRequest('Motorista não encontrado ou bloqueado!')
                
                try:
                    remetente = formatar_nome(passageiro[0])
                    recebedor = formatar_nome(motorista[0])
                    valor_total = Decimal(str(viagem[2])).quantize(Decimal('0.01'))
                except (ValueError, TypeError, InvalidOperation) as erro:
                    logger.warning(f'Erro ao coletar dados em banco: {str(erro)}')
                    raise BadRequest('Erro ao coletar dados em banco SQL!')
                
                parcelas = 1
                
                if dados['metodo_pagamento'] in ('credito', 'boleto'):
                    if 'parcelas' not in dados:
                        logger.warning('Parcelas obrigatórias para crédito ou boleto.')
                        raise BadRequest('Parcelas obrigatórias para crédito ou boleto!')
                    try:
                        parcelas = int(dados['parcelas'])
                    except ValueError:
                        logger.warning('Parcelas inválidas.')
                        raise BadRequest('Parcelas inválidas!')
                    
                    if parcelas < 1 or parcelas > 12:
                        logger.warning('Parcelas deve ser entre 1 e 12.')
                        return jsonify(
                            {'erro': 'Parcelas deve estar entre 1 e 12!'}), 400
                    
                else:
                    parcelas = 1

                cursor.execute('''
                        INSERT INTO registros_pagamento
                            (id_viagem, remetente, recebedor,
                            metodo_pagamento, parcelas, valor_total
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                        ''', (dados['id_viagem'], remetente, recebedor,
                            dados['metodo_pagamento'], parcelas, valor_total))

                novo_id = cursor.lastrowid

                cursor.execute('COMMIT')

                logger.info(
                    f'Registro de pagamento id={novo_id} criado com sucesso.')
                return jsonify(
                    {'mensagem': 'Registro de pagamento criado com sucesso!',
                    'id': novo_id,
                    'total': valor_total}), 201

            except Exception:
                cursor.execute('ROLLBACK')
                raise

    except (BadRequest, Conflict) as erro:
        logger.warning(f'Erro de valores inválidos: {erro.description}')
        return jsonify({'erro': erro.description}), erro.code

    except Exception as erro:
        logger.error(
            f'Erro inesperado ao registrar pagamento: {str(erro)}')
        return jsonify(
            {'erro': 'Erro inesperado ao registrar pagamento!'}), 500


@app4.route('/registros-pagamento/<int:id>/cancelar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida
def cancelar_registro_pagamento(id):
    try:
        logger.info(f'Cancelando registro com id={id}...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id_viagem, status FROM registros_pagamento
                    WHERE id = %s FOR UPDATE''', (id,))
            registro = cursor.fetchone()

            if not registro:
                logger.warning(f'Registro id={id} não encontrado.')
                return jsonify({'erro': 'Registro não encontrado!'}), 404
            
            if registro[1] == 'cancelado':
                logger.warning(f'Registro id={id} já foi cancelado.')
                return jsonify({'erro': 'Registro já está cancelado!'}), 409

            cursor.execute(
                '''SELECT id_passageiro, id_motorista, total_viagem, status
                        FROM viagens WHERE id = %s FOR UPDATE''',
                  (registro[0],))
            viagem = cursor.fetchone()

            if not viagem:
                logger.warning(f'Viagem id={registro[0]} não encontrado.')
                return jsonify({'erro': 'Viagem não encontrada!'}), 404
            
            if viagem[3] != 'cancelada':
                logger.warning(f'Viagem não foi cancelada.')
                return '', 204
           
            cursor.execute(
                'SELECT saldo FROM passageiros WHERE id = %s FOR UPDATE',
                            (viagem[0],))

            passageiro = cursor.fetchone()

            if not passageiro:
                logger.warning(
                    f"Passageiro id={viagem[0]} não encontrado.")
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404

            cursor.execute(
                'SELECT quantia FROM motoristas WHERE id = %s FOR UPDATE',
                (viagem[1],))

            motorista = cursor.fetchone()

            if not motorista:
                logger.warning(
                    f"Motorista id={viagem[1]} não encontrado.")
                return jsonify({'erro': 'Motorista não encontrado!'}), 404
            
            if viagem[2] > motorista[0]:
                logger.warning('Incosistência financeira detectada.')
                return jsonify({'erro': 'Incosistência financeira!'}), 400

            cursor.execute(
                'UPDATE motoristas SET quantia = quantia - %s WHERE id = %s',
                            (viagem[2], viagem[1]))

            cursor.execute(
                '''UPDATE passageiros SET saldo = saldo + %s WHERE id = %s''',
                    (viagem[2], viagem[0]))
            
            cursor.execute('''
                UPDATE registros_pagamento SET status = 'cancelado',
                    pagamento = 'cancelado' WHERE id = %s''',
                    (id,))

            logger.info('Estorno realizado com sucesso!')
            return '', 204

    except Exception as erro:
        logger.error(f'Erro inesperado ao atualizar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao atualizar viagem!'}), 500


register_erro_handlers(app4)


app5 = Flask('API5')


@app5.route('/admin/pagamentos/pix', methods=['POST'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def criar_pagamento_pix_admin():
    try:
        logger.info('Criando pagamento via pix...')

        dados = validar_json()

        NORMALIZACOES = {
            'id_passageiro': lambda v: int(v),
            'valor': lambda v: Decimal(str(v)).quantize(Decimal('0.01'))
        }

        REGRAS = {
            'id_passageiro': lambda v: v > 0,
            'valor': lambda v: v > 0
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = dados[campo]
                valor = NORMALIZACOES[campo](valor)

                if not regra(valor):
                    raise ValueError
                
                dados[campo] = valor
            
            except Exception:
                logger.warning(f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        with conexao() as cursor:
            try:
                cursor.execute('START TRANSACTION')

                cursor.execute(
                    "SELECT nome_recebedor, chave_pix, cidade, status" \
                    "   FROM configuracoes_pix WHERE id = 1")
                
                config_pix = cursor.fetchone()

                if not config_pix:
                    logger.warning(f'Chave pix id=1 não encontrada.')
                    raise NotFound('Chave pix não encontrada!')
                
                nome_recebedor, chave_pix, cidade, status = config_pix

                if status == 'removida':
                    logger.warning(f'Chave pix id=1 foi removida.')
                    raise Conflict('Chave pix foi removida!')

                cursor.execute(
                    'SELECT id FROM passageiros WHERE id = %s',
                    (dados['id_passageiro'],))
                
                if not cursor.fetchone():
                    logger.warning(f"Passageiro id={dados['id_passageiro']} não encontrado.")
                    raise NotFound('Passageiro não encontrado!')
                
                idempotency_key = (request.headers.get('Idempotency-Key') 
                                or request.headers.get('X-Request-Id'))
                if not idempotency_key:
                    logger.warning('Idempotency-key é obrigatório.')
                    raise BadRequest(
                        'Idempotency-key é obrigatório. Gere um UUID e envie no header!')

                try:
                    uuid.UUID(idempotency_key)
                except ValueError:
                    logger.warning('Idempotency-Key inválida.')
                    raise BadRequest('idempotency-Key inválida. Use o formato UUID v4!')

                cursor.execute('''
                    SELECT id, qr_code_payload FROM pagamentos_pix
                        WHERE idempotency_key = %s FOR UPDATE''', 
                        (idempotency_key,))
                
                existente = cursor.fetchone()
                if existente:
                    logger.warning(
                        'Pagamentos via PIX já existe para esta Idempotency-Key.')
                    return jsonify({
                        'id': existente[0],
                        'QR Code': existente[1]
                    }), 200
                
                txid = ''.join(
                    random.choices(string.ascii_letters + string.digits, k=26)
                )
                qr_code_payload = gerar_qr_pix(
                    chave_pix=chave_pix,
                    valor=dados['valor'],
                    txid=txid,
                    nome=nome_recebedor,
                    cidade=cidade
                )
                liquidado_em = None
                
                cursor.execute('''
                    INSERT INTO pagamentos_pix
                        (id_passageiro, txid, idempotency_key,
                        valor, qr_code_payload, status, liquidado_em)
                        VALUES (%s, %s, %s, %s, %s, 'pendente', %s)''', 
                        (dados['id_passageiro'], txid, 
                        idempotency_key, dados['valor'], 
                        qr_code_payload, liquidado_em))
                
                novo_id = cursor.lastrowid

                cursor.execute('COMMIT')

                logger.info(f'Pagamento via pix criado id={novo_id}.')
                return jsonify({
                    'mensagem': 'Pagamento via pix gerado!',
                    'id': novo_id,
                    'pix_copia_e_cola': qr_code_payload,
                    'qr_code_url': f'/pagamentos/pix/{novo_id}/qrcode'}), 201
            
            except Exception:
                cursor.execute('ROLLBACK')
                raise

    except (BadRequest, Conflict, NotFound) as erro:
        logger.warning(f'Erro de válidação: {erro.description}')
        return jsonify({'erro': erro.description}), erro.code
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao criar pagamento via pix: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao criar pagamento via pix!'}), 500
            

app10 = Flask('API10')


@app10.route('/passageiros/<int:id_passageiro>/contas-bancarias', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida
def listar_contas_bancarias_passageiro(id_passageiro):
    try:
        logger.info(f'Listando contas bancárias de passageiro com id={id_passageiro}...')

        with conexao() as cursor:
            cursor.execute(
                "SELECT id, id_passageiro, banco_codigo, banco_nome, "\
                       "agencia, agencia_digito, conta, conta_digito, "\
                       "tipo_conta, titular_nome, status, principal, "\
                       "criado_em, atualizado_em "\
                "FROM contas_bancarias WHERE id_passageiro = %s AND status = 'ativa' "\
                "ORDER BY principal DESC, criado_em DESC",
                (id_passageiro,))
            
            dados = [{
                'id': c[0],
                'id_passageiro': c[1],
                'banco_codigo': c[2],
                'banco_nome': c[3],
                'agencia': c[4],
                'agencia_digito': c[5],
                'conta': c[6],
                'conta_digito': c[7],
                'tipo_conta': c[8],
                'titular_nome': c[9],
                'status': c[10],
                'principal': c[11],
                'criado_em': c[12],
                'atualizado_em': c[13]
            } for c in cursor.fetchall()]

            if not dados:
                logger.info(f'Nenhum conta bancária do passageiro encontrada')
                return jsonify([]), 200
            
            logger.info('Listagem de contas de bancárias bem-sucedida.')
            return jsonify(dados), 200
    
    except Exception as erro:
        logger.error(
            f'Erro inesperado ao listar contas bancárias de passageiro: {str(erro)}')
        return jsonify(
            {'erro': 'Erro inesperado ao listar contas bancárias de passageiro'}), 500
    

@app10.route('/admin/contas-bancarias', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def listar_contas_bancarias_admin():
    try:
        logger.info('Listando conta bancária...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, id_passageiro, banco_codigo, banco_nome,
                    agencia, agencia_digito, conta, conta_digito,
                    tipo_conta, titular_nome, status, principal,
                    criado_em, atualizado_em
                FROM contas_bancarias ORDER BY criado_em DESC''')
            
            dados = [{
                'id': c[0],
                'id_passageiro': c[1],
                'banco_codigo': c[2],
                'banco_nome': c[3],
                'agencia': c[4],
                'agencia_digito': c[5],
                'conta': c[6],
                'conta_digito': c[7],
                'tipo_conta': c[8],
                'titular_nome': c[9],
                'status': c[10],
                'principal': c[11],
                'criado_em': c[12],
                'atualizado_em': c[13]
            } for c in cursor.fetchall()]

            if not dados:
                logger.info('Nenhum conta bancária encontrado.')
                return jsonify([]), 200
            
            logger.info('Listagem de contas bancárias bem-sucedida.')
            return jsonify(dados), 200
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao listar contas bancárias: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao listar contas bancárias!'}), 500


@app10.route('/admin/contas-bancarias/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def buscar_conta_bancaria_admin(id):
    try:
        logger.info(f'Buscando conta bancária com id={id}...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, id_passageiro, banco_codigo, banco_nome, 
                       agencia, agencia_digito, conta, conta_digito,
                       tipo_conta, titular_nome, titular_documento,
                       status, principal, criado_em, atualizado_em
                FROM contas_bancarias WHERE id = %s''', (id,))
            
            dado = cursor.fetchone()

            if not dado:
                logger.warning(f'Conta bancária id={id} não encontrada.')
                return jsonify({'erro': 'Conta bancária não encontrada!'}), 404
            
            logger.info('Busca de conta bancária bem-sucedida.')
            return jsonify({
                'id': dado[0],
                'id_passageiro': dado[1],
                'banco_codigo': dado[2],
                'banco_nome': dado[3],
                'agencia': dado[4],
                'agencia_digito': dado[5],
                'conta': dado[6],
                'conta_digito': dado[7],
                'tipo_conta': dado[8],
                'titular_nome': dado[9],
                'titular_documento': dado[10],
                'status': dado[11],
                'principal': dado[12],
                'criado_em': dado[13],
                'atualizado_em': dado[14]
            }), 200
        
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao buscar conta bancária: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar conta bancária!'}), 500


@app10.route('/admin/contas-bancarias', methods=['POST'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def criar_conta_bancaria():
    try:
        logger.info('Criando conta bancária...')

        dados = validar_json()

        NORMALIZACOES = {
            'id_passageiro': lambda v: int(v),
            'banco_codigo': lambda v: f'{int(v):03d}',
            'banco_nome': lambda v: formatar_nome(v),
            'agencia': lambda v: str(v).strip(),
            'agencia_digito': lambda v: str(v).strip().upper() if v else None,
            'conta': lambda v: str(v).strip(),
            'conta_digito': lambda v: str(v).strip().upper() if v else None,
            'tipo_conta': lambda v: str(v).strip().lower(),
            'titular_nome': lambda v: formatar_nome(v),
            'titular_documento': lambda v: re.sub(r'\D', '', str(v)),
            'principal': lambda v: int(v)
        }

        REGRAS = {
            'id_passageiro': lambda v: v > 0,
            'banco_codigo': lambda v: re.fullmatch(r'\d{3}', v),
            'banco_nome': lambda v: re.fullmatch(
                r'[A-Za-zÀ-ÿ0-9\s\.\-]{3,}', v),
            'agencia': lambda v: re.fullmatch(r'\d{1,10}', v),
            'agencia_digito': lambda v: v is None or re.fullmatch(r'[0-9A-Za-z]', v),
            'conta': lambda v: re.fullmatch(r'\d{1,20}', v),
            'conta_digito': lambda v: v is None or re.fullmatch(r'[0-9A-Za-z]', v),
            'tipo_conta': lambda v: v in (
                'corrente', 'poupanca', 'salario', 'pagamento'),
            'titular_nome': lambda v: re.fullmatch(
                r'[A-Za-zÀ-ÿ\s\.\-]{3,}', v),
            'titular_documento': lambda v: re.fullmatch(r'\d{11}|\d{14}', v),
            'principal': lambda v: v in (0, 1)
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400
        
        for campo, regra in REGRAS.items():
            try:
                valor = dados[campo]
                valor = NORMALIZACOES[campo](valor)

                if not regra(valor):
                    raise ValueError

                dados[campo] = valor
                 
            except Exception:
                logger.warning(f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400
            
        with conexao() as cursor:
            try:
                cursor.execute('START TRANSACTION')

                cursor.execute(
                    "SELECT id FROM passageiros WHERE id = %s"\
                    "   AND status = 'ativo'",
                    (dados['id_passageiro']))
                
                if not cursor.fetchone():
                    logger.warning(
                        f"Passageiro id={dados['id_passageiro']} não encontrado ou bloqueado.")
                    raise Conflict('Passageiro não encontrado ou bloqueado!')
                
                if dados['principal'] == 1:
                    cursor.execute('''
                        SELECT id FROM contas_bancarias WHERE id_passageiro = %s
                            AND principal = 1 FOR UPDATE''',
                            (dados['id_passageiro']))
                    
                    if cursor.fetchone():
                        logger.warning('Já existe uma conta principal.')
                        raise Conflict('Já existe uma conta principal!')

                cursor.execute('''
                    INSERT INTO contas_bancarias
                        (id_passageiro, banco_codigo, banco_nome, agencia,
                        agencia_digito, conta, conta_digito, tipo_conta,
                        titular_nome, titular_documento, principal
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (dados['id_passageiro'], dados['banco_codigo'], dados['banco_nome'],
                    dados['agencia'], dados['agencia_digito'], dados['conta'],
                    dados['conta_digito'], dados['tipo_conta'], dados['titular_nome'],
                    dados['titular_documento'], dados['principal']))
                
                novo_id = cursor.lastrowid

                cursor.execute('COMMIT')

                logger.info(f'Conta id={novo_id} criada com sucesso.')
                return jsonify({'mensagem': 'Conta criado',
                                'id': novo_id}), 201
            except Exception:
                cursor.execute('ROLLBACK')
                raise
        
    except Exception as erro:
        logger.error(f'Erro inesperado ao criar conta bancária: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao criar conta bancária!'}), 500
    

@app10.route('/admin/contas-bancarias/<int:id>/principal', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def tornar_conta_bancaria_principal(id):
    try:
        logger.info(f'Tornar conta bancária principal com id={id}...')

        with conexao() as cursor:
            cursor.execute('START TRANSACTION')

            cursor.execute(
                "SELECT id_passageiro, principal FROM contas_bancarias "\
                    "WHERE id = %s AND status = 'ativa' FOR UPDATE",
                    (id,))
            
            conta = cursor.fetchone()

            if not conta:
                cursor.execute('ROLLBACK')
                logger.warning(f'Conta bancária id={id} não encontrado ou inativa.')
                return jsonify(
                    {'erro': 'Conta bancária não encontrada ou inativa!'}), 404
            
            id_passageiro, ja_principal = conta

            if ja_principal:
                cursor.execute('ROLLBACK')
                logger.warning(f'Conta bancária id={id} já é principal.')
                return jsonify({'erro': 'Conta bancária já é principal!'}), 409
            
            cursor.execute('''
                UPDATE contas_bancarias SET principal = FALSE
                    WHERE id_passageiro = %s AND principal = TRUE''',
                    (id_passageiro,))

            cursor.execute('''
                UPDATE contas_bancarias SET principal = TRUE
                    WHERE id = %s''', (id,))
            
            cursor.execute('COMMIT')

            logger.info(f'Conta bancária id={id} definida como principal com sucesso.')
            return '', 204
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao tornar conta bancária principal: {str(erro)}')
        return jsonify(
            {'erro': 'Erro inesperado ao tornar conta bancária principal!'}), 500
    

@app10.route('/admin/contas-bancarias/<int:id>/bloquear', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def bloquear_conta_bancaria(id):
    try:
        logger.info(f'Bloqueando conta bancária com id={id}...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id_passageiro, principal FROM contas_bancarias 
                   WHERE id = %s AND status = 'ativa' FOR UPDATE''',
                   (id,))
            
            conta = cursor.fetchone()
            
            if not conta:
                logger.warning(
                    f'Conta bancária id={id} não encontrada ou inativa.')
                return jsonify({'erro': 'Conta bancária não encontrada ou inativa!'}), 404
            
            id_passageiro, principal = conta

            if principal:
                cursor.execute('''
                    SELECT id FROM contas_bancarias WHERE id_passageiro = %s
                        AND status = 'ativa' AND id != %s LIMIT 1''',
                        (id_passageiro, id))
                
                if not cursor.fetchone():
                    logger.warning(f'Conta principal id={id} única.')
                    return jsonify(
                        {'erro': 'Não é possivel bloquear a única'
                        ' conta principal ativa!'}), 409
                
            cursor.execute(
                "UPDATE contas_bancarias SET status = 'bloqueada', " \
                "   principal = FALSE WHERE id = %s", (id,))
            
            logger.info(f'Conta bancária id={id} bloqueada com sucesso.')
            return '', 204
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao bloquear conta bancária: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao bloquear conta bancária!'}), 500
    

@app10.route('/admin/contas-bancarias/<int:id>/reativar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def reativar_conta_bancaria(id):
    try:
        logger.info(f'Reativando conta bancária com id={id}...')

        with conexao() as cursor:
            cursor.execute('START TRANSACTION')

            cursor.execute(
                "SELECT status FROM contas_bancarias" \
                " WHERE id = %s FOR UPDATE", (id,))
            
            conta = cursor.fetchone()

            if not conta:
                cursor.execute('ROLLBACK')
                logger.warning(f'Conta bancária id={id} não encontrada.')
                return jsonify({'erro': 'Conta bancária não encontrada!'}), 404
            
            if conta[0] == 'ativa':
                cursor.execute('ROLLBACK')
                logger.warning(f'Conta bancária id={id} já está ativa.')
                return jsonify({'erro': 'Conta bancária já está ativa!'}), 409
            
            if conta[0] == 'encerrada':
                cursor.execute('ROLLBACK')
                logger.warning(
                    f'Conta bancária id={id} está encerrada não pode ser reativada.')
                return jsonify(
                    {'erro': 'Conta bancária está encerrada não pode ser reativada!'}), 409
            
            cursor.execute('''
                UPDATE contas_bancarias SET status = 'ativa', 
                    principal = FALSE WHERE id = %s''', (id,))
            
            cursor.execute('COMMIT')
            
            logger.info(f'Conta bancária id={id} reativada com sucesso.')
            return '', 204
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao reativar conta bancária: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao reativar conta bancária!'}), 500


@app10.route('/admin/contas-bancarias/<int:id>/encerrar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def encerrar_conta_bancaria(id):
    try:
        logger.info(f'Encerrando conta bancária com id={id}...')

        with conexao() as cursor:
            cursor.execute('START TRANSACTION')

            cursor.execute(
                "SELECT status FROM contas_bancarias" \
                "   WHERE id = %s FOR UPDATE", (id,))
            
            conta = cursor.fetchone()

            if not conta:
                cursor.execute('ROLLBACK')
                logger.warning(f'Conta bancária id={id} não encontrada.')
                return jsonify({'erro': 'Conta bancária não encontrada!'}
                               ), 404
            
            if conta[0] == 'encerrada':
                cursor.execute('ROLLBACK')
                logger.warning(f'Conta bancária id={id} já foi encerrada.')
                return jsonify({'erro': 'Conta bancária já foi encerrada!'}), 409

            cursor.execute(
                "UPDATE contas_bancarias SET status = 'encerrada', " \
                "   principal = FALSE WHERE id = %s "\
                "   LIMIT 1", (id,))
            
            cursor.execute('COMMIT')
            
            logger.info(f'Conta bancária id={id} encerrada com sucesso.')
            return '', 204
        
    except Exception as erro:
        logger.error(f'Erro inesperado ao encerrar conta bancária: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao encerrar conta bancária!'}), 500


register_erro_handlers(app10)


def start_api(app, port):
    app.run(debug=True, port=port, use_reloader=False)


def main():
    apis = [(app1, 5001),
            (app2, 5002),
            (app3, 5003),
            (app4, 5004)]
        

    for app, port in apis:
        threading.Thread(target=start_api,
                        args=(app, port),
                        daemon=True).start()

    input('APIs rodando. Pressione ENTER para sair.\n')
    
if __name__ == '__main__':
    main()






