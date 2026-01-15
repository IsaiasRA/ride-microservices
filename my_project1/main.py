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
from decimal import Decimal, InvalidOperation
import threading
import logging
import bcrypt
import re
import mysql.connector


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
                status ENUM('ativo', 'bloqueado') DEFAULT 'ativo',
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
        
    cursor.execute("SHOW INDEX FROM passageiros WHERE Key_name = 'idx_passa_data'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_passa_data ON passageiros(criado_em)')

    cursor.execute("SHOW INDEX FROM passageiros WHERE Key_name = 'idx_passa_status_data'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_passa_status_data ON passageiros(status, criado_em)')


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
                status ENUM('ativo', 'bloqueado') DEFAULT 'ativo',
                valor_passagem DECIMAL(10, 2) UNSIGNED NOT NULL,
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
                status ENUM('criada', 'em_andamento', 'confirmada',
                   'finalizada', 'cancelada') DEFAULT 'criada',
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
            criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_endereco_viagem
                FOREIGN KEY (id_viagem)
                REFERENCES viagens(id)
                ON DELETE CASCADE ON UPDATE RESTRICT,
            
            UNIQUE KEY uk_viagem_tipo (id_viagem, tipo)
        ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;''')
    

    cursor.execute(
        "SHOW INDEX FROM viagens_enderecos WHERE Key_name = 'idx_endereco_viagem'")
    if not cursor.fetchone():
        cursor.execute('CREATE INDEX idx_endereco_viagem ON viagens_enderecos(id_viagem)')


    cursor.execute('''
            CREATE TABLE IF NOT EXISTS registros_pagamento (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                id_viagem INT UNSIGNED,
                remetente VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(remetente)) > 0),
                recebedor VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(recebedor)) > 0),
                metodo_pagamento ENUM('pix', 'credito', 'debito', 'boleto') NOT NULL,
                pagamento ENUM('pago', 'cancelado', 'pendente') DEFAULT 'pago',
                valor_viagem DECIMAL(10, 2) NOT NULL CHECK(valor_viagem > 0),
                parcelas TINYINT UNSIGNED DEFAULT 1 CHECK(parcelas BETWEEN 1 AND 12),
                criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,

                CONSTRAINT fk_registro_viagem
                   FOREIGN KEY (id_viagem)
                   REFERENCES viagens(id)
                   ON DELETE RESTRICT ON UPDATE RESTRICT,

                CONSTRAINT chk_metodo_parcelas
                    CHECK((metodo_pagamento IN ('pix', 'debito') AND parcelas = 1)
                    OR (metodo_pagamento IN ('credito', 'boleto')
                    AND parcelas BETWEEN 1 AND 12))

            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
        ''')

    cursor.execute(
        "SHOW INDEX FROM registros_pagamento WHERE Key_name = 'idx_pagamento_viagem'")
    if not cursor.fetchone():
        cursor.execute(
            'CREATE INDEX idx_pagamento_viagem ON registros_pagamento(id_viagem)')

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


@app1.route('/passageiros', methods=['GET'])
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


@app1.route('/passageiros/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def buscar_passageiro(id):
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


@app1.route('/passageiros', methods=['POST'])
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


@app1.route('/passageiros/<int:id>', methods=['PATCH'])
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


@app1.route('/passageiros/<int:id>/bloquear', methods=['PATCH'])
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


@app1.route('/passageiros/<int:id>/reativar', methods=['PATCH'])
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


@app2.route('/motoristas', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def listar_motoristas_admin():
    try:
        logger.info('Listando motoristas...')

        with conexao() as cursor:
            cursor.execute('''
                SELECT id, nome, cnh, telefone, categoria_cnh, placa,
                       modelo_carro, ano_carro, status, valor_passagem,
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
                'valor_passagem': m[9],
                'quantia': m[10],
                'criado_em': m[11],
                'atualizado_em': m[12]
            } for m in cursor.fetchall()]

            if not dados:
                logger.warning('Nenhum motorista registrado ainda.')
                return jsonify([]), 200

            logger.info('Listagem de motoristas bem-sucedida.')
            return jsonify(dados), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao listar motoristas: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao listar motoristas!'}), 500


@app2.route('/motoristas/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida(role='admin')
def buscar_motorista_admin(id):
    try:
        logger.info(f'Buscando motorista com id={id}...')
        with conexao() as cursor:
            cursor.execute('''
                SELECT id, nome, cnh, telefone, categoria_cnh, placa,
                       modelo_carro, ano_carro, status, valor_passagem,
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
                'valor_passagem': dado[9],
                'quantia': dado[10],
                'criado_em': dado[11],
                'atualizado_em': dado[12]
            }), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao buscar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar motorista!'}), 500


@app2.route('/motoristas', methods=['POST'])
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
            'ano_carro': lambda v: int(v),
            'valor_passagem': lambda v: Decimal(str(v)).quantize(Decimal('0.01'))
        }

        REGRAS = {
            'nome': lambda v: v != '',
            'cnh': lambda v: re.fullmatch(r'\d{11}', v) is not None,
            'telefone': lambda v: re.fullmatch(r'\d{10,11}', v) is not None,
            'categoria_cnh': lambda v: v in ('A', 'B', 'C', 'D', 'E'),
            'placa': lambda v: re.fullmatch(
                r'[A-Z]{3}-?\d{4}|[A-Z]{3}\d[A-Z]\d{2}', v) is not None,
            'modelo_carro': lambda v: v != '',
            'ano_carro': lambda v: v >= 1980,
            'valor_passagem': lambda v: v > 0
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
                     placa, modelo_carro, ano_carro, valor_passagem)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                           (dados['nome'], dados['cnh'], dados['telefone'],
                            dados['categoria_cnh'], dados['placa'], dados['modelo_carro'],
                               dados['ano_carro'], dados['valor_passagem']))

            novo_id = cursor.lastrowid
            logger.info(f'Motorista id={novo_id} adicionado com sucesso.')
            return jsonify({'mensagem': 'Motorista adicionado!',
                            'id': novo_id}), 201

    except Exception as erro:
        logger.error(f'Erro inesperado ao adicionar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao adicionar motorista!'}), 500


@app2.route('/motoristas/<int:id>', methods=['PATCH'])
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
            'ano_carro': lambda v: int(v),
            'valor_passagem': lambda v: Decimal(str(v)).quantize(Decimal('0.01'))
        }

        REGRAS = {
            'nome': lambda v: v != '',
            'telefone': lambda v: re.fullmatch(r'\d{10,11}', v) is not None,
            'categoria_cnh': lambda v: v in ('A', 'B', 'C', 'D', 'E'),
            'placa': lambda v: re.fullmatch(
                r'[A-Z]{3}-?\d{4}|[A-Z]{3}\d[A-Z]\d{2}', v) is not None,
            'modelo_carro': lambda v: v != '',
            'ano_carro': lambda v: v >= 1980,
            'valor_passagem': lambda v: v > 0
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


@app2.route('/motoristas/<int:id>/bloquear', methods=['PATCH'])
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


@app2.route('/motoristas/<int:id>/reativar', methods=['PATCH'])
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
            'km': lambda v: v > 0
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
                
                elif campo == 'km':
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
                    SELECT valor_passagem, status
                        FROM motoristas WHERE id = %s''',
                        (dados['id_motorista'],))
                
                motorista = cursor.fetchone()

                if not motorista:
                    logger.warning(
                        f"Motorista {dados['id_motorista']} não encontrado.")
                    raise NotFound('Motorista não encontrado!')
                
                if motorista[1] == 'bloqueado':
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
                    valor_por_km = Decimal(str(motorista[0])).quantize(Decimal('0.01'))
                except InvalidOperation as erro:
                    logger.warning(f'Erro ao coletar dados: {str(erro)}')
                    raise BadRequest('Erro ao coletar dados!')

                if valor_por_km <= 0:
                    logger.warning(f'Valor da passagem não pode ser negativo.')
                    raise BadRequest('Valor da passagem não pode ser negativo!')

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


@app3.route('/viagens/<int:id>/cancelar', methods=['PATCH'])
@limiter.limit('100 per hour')
@rota_protegida
def cancelar_viagem(id):
    try:
        logger.info(f'Cancelando viagem com id={id}...')

        with conexao() as cursor:
            cursor.execute(
                '''SELECT status FROM viagens WHERE id = %s''',
                  (id,))
            viagem = cursor.fetchone()

            if not viagem:
                logger.warning(f'Viagem id={id} não encontrada.')
                return jsonify({'erro': 'Viagem não encontrada!'}), 404
            
            cursor.execute("UPDATE viagens SET status = 'cancelada'"\
                    "WHERE id = %s AND status != 'cancelada'",
                    (id,))
            
            if cursor.rowcount == 0:
                logger.warning(f'Viagem já foi cancelada.')
                return jsonify({'erro': 'Viagem já está cancelada!'}), 409

            logger.info('Viagem cancelada com sucesso!')
            return '', 204

    except Exception as erro:
        logger.error(f'Erro inesperado ao atualizar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao atualizar viagem!'}), 500
    

register_erro_handlers(app3)


app4 = Flask('API4')


@app4.route('/registros-pagamento', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida
def listar_registros_pagamento():
    try:
        logger.info('Listando registros de pagamentos...')
        with conexao() as cursor:
            cursor.execute('''
                SELECT id, id_viagem, remetente, recebedor,
                       metodo_pagamento, pagamento, status,
                       valor_viagem, criado_em, atualizado_em
                    FROM registros_pagamento''')
            
            dados = [{
                'id': rg[0],
                'id_viagem': rg[1],
                'remetente': rg[2],
                'recebedor': rg[3],
                'metodo_pagamento': rg[4],
                'pagamento': rg[5],
                'status': rg[6],
                'valor_viagem': rg[7],
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


@app4.route('/registros-pagamento/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
@rota_protegida
def buscar_registro_pagamento(id):
    try:
        logger.info(f'Buscando registro de pagamento com id={id}...')
        with conexao() as cursor:
            cursor.execute('''
                SELECT id, id_viagem, remetente, recebedor,
                       metodo_pagamento, pagamento, status,
                       valor_viagem, criado_em, atualizado_em
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
                'status': dado[6],
                'valor_viagem': dado[7],
                'criado_em': dado[8],
                'atualizado_em': dado[9]
            }), 200

    except Exception as erro:
        logger.error(
            f'Erro inesperado ao buscar registro de pagamento: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar registro de pagamento!'}), 500


@app4.route('/registros-pagamento', methods=['POST'])
@limiter.limit('100 per hour')
@rota_protegida
def adicionar_pagamento():
    try:
        logger.info('Adicionando registro de pagamentos...')

        dados = validar_json()

        REGRAS = {
            'id_viagem': lambda v: isinstance(v, int) and v > 0
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campo obrigatório: {''.join(faltando)}")
            return jsonify({'erro': f"Campo obrigatório: {''.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = int(dados[campo])

                if not regra(valor):
                    raise ValueError

                dados[campo] = valor
            except Exception:
                logger.warning(f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        with conexao() as cursor:
            cursor.execute('''
                SELECT nome_passageiro, nome_motorista,
                       metodo_pagamento, total_viagem, status
                    FROM viagens WHERE id = %s FOR UPDATE''',
                (dados['id_viagem'],))

            viagem = cursor.fetchone()

            if not viagem:
                logger.warning(
                    f"Viagem id={dados['id_viagem']} não encontrada.")
                return jsonify({'erro': 'Viagem não encontrada!'}), 404
            
            cursor.execute('''
                SELECT id FROM registros_pagamento
                    WHERE id_viagem = %s''', (dados['id_viagem'],))
            
            if cursor.fetchone():
                logger.warning(f"Pagamento id={dados['id_viagem']} já existe.")
                return jsonify({'erro': 'Pagamento de viagem já está registrado!'}), 409

            try:
                remetente = formatar_nome(viagem[0])
                recebedor = formatar_nome(viagem[1])
                metodo_pagamento = str(viagem[2]).strip().lower()
                valor_viagem = Decimal(str(viagem[3])).quantize(Decimal('0.01'))
                status_viagem = str(viagem[4]).strip().lower()
            except (ValueError, TypeError, InvalidOperation) as erro:
                logger.warning(f'Erro ao coletar dados em banco: {str(erro)}')
                return jsonify({'erro': 'Erro ao coletar dados em banco SQL!'}), 400
            
            if status_viagem != 'confirmada':
                logger.warning('Viagem cancelada.')
                return jsonify({'erro': 'Viagem cancelada não pode ser registrada!'}), 400

            cursor.execute('''
                    INSERT INTO registros_pagamento
                        (id_viagem, remetente, recebedor,
                         metodo_pagamento, valor_viagem
                        ) VALUES (%s, %s, %s, %s, %s)
                    ''', (dados['id_viagem'], remetente, recebedor,
                          metodo_pagamento, valor_viagem))

            novo_id = cursor.lastrowid
            logger.info(
                f'Registro de pagamento id={novo_id} adicionado com sucesso.')
            return jsonify(
                {'mensagem': 'Registro de pagamento adicionado com sucesso!',
                 'id': novo_id}), 201

    except Exception as erro:
        logger.error(
            f'Erro inesperado ao adicionar registro de pagamento: {str(erro)}')
        return jsonify(
            {'erro': 'Erro inesperado ao adicionar registro de pagamento!'}), 500


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
