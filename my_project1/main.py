from flask import Flask, jsonify, request
from contextlib import closing, contextmanager
from app1.error import (tratamento_erro_mysql,
                        register_erro_handlers,
                        tratamento_erro_requests)
from app1.log import configurar_logging
from app1.validation import validar_json
from app1.auth import (SECRET_KEY, USUARIO, gerar_tokens,
                       rota_protegida, validar_token)
from datetime import datetime, timedelta, timezone
from werkzeug.exceptions import BadRequest
from decimal import Decimal, InvalidOperation
import threading
import logging
import requests
import re
import mysql.connector


app1 = Flask('API1')

configurar_logging()
logger = logging.getLogger(__name__)


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
            CREATE TABLE IF NOT EXISTS passageiros (
                id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                nome VARCHAR(100) NOT NULL CHECK(LENGTH(TRIM(nome)) > 0),
                cpf VARCHAR(14) NOT NULL UNIQUE CHECK(LENGTH(TRIM(cpf)) IN (11, 14)),
                telefone VARCHAR(20) NOT NULL UNIQUE CHECK(LENGTH(TRIM(telefone)) >= 8),
                valor DECIMAL(10, 2) NOT NULL CHECK(valor > 0),
                endereco_rua VARCHAR(100) NOT NULL,
                endereco_numero VARCHAR(10) NOT NULL,
                endereco_bairro VARCHAR(50) NOT NULL,
                endereco_cidade VARCHAR(50) NOT NULL,
                endereco_estado CHAR(2) NOT NULL CHECK(LENGTH(TRIM(endereco_estado)) = 2),
                endereco_cep VARCHAR(10) NOT NULL CHECK(LENGTH(TRIM(endereco_cep)) >= 8),
                km DECIMAL(6, 2) NOT NULL CHECK(km > 0),
                metodo_pagamento ENUM('pix', 'credito', 'debito', 'boleto') NOT NULL,
                pagamento ENUM('pago', 'cancelado', 'pendente') DEFAULT 'pendente',
                criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE = InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
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
                valor_passagem DECIMAL(5, 2) UNSIGNED DEFAULT 2.50,
                quantia DECIMAL(10, 2) UNSIGNED DEFAULT 0 CHECK(quantia >= 0),
                criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
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
                metodo_pagamento ENUM('pix', 'credito', 'debito', 'boleto') NOT NULL,
                pagamento ENUM('pago', 'cancelado', 'pendente') NOT NULL,
                status ENUM('confirmada', 'cancelada') DEFAULT 'confirmada',
                criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
                atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                CONSTRAINT fk_viagens_passageiro
                   FOREIGN KEY (id_passageiro)
                   REFERENCES passageiros(id)
                   ON DELETE CASCADE ON UPDATE CASCADE,

                CONSTRAINT fk_viagens_motoristas
                    FOREIGN KEY (id_motorista)
                    REFERENCES motoristas(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,

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
                pagamento ENUM('pago', 'cancelado', 'pendente') NOT NULL,
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


@app1.route('/passageiros', methods=['GET'])
@rota_protegida
def listar_passageiros():
    try:
        logger.info('Listando passageiros...')
        with conexao() as cursor:
            cursor.execute('SELECT * FROM passageiros')

            dados = [{
                'id': p[0],
                'nome': p[1],
                'cpf': p[2],
                'telefone': p[3],
                'valor': p[4],
                'endereco_rua': p[5],
                'endereco_numero': p[6],
                'endereco_bairro': p[7],
                'endereco_cidade': p[8],
                'endereco_estado': p[9],
                'endereco_cep': p[10],
                'km': p[11],
                'metodo_pagamento': p[12],
                'pagamento': p[13],
                'criado_em': p[14],
                'atualizado_em': p[15]
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
@rota_protegida
def buscar_passageiro(id):
    try:
        logger.info(f'Buscando passageiro com id={id}...')
        with conexao() as cursor:
            cursor.execute('SELECT * FROM passageiros WHERE id = %s', (id,))
            dado = cursor.fetchone()

            if not dado:
                logger.warning(f'Passageiro {id} não encontrado.')
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404

            logger.info(f'Busca de passageiro com id={id} bem-sucedida.')
            return jsonify({
                'id': dado[0],
                'nome': dado[1],
                'cpf': dado[2],
                'telefone': dado[3],
                'valor': dado[4],
                'endereco_rua': dado[5],
                'endereco_numero': dado[6],
                'endereco_bairro': dado[7],
                'endereco_cidade': dado[8],
                'endereco_estado': dado[9],
                'endereco_cep': dado[10],
                'km': dado[11],
                'metodo_pagamento': dado[12],
                'pagamento': dado[13],
                'criado_em': dado[14],
                'atualizado_em': dado[15]
            }), 200
    except Exception as erro:
        logger.error(f'Erro inesperado ao buscar passageiros: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar passageiros!'}), 500


@app1.route('/login/<id_usuario>', methods=['POST'])
def login(id_usuario):
    try:
        logger.info('Gerando tokens...')

        dados = validar_json()

        if not dados:
            logger.warning(
                'Dados ausentes ou inválidos no corpo da requisição.')
            return jsonify(
                {'erro': 'Dados ausentes ou inválidos no corpo da requisição!'}), 404

        REGRAS = {
            'usuario': lambda v: isinstance(v, str) and v.strip() != '',
            'senha': lambda v: isinstance(v, str) and v.strip() != ''
        }

        faltando = [c for c in REGRAS if c not in dados]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"})

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

        if dados['usuario'] != USUARIO or dados['senha'] != SECRET_KEY:
            logger.warning('Usuário e/ou senha inválido.')
            return jsonify({'erro': 'Usuário e/ou senha inválido!'}), 400

        tokens, status = gerar_tokens(id_usuario)
        if status != 200:
            return jsonify(tokens), status

        response = jsonify({
            'access_token': tokens['access_token']
        })

        response.set_cookie(
            'refresh_token',
            tokens['refresh_token'],
            httponly=True,
            secure=False,
            samesite='Lax',
            max_age=60 * 60 * 24 * 7
        )

        return response, 200

    except BadRequest:
        logger.warning(
            'JSON malformado! Dado inválido no corpo da requisição.')
        return jsonify({'erro': 'JSON malformado. Envie dado válido!'}), 400

    except Exception as erro:
        logger.error(f'Erro inesperado ao gerar token: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao gerar token!'}), 500


@app1.route('/refresh', methods=['POST'])
def refresh():
    try:
        refresh_token = request.cookies.get('refresh_token')

        if not refresh_token:
            logger.warning('Refresh token não enviado.')
            return jsonify({'erro': 'Refresh token não enviado!'}), 401

        payload, status = validar_token(refresh_token, token_type='refresh')
        if status != 200:
            return jsonify(payload), status

        novos_tokens, _ = gerar_tokens(payload['sub'])

        response = jsonify({
            'access_token': novos_tokens['access_token']
        })

        response.set_cookie(
            'refresh_token',
            novos_tokens['refresh_token'],
            httponly=True,
            secure=False,
            samesite='Lax',
            max_age=60 * 60 * 24 * 7
        )

        return response, 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao renovar token: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao renovar token!'}), 500


@app1.route('/passageiros', methods=['POST'])
@rota_protegida
def adicionar_passageiro():
    try:
        logger.info('Adicionando passageiro...')

        dados = validar_json()

        REGRAS = {
            'nome': lambda v: isinstance(v, str) and v.strip() != '',
            'cpf': lambda v: isinstance(
                v, str) and re.fullmatch(r'\d{3}\.?\d{3}\.?\d{3}-?\d{2}', v) is not None,
            'telefone': lambda v: isinstance(
                v, str) and re.fullmatch(r'\(?\d{2}\)?\s?\d{4,5}-?\d{4}', v) is not None,
            'valor': lambda v: isinstance(v, Decimal) and v > 0,
            'endereco_rua': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_numero': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_bairro': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_cidade': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_estado': lambda v: isinstance(v, str) and len(v.strip()) == 2,
            'endereco_cep': lambda v: isinstance(
                v, str) and re.fullmatch(r'\d{5}-?\d{3}', v) is not None,
            'km': lambda v: isinstance(v, Decimal) and v > 0,
            'metodo_pagamento': lambda v: isinstance(
                v, str) and v.strip().lower() in ('pix', 'credito', 'debito', 'boleto'),
            'pagamento': lambda v: isinstance(
                v, str) and v.strip().lower() in ('pago', 'cancelado', 'pendente')
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = dados[campo]

                if campo in ('cpf', 'telefone', 'endereco_rua', 'endereco_numero',
                             'endereco_bairro', 'endereco_cep'):
                    valor = str(valor).strip()
                
                elif campo in ('nome', 'endereco_cidade'):
                    valor = str(valor).strip().title()
                
                elif campo in ('pagamento', 'metodo_pagamento'):
                    valor = str(valor).strip().lower()
                
                elif campo == 'endereco_estado':
                    valor = str(valor).strip().upper()

                elif campo in ('valor', 'km'):
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
                return jsonify({'erro': f'Valor inválido para {campo}'}), 400

        br = timezone(timedelta(hours=-3))
        agora = datetime.now(br).isoformat()

        with conexao() as cursor:
            cursor.execute('''
                INSERT INTO passageiros (
                    nome, cpf, telefone, valor, endereco_rua, endereco_numero,
                    endereco_bairro, endereco_cidade, endereco_estado,
                    endereco_cep, km, metodo_pagamento, pagamento,
                    criado_em, atualizado_em) VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s, %s)
                ''', (dados['nome'], dados['cpf'], dados['telefone'], dados['valor'],
                      dados['endereco_rua'], dados['endereco_numero'],
                      dados['endereco_bairro'], dados['endereco_cidade'],
                      dados['endereco_estado'], dados['endereco_cep'], dados['km'],
                      dados['metodo_pagamento'], dados['pagamento'], agora, agora))

            novo_id = cursor.lastrowid
            logger.info(f'Passageiro {novo_id} adicionado com sucesso.')
            return jsonify({'mensagem': 'Passageiro adicionado com suceso!',
                            'id': novo_id}), 201

    except Exception as erro:
        logger.error(f'Erro inesperado ao adicionar passageiro: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao adicionar passageiro!'}), 500


@app1.route('/passageiros/<int:id>', methods=['PUT'])
@rota_protegida
def atualizar_passageiro(id):
    try:
        logger.info(f'Atualizando passageiro com id={id}...')
        dados = validar_json()

        REGRAS = {
            'nome': lambda v: isinstance(v, str) and v.strip() != '',
            'cpf': lambda v: isinstance(
                v, str) and re.fullmatch(r'\d{3}\.?\d{3}\.?\d{3}-?\d{2}', v) is not None,
            'telefone': lambda v: isinstance(
                v, str) and re.fullmatch(r'\(?\d{2}\)?\s?\d{4,5}-?\d{4}', v) is not None,
            'valor': lambda v: isinstance(v, Decimal) and v > 0,
            'endereco_rua': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_numero': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_bairro': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_cidade': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_estado': lambda v: isinstance(v, str) and len(v.strip()) == 2,
            'endereco_cep': lambda v: isinstance(
                v, str) and re.fullmatch(r'\d{5}-?\d{3}', v) is not None,
            'km': lambda v: isinstance(v, Decimal) and v > 0,
            'metodo_pagamento': lambda v: isinstance(
                v, str) and v.strip().lower() in ('pix', 'credito', 'debito', 'boleto'),
            'pagamento': lambda v: isinstance(
                v, str) and v.strip().lower() in ('pago', 'cancelado', 'pendente')
        }

        enviados = {k: v for k, v in dados.items(
        ) if k in REGRAS and v is not None}

        if not enviados:
            logger.warning('Nenhum campo enviado para a atualização.')
            return jsonify({'erro': 'Envie ao menos um campo para atualizar!'}), 400

        for campo, valor in enviados.items():
            try:
                valor = dados[campo]

                if campo in ('cpf', 'telefone', 'endereco_rua',
                             'endereco_numero', 'endereco_bairro',
                             'endereco_cep'):
                    valor = str(valor).strip()
                
                elif campo in ('nome', 'endereco_cidade'):
                    valor = str(valor).strip().title()
                
                elif campo == 'endereco_estado':
                    valor = str(valor).strip().upper()
                
                elif campo in ('pagamento', 'metodo_pagamento'):
                    valor = str(valor).strip().lower()

                elif campo in ('valor', 'km'):
                    try:
                        valor = Decimal(str(valor)).quantize(Decimal('0.01'))
                    except InvalidOperation:
                        raise ValueError

                if not REGRAS[campo](valor):
                    raise ValueError

                enviados[campo] = valor
            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        br = timezone(timedelta(hours=-3))
        enviados['atualizado_em'] = datetime.now(br).isoformat()

        set_sql = ", ".join(f"{campo} = %s" for campo in enviados.keys())
        valores = list(enviados.values())
        valores.append(id)

        query = f"UPDATE passageiros SET {set_sql} WHERE id = %s"
        with conexao() as cursor:
            cursor.execute(query, valores)

            if cursor.rowcount == 0:
                logger.warning(f'Passageiro id={id} não encontrado.')
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404

            logger.info(f'Passageiro id={id} atualizado com sucesso.')
            return jsonify({'mensagem': 'Passageiro atualizado!',
                            'atualizado': enviados}), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao atualizar passageiro: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao atualizar passageiro!'}), 500


@app1.route('/passageiros/<int:id>', methods=['DELETE'])
@rota_protegida
def deletar_passageiro(id):
    try:
        logger.info(f'Deletando passageiro com id={id}...')
        with conexao() as cursor:
            cursor.execute('DELETE FROM passageiros WHERE id = %s', (id,))
            if cursor.rowcount == 0:
                logger.warning(f'Passageiro {id} não encontrado.')
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404

            logger.info('Recurso deletado com sucesso.')
            return '', 204
    except Exception as erro:
        logger.error(f'Erro inesperado ao deletar passageiro: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao deletar passageiro!'}), 500


register_erro_handlers(app1)


app2 = Flask('API2')


@app2.route('/motoristas', methods=['GET'])
@rota_protegida
def listar_motoristas():
    try:
        logger.info('Listando motoristas...')
        with conexao() as cursor:
            cursor.execute('SELECT * FROM motoristas')
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
@rota_protegida
def buscar_motorista(id):
    try:
        logger.info(f'Buscando motorista com id={id}...')
        with conexao() as cursor:
            cursor.execute('SELECT * FROM motoristas WHERE id = %s', (id,))
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
@rota_protegida
def adicionar_motorista():
    try:
        logger.info('Adicionando motorista...')

        dados = validar_json()

        REGRAS = {
            'nome': lambda v: isinstance(v, str) and v.strip() != '',
            'cnh': lambda v: isinstance(v, str) and len(v.strip()) == 11,
            'telefone': lambda v: isinstance(v, str) and len(v.strip()) >= 8,
            'categoria_cnh': lambda v: isinstance(
                v, str) and v.strip().upper() in ('A', 'B', 'C', 'D', 'E'),
            'placa': lambda v: isinstance(v, str) and re.fullmatch(
                r'[A-Z]{3}-?\d{4}|[A-Z]{3}\d[A-Z]\d{2}', v.upper()) is not None,
            'modelo_carro': lambda v: isinstance(v, str) and v.strip() != '',
            'ano_carro': lambda v: isinstance(v, int) and v >= 1980
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = dados[campo]

                if campo in ('cnh', 'telefone', 'placa', 'modelo_carro'):
                    valor = str(valor).strip()

                elif campo == 'nome':
                    valor = str(valor).strip().title()
                
                elif campo == 'categoria_cnh':
                    valor = str(valor).strip().upper()

                elif campo == 'ano_carro':
                    valor = int(valor)

                if not regra(valor):
                    raise ValueError

                dados[campo] = valor

            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        br = timezone(timedelta(hours=-3))
        agora = datetime.now(br).isoformat()

        with conexao() as cursor:
            cursor.execute('''
                INSERT INTO motoristas (nome, cnh, telefone, categoria_cnh,
                     placa, modelo_carro, ano_carro, criado_em, atualizado_em)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                           (dados['nome'], dados['cnh'], dados['telefone'],
                            dados['categoria_cnh'], dados['placa'], dados['modelo_carro'],
                               dados['ano_carro'], agora, agora))

            novo_id = cursor.lastrowid
            logger.info(f'Motorista id={novo_id} adicionado com sucesso.')
            return jsonify({'mensagem': 'Motorista adicionado!',
                            'id': novo_id}), 201

    except Exception as erro:
        logger.error(f'Erro inesperado ao adicionar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao adicionar motorista!'}), 500


@app2.route('/motoristas/<int:id>', methods=['PUT'])
@rota_protegida
def atualizar_motorista(id):
    try:
        logger.info(f'Atualizando motorista com id={id}...')

        dados = validar_json()

        REGRAS = {
            'nome': lambda v: isinstance(v, str) and v.strip() != '',
            'cnh': lambda v: isinstance(v, str) and len(v.strip()) == 11,
            'telefone': lambda v: isinstance(v, str) and len(v.strip()) >= 8,
            'categoria_cnh': lambda v: isinstance(
                v, str) and v.strip().upper() in ('A', 'B', 'C', 'D', 'E'),
            'placa': lambda v: isinstance(v, str) and re.fullmatch(
                r'[A-Z]{3}-?\d{4}|[A-Z]{3}\d[A-Z]\d{2}', v.upper()) is not None,
            'modelo_carro': lambda v: isinstance(v, str) and v.strip() != '',
            'ano_carro': lambda v: isinstance(v, int) and v >= 1980,
            'status': lambda v: isinstance(
                v, str) and v.strip().lower() in ('ativo', 'suspenso', 'bloqueado'),
            'valor_passagem': lambda v: isinstance(v, Decimal) and v > 0,
            'quantia': lambda v: isinstance(v, Decimal) and v >= 0
        }

        enviados = {k: v for k, v in dados.items(
        ) if k in REGRAS and v is not None}

        if not enviados:
            logger.warning('Nenhum campo enviado para a atualização.')
            return jsonify({'erro': 'Envie ao menos um campo para atualizar!'}), 400

        for campo, valor in enviados.items():
            try:
                valor = dados[campo]

                if campo in ('cnh', 'telefone', 'placa', 'modelo_carro'):
                    valor = str(valor).strip()

                elif campo == 'nome':
                    valor = str(valor).strip().title()
                
                elif campo == 'categoria_cnh':
                    valor = str(valor).strip().upper()
                
                elif campo == 'status':
                    valor = str(valor).strip().lower()

                elif campo in ('valor_passagem', 'quantia'):
                    valor = Decimal(str(valor)).quantize(Decimal('0.01'))

                elif campo == 'ano_carro':
                    valor = int(valor)

                if not REGRAS[campo](valor):
                    raise ValueError

                dados[campo] = valor
            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        br = timezone(timedelta(hours=-3))
        enviados['atualizado_em'] = datetime.now(br).isoformat()

        set_sql = ", ".join(f"{campo} = %s" for campo in enviados.keys())
        valores = list(enviados.values())
        valores.append(id)

        query = f"UPDATE motoristas SET {set_sql} WHERE id = %s"
        with conexao() as cursor:
            cursor.execute(query, valores)

            if cursor.rowcount == 0:
                logger.warning(f'Motorista id={id} não encontrado.')
                return jsonify({'erro': 'Motorista não encontrado!'}), 404

            logger.info(f'Motorista id={id} atualizado com sucesso.')
            return jsonify({'mensagem': 'Motorista atualizado!',
                            'atualizado': enviados}), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao atualizar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao atualizar motorista!'}), 500


@app2.route('/motoristas/<int:id>', methods=['DELETE'])
@rota_protegida
def deletar_motorista(id):
    try:
        logger.info(f'Deletando motorista com id={id}...')
        with conexao() as cursor:
            cursor.execute('DELETE FROM motoristas WHERE id = %s', (id,))

            if cursor.rowcount == 0:
                logger.warning(f'Motorista id={id} não encontrado.')
                return jsonify({'erro': 'Motorista não encontrado!'}), 404

            logger.info('Recurso deletado com sucesso.')
            return '', 204
    except Exception as erro:
        logger.error(f'Erro inesperado ao deletar motorista: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao deletar motorista!'}), 500


register_erro_handlers(app2)


app3 = Flask('API3')


@app3.route('/viagens', methods=['GET'])
@rota_protegida
def listar_viagens():
    try:
        logger.info('Listando viagens...')
        with conexao() as cursor:
            cursor.execute('SELECT * FROM viagens')
            dados = [{
                'id': v[0],
                'id_passageiro': v[1],
                'id_motorista': v[2],
                'nome_passageiro': v[3],
                'nome_motorista': v[4],
                'endereco_rua': v[5],
                'endereco_numero': v[6],
                'endereco_bairro':  v[7],
                'endereco_cidade': v[8],
                'endereco_estado': v[9],
                'endereco_cep': v[10],
                'valor_por_km': v[11],
                'total_viagem': v[12],
                'metodo_pagamento': v[13],
                'pagamento': v[14],
                'status': v[15],
                'criado_em': v[16],
                'atualizado_em': v[17]
            } for v in cursor.fetchall()]

            if not dados:
                logger.warning(f'Nenhum viagem cadastrada ainda.')
                return jsonify([]), 200

            logger.info(f'Listagem de viagens bem-sucedida.')
            return jsonify(dados), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao listar viagens: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao listar viagens!'}), 500


@app3.route('/viagens/<int:id>', methods=['GET'])
@rota_protegida
def buscar_viagem(id):
    try:
        logger.info(f'Buscando viagem com id={id}...')
        with conexao() as cursor:
            cursor.execute('SELECT * FROM viagens WHERE id = %s', (id,))
            dado = cursor.fetchone()

            if not dado:
                logger.warning(f'Viagem id={id} não encontrada.')
                return jsonify({'erro': 'Viagem não encontrado!'}), 404

            logger.info(f'Busca de viagem bem-sucedida.')
            return jsonify({
                'id': dado[0],
                'id_passageiro': dado[1],
                'id_motorista': dado[2],
                'nome_passageiro': dado[3],
                'nome_motorista': dado[4],
                'endereco_rua': dado[5],
                'endereco_numero': dado[6],
                'endereco_bairro': dado[7],
                'endereco_cidade': dado[8],
                'endereco_estado': dado[9],
                'endereco_cep': dado[10],
                'valor_por_km': dado[11],
                'total_viagem': dado[12],
                'metodo_pagamento': dado[13],
                'pagamento': dado[14],
                'status': dado[15],
                'criado_em': dado[16],
                'atualizado_em': dado[17]
            }), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao buscar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao buscar viagem!'}), 500


@app3.route('/viagens', methods=['POST'])
@rota_protegida
def adicionar_viagem():
    try:
        logger.info('Adicionando viagem...')

        dados = validar_json()

        REGRAS = {
            'id_passageiro': lambda v: isinstance(v, int) and v > 0,
            'id_motorista': lambda v: isinstance(v, int) and v > 0
        }

        faltando = [c for c in REGRAS if c not in dados or dados[c] is None]

        if faltando:
            logger.warning(f"Campos obrigatórios: {', '.join(faltando)}")
            return jsonify({'erro': f"Campos obrigatórios: {', '.join(faltando)}"}), 400

        for campo, regra in REGRAS.items():
            try:
                valor = int(dados[campo])

                if not regra(valor):
                    raise ValueError

                dados[campo] = valor

            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        with conexao() as cursor:
            cursor.execute('SELECT valor FROM passageiros WHERE id = %s FOR UPDATE',
                           (dados['id_passageiro'],))

            saldo = cursor.fetchone()
            if not saldo:
                logger.warning(
                    f"Passageiro {dados['id_passageiro']} não encontrado.")
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404

            cursor.execute('SELECT * FROM motoristas WHERE id = %s',
                           (dados['id_motorista'],))

            if not cursor.fetchone():
                logger.warning(
                    f"Motorista {dados['id_motorista']} não encontrado.")
                return jsonify({'erro': 'Motorista não encontrado!'}), 404

            token = request.headers.get('Authorization')

            try:
                resp_passa = requests.get(
                    f"http://127.0.0.1:5001/passageiros/{dados['id_passageiro']}",
                    timeout=3, headers={'Authorization': token})
                resp_passa.raise_for_status()

                resp_moto = requests.get(
                    f"http://127.0.0.1:5002/motoristas/{dados['id_motorista']}",
                    timeout=3, headers={'Authorization': token})
                resp_moto.raise_for_status()
            except Exception as erro:
                return tratamento_erro_requests(erro)

            try:
                passa_obj = resp_passa.json()
                moto_obj = resp_moto.json()
            except ValueError as erro_json:
                logger.warning(
                    f'Erro ao decodificar API externa: {str(erro_json)}')
                return jsonify({'erro': 'Erro ao decodificar API externa!'}), 400

            passa_payload = passa_obj[0] if isinstance(
                passa_obj, list) else passa_obj

            moto_payload = moto_obj[0] if isinstance(
                moto_obj, list) else moto_obj

            if not isinstance(passa_payload, dict):
                logger.warning(
                    f'Formato inválido de passageiro: {passa_payload}')
                return jsonify({'erro': 'Formato inválido de passageiro!'}), 502

            if not isinstance(moto_payload, dict):
                logger.warning(
                    f'Formato inválido de motorista: {moto_payload}')
                return jsonify({'erro': 'Formato inválido de motorista!'}), 502

            br = timezone(timedelta(hours=-3))
            agora = datetime.now(br).isoformat()

            campos_obrigatorio_passageiro = ['nome', 'endereco_rua', 'endereco_numero',
                                             'endereco_bairro', 'endereco_cidade',
                                             'endereco_estado', 'endereco_cep', 'km',
                                             'metodo_pagamento', 'pagamento']

            for campo in campos_obrigatorio_passageiro:
                valor = passa_payload.get(campo)
                if valor is None or str(valor).strip() == '':
                    logger.warning(f'Campo obrigatório vázio: {campo}')
                    return jsonify({'erro': f'Campo obrigatório vázio: {campo}'}), 400

            campos_obrigatorios_motorista = [
                'nome', 'valor_passagem', 'status']

            for campo in campos_obrigatorios_motorista:
                valor = moto_payload.get(campo)
                if valor is None or str(valor).strip() == '':
                    logger.warning(f'Campo obrigatório vázio: {campo}')
                    return jsonify({'erro': f'Campo obrigatório vázio: {campo}'}), 400

            try:
                nome_passageiro = str(passa_payload.get('nome')).strip().title()
                nome_motorista = str(moto_payload.get('nome')).strip().title()
                endereco_rua = str(passa_payload.get('endereco_rua')).strip()
                endereco_numero = str(passa_payload.get('endereco_numero')).strip()
                endereco_bairro = str(passa_payload.get('endereco_bairro')).strip()
                endereco_cidade = str(passa_payload.get('endereco_cidade')).strip().title()
                endereco_estado = str(passa_payload.get('endereco_estado')).strip().upper()
                endereco_cep = str(passa_payload.get('endereco_cep')).strip()
                valor_por_km = Decimal(str(moto_payload.get('valor_passagem'))
                                    ).quantize(Decimal('0.01'))
                km = Decimal(str(passa_payload.get('km'))).quantize(Decimal('0.01'))
                metodo_pagamento = str(passa_payload.get('metodo_pagamento')).strip().lower()
                pagamento = str(passa_payload.get('pagamento')).strip().lower()
                status_moto = str(moto_payload.get('status')).strip().lower()
            except (ValueError, TypeError, InvalidOperation) as erro:
                logger.warning(f'Erro ao extrair campo da API: {str(erro)}')
                return jsonify({'erro': 'Erro ao extrair campo da API!'}), 400

            if valor_por_km <= 0:
                logger.warning(f'Valor da passagem não pode ser negativo.')
                return jsonify({'erro': 'Valor da passagem não pode ser negativo!'}), 400

            if km <= 0:
                logger.warning(f'Km deve ser positivo.')
                return jsonify({'erro': 'Km deve ser positivo!'}), 400

            if status_moto != 'ativo':
                logger.warning(
                    f'Motorista suspenso ou bloqueado não pode fazer viagens.')
                return jsonify(
                    {'erro': 'Motorista suspenso ou bloqueado não pode fazer viagens!'}), 400

            if pagamento in ('cancelado', 'pendente'):
                logger.warning(f'Pagamento cancelado ou pendente')
                return jsonify({
                    'erro': 'Pagamento cancelado ou pendente para'
                    ' confirmar a viagem atualize para pago'
                }), 400

            total_viagem = (valor_por_km * km).quantize(Decimal('0.01'))

            if saldo[0] < total_viagem:
                logger.warning('Saldo insuficiente.')
                return jsonify({'erro': 'Saldo insuficiente!'}), 400

            cursor.execute('UPDATE passageiros SET valor = valor - %s WHERE id = %s',
                           (total_viagem, dados['id_passageiro']))

            cursor.execute('UPDATE motoristas SET quantia = quantia + %s WHERE id = %s',
                           (total_viagem, dados['id_motorista']))

            cursor.execute('''
                    INSERT INTO viagens 
                        (id_passageiro, id_motorista, nome_passageiro,
                        nome_motorista, endereco_rua, endereco_numero, endereco_bairro,
                        endereco_cidade, endereco_estado, endereco_cep, valor_por_km,
                        total_viagem, metodo_pagamento, pagamento, criado_em,
                        atualizado_em) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (dados['id_passageiro'], dados['id_motorista'], nome_passageiro,
                          nome_motorista, endereco_rua, endereco_numero, endereco_bairro,
                          endereco_cidade, endereco_estado, endereco_cep, valor_por_km,
                          total_viagem, metodo_pagamento, pagamento, agora, agora))
            
            novo_id = cursor.lastrowid

            logger.info(f'Viagem id={novo_id} adicionada com sucesso.')
            return jsonify({
                'mensagem': 'Viagem adicionada com sucesso!',
                'id': novo_id
                }), 201

    except Exception as erro:
        logger.error(f'Erro inesperado ao adicionar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao adicionar viagem!'}), 500


@app3.route('/viagens/<int:id>', methods=['PUT'])
@rota_protegida
def atualizar_viagens(id):
    try:
        logger.info(f'Atualizando viagem com id={id}...')

        dados = validar_json()

        REGRAS = {
            'nome_passageiro': lambda v: isinstance(v, str) and v.strip() != '',
            'nome_motorista': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_rua': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_numero': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_bairro': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_cidade': lambda v: isinstance(v, str) and v.strip() != '',
            'endereco_estado': lambda v: isinstance(v, str) and len(v.strip()) == 2,
            'endereco_cep': lambda v: isinstance(
                v, str) and re.fullmatch(r'\d{5}-?\d{3}', v) is not None,
            'valor_por_km': lambda v: isinstance(v, Decimal) and v > 0,
            'total_viagem': lambda v: isinstance(v, Decimal) and v > 0,
            'metodo_pagamento': lambda v: isinstance(
                v, str) and v.strip().lower() in ('pix', 'credito', 'debito', 'boleto'),
            'pagamento': lambda v: isinstance(
                v, str) and v.strip().lower() in ('pago', 'cancelado', 'pendente'),
            'status': lambda v: isinstance(
                v, str) and v.strip().lower() in ('confirmada', 'cancelada')
        }

        enviados = {k: v for k, v in dados.items() if k in REGRAS and v is not None}

        if not enviados:
            logger.warning('Nenhum campo enviado para a atualização.')
            return jsonify({'erro': 'Envie ao menos um campo para a atualizar!'}), 400

        for campo, valor in enviados.items():
            try:
                if campo in ('endereco_rua','endereco_numero',
                             'endereco_bairro', 'endereco_cep'):
                    valor = str(valor).strip()
                
                elif campo in ('nome_passageiro', 'nome_motorista', 'endereco_cidade'):
                    valor = str(valor).strip().title()

                elif campo in ('metodo_pagamento', 'pagamento', 'status'):
                    valor = str(valor).strip().lower()
                
                elif campo == 'endereco_estado':
                    valor = str(valor).strip().upper()

                elif campo in ('valor_por_km', 'total_viagem'):
                    try:
                        valor = Decimal(str(valor)).quantize(Decimal('0.01'))
                    except InvalidOperation:
                        raise ValueError

                if not REGRAS[campo](valor):
                    raise ValueError

                enviados[campo] = valor
            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        with conexao() as cursor:
            cursor.execute(
                '''SELECT id_passageiro, id_motorista, total_viagem, status, pagamento
                  FROM viagens WHERE id = %s FOR UPDATE''',
                  (id,))
            viagem = cursor.fetchone()

            if not viagem:
                logger.warning(f'Viagem id={id} não encontrado.')
                return jsonify({'erro': 'Viagem não encontrada!'}), 404

            if viagem[3] == 'cancelada' or viagem[4] == 'cancelado':
                cursor.execute(
                    'SELECT valor FROM passageiros WHERE id = %s FOR UPDATE',
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

                cursor.execute(
                    'UPDATE motoristas SET quantia = quantia - %s WHERE id = %s',
                               (viagem[2], viagem[1]))

                cursor.execute(
                    'UPDATE passageiros SET valor = valor + %s WHERE id = %s',
                               (viagem[2], viagem[0]))
                
                cursor.execute('DELETE FROM viagens WHERE id = %s', (id,))

                logger.info('Estorno realizado com sucesso!')
                return jsonify({'erro': 'Viagem cancelada! Estorno bem-sucedido.!'}), 200
            
            br = timezone(timedelta(hours=-3))
            enviados['atualizado_em'] = datetime.now(br).isoformat()

            set_sql = ", ".join(f"{campo} = %s" for campo in enviados.keys())
            valores = list(enviados.values())
            valores.append(id)

            query = f"UPDATE viagens SET {set_sql} WHERE id = %s"
            cursor.execute(query, valores)

            logger.info(f'Viagem id={id} atualizada com sucesso.')
            return jsonify({
                'mensagem': 'Viagem atualizada com sucesso',
                'atualizado': enviados
            }), 200

    except Exception as erro:
        logger.error(f'Erro inesperado ao atualizar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao atualizar viagem!'}), 500


@app3.route('/viagens/<int:id>', methods=['DELETE'])
@rota_protegida
def deletar_viagem(id):
    try:
        logger.info(f'Deletando viagem com id={id}...')
        with conexao() as cursor:
            cursor.execute('''
                    SELECT id_passageiro, id_motorista,
                    total_viagem FROM viagens WHERE id = %s
                    FOR UPDATE''', (id,))
            viagem = cursor.fetchone()

            if not viagem:
                logger.warning(f'Viagem id={id} não encontrada.')
                return jsonify({'erro': 'Viagem não encontrada!'}), 404

            cursor.execute('SELECT valor FROM passageiros WHERE id = %s FOR UPDATE',
                           (viagem[0],))
            valor = cursor.fetchone()

            if not valor:
                logger.warning(f'Passageiro id={viagem[0]} não encontrado.')
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404

            cursor.execute('SELECT quantia FROM motoristas WHERE id = %s FOR UPDATE',
                           (viagem[1],))
            quantia = cursor.fetchone()

            if not quantia:
                logger.warning(f'Motorista id={viagem[1]} não encontrado.')
                return jsonify({'erro': 'Motorista não encontrado!'}), 404

            if quantia[0] < viagem[2]:
                logger.warning('Incosistência financeira detectada.')
                return jsonify({'erro': 'Incosistência financeira!'}), 409

            cursor.execute('UPDATE motoristas SET quantia = quantia - %s WHERE id = %s',
                           (viagem[2], viagem[1]))
            
            cursor.execute('UPDATE passageiros SET valor = valor + %s WHERE id = %s',
                           (viagem[2], viagem[0]))

            cursor.execute('DELETE FROM viagens WHERE id = %s', (id,))

            logger.info('Recurso deletado com sucesso.')
            return '', 204

    except Exception as erro:
        logger.error(f'Erro inesperado ao deletar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao deletar viagem!'}), 500


app4 = Flask('API4')


@app4.route('/registros-pagamento', methods=['GET'])
@rota_protegida
def listar_registros_pagamento():
    try:
        logger.info('Listando registros de pagamentos...')
        with conexao() as cursor:
            cursor.execute('SELECT * FROM registros_pagamento')
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
@rota_protegida
def buscar_registro_pagamento(id):
    try:
        logger.info(f'Buscando registro de pagamento com id={id}...')
        with conexao() as cursor:
            cursor.execute('SELECT * FROM registros_pagamento WHERE id = %s', (id,))
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
            cursor.execute(
                'SELECT * FROM viagens WHERE id = %s FOR UPDATE',
                (dados['id_viagem'],))

            viagem = cursor.fetchone()

            if not viagem:
                logger.warning(
                    f"Viagem id={dados['id_viagem']} não encontrada.")
                return jsonify({'erro': 'Viagem não encontrada!'}), 404

            token = request.headers.get('Authorization')

            try:
                resp_viagem = requests.get(
                    f"http://127.0.0.1:5003/viagens/{dados['id_viagem']}",
                    timeout=3, headers={'Authorization': token})
                resp_viagem.raise_for_status()
            except Exception as erro:
                return tratamento_erro_requests(erro)

            try:
                viagem_obj = resp_viagem.json()
            except ValueError as erro_json:
                logger.warning(
                    f'Erro ao decodificar API externa: {str(erro_json)}')
                return jsonify({'erro': 'Erro ao decodificar API externa!'}), 400

            viagem_payload = viagem_obj[0] if isinstance(
                viagem_obj, list) else viagem_obj

            if not isinstance(viagem_payload, dict):
                logger.warning(f'Formato inválido de registros de pagamentos.')
                return jsonify(
                    {'erro': 'Requisição da API externa não é um dicionário!'}), 400

            campos_obrigatorios = ['nome_passageiro', 'nome_motorista', 'metodo_pagamento',
                                   'pagamento', 'status', 'total_viagem']

            for campo in campos_obrigatorios:
                valor = viagem_payload.get(campo)
                if valor is None or str(valor).strip() == '':
                    logger.warning(f"Campo obrigatório vázio: {campo}")
                    return jsonify({'erro': f"Campo obrigatório vázio: {campo}"}), 400

            br = timezone(timedelta(hours=-3))
            agora = datetime.now(br).isoformat()

            try:
                remetente = str(viagem_payload.get('nome_passageiro')).strip().title()
                recebedor = str(viagem_payload.get('nome_motorista')).strip().title()
                metodo_pagamento = str(
                    viagem_payload.get('metodo_pagamento')).strip().lower()
                pagamento = str(viagem_payload.get('pagamento')).strip().lower()
                status_viagem = str(
                    viagem_payload.get('status')).strip().lower()
                valor_viagem = Decimal(
                    str(viagem_payload.get('total_viagem'))).quantize(Decimal('0.01'))
            except (ValueError, TypeError, InvalidOperation) as erro:
                logger.warning(
                    f'Erro ao extrair campos da API externa: {str(erro)}')
                return jsonify({'erro': 'Erro ao extrair campos da API externa!'}), 400

            if pagamento in ('cancelado', 'pendente'):
                logger.warning('Pagamento cancelado ou pendente.')
                return jsonify(
                    {'erro': 'Pagamento cancelado ou pendente não pode ser registrado!'}), 400

            if status_viagem == 'cancelada':
                logger.warning(f'Viagem cancelada não pode ser registrada.')
                return jsonify({'erro': 'Viagem cancelada não pode ser registrada!'}), 400

            if valor_viagem <= 0:
                logger.warning(f'Valor da viagem negativa.')
                return jsonify({'erro': 'Valor da viagem não pode ser negativo!'}), 400

            cursor.execute('''
                    INSERT INTO registros_pagamento
                        (id_viagem, remetente, recebedor, metodo_pagamento,
                         pagamento, valor_viagem, criado_em, atualizado_em
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (dados['id_viagem'], remetente, recebedor, metodo_pagamento,
                          pagamento, valor_viagem, agora, agora))

            novo_id = cursor.lastrowid
            logger.info(
                f'Registro de pagamento id={novo_id} adicionado com sucesso.')
            return jsonify(
                {'mensagem': 'Registro de pagamento adicionado com sucesso!'}), 201

    except Exception as erro:
        logger.error(
            f'Erro inesperado ao adicionar registro de pagamento: {str(erro)}')
        return jsonify(
            {'erro': 'Erro inesperado ao adicionar registro de pagamento!'}), 500


@app4.route('/registros-pagamento/<int:id>', methods=['PUT'])
@rota_protegida
def atualizar_registro_pagamento(id):
    try:
        logger.info(f'Atualizando registro de pagamento com id={id}...')

        dados = validar_json()

        REGRAS = {
            'remetente': lambda v: isinstance(v, str) and v.strip() != '',
            'recebedor': lambda v: isinstance(v, str) and v.strip() != '',
            'metodo_pagamento': lambda v: isinstance(
                v, str) and v.strip().lower() in ('pix', 'credito', 'debito', 'boleto'),
            'pagamento': lambda v: isinstance(
                v, str) and v.strip().lower() in ('pago', 'cancelado', 'pendente'),
            'valor_viagem': lambda v: isinstance(v, Decimal) and v > 0
        }

        enviados = {k: v for k, v in dados.items() if k in REGRAS and v is not None}

        if not enviados:
            logger.warning('Nenhum campo enviado para a atualização.')
            return jsonify({'erro': 'Envie ao menos um campo para atualizar!'}), 400

        for campo, valor in enviados.items():
            try:
                valor = dados[campo]

                if campo in ('remetente', 'recebedor'):
                    valor = str(valor).strip().title()
                
                elif campo in ('metodo_pagamento', 'pagamento'):
                    valor = str(valor).strip().lower()

                elif campo == 'valor_viagem':
                    valor = Decimal(valor).quantize(Decimal('0.01'))

                if not REGRAS[campo](valor):
                    raise ValueError

                enviados[campo] = valor
            except Exception:
                logger.warning(
                    f'Valor inválido para {campo}: {dados.get(campo)}')
                return jsonify({'erro': f'Valor inválido para {campo}!'}), 400

        br = timezone(timedelta(hours=-3))
        enviados['atualizado_em'] = datetime.now(br).isoformat()

        with conexao() as cursor:
            cursor.execute('''
                    SELECT id_viagem FROM registros_pagamento
                    WHERE id = %s''', (id,))

            registro = cursor.fetchone()

            if not registro:
                logger.warning(
                    f'Registro de pagamento id={id} não encontrado.')
                return jsonify({'erro': 'Registro de pagamento não encontrado!'}), 404

            cursor.execute('''
                    SELECT id_passageiro, id_motorista,
                    total_viagem, status, pagamento FROM viagens
                    WHERE id = %s FOR UPDATE''',
                           (registro[0],))

            viagem = cursor.fetchone()

            if not viagem:
                logger.warning(f'Viagem id={registro[0]} não encontrado.')
                return jsonify({'erro': 'Viagem não encontrado!'}), 404

            if viagem[3] == 'cancelada' or viagem[4] == 'cancelado':
                cursor.execute('''
                        UPDATE registros_pagamento SET
                        pagamento = 'cancelado', status = 'cancelado' WHERE id = %s''',
                        (id,))

                cursor.execute('SELECT valor FROM passageiros WHERE id = %s FOR UPDATE',
                               (viagem[0],))

                passa = cursor.fetchone()

                if not passa:
                    logger.warning(
                        f'Passageiro id={viagem[0]} não encontrado.')
                    return jsonify({'erro': 'Passageiro não encontrado!'}), 404

                cursor.execute('SELECT quantia FROM motoristas WHERE id = %s FOR UPDATE',
                               (viagem[1],))

                moto = cursor.fetchone()

                if not moto:
                    logger.warning(f'Motorista id={viagem[1]} não encontrado.')
                    return jsonify({'erro': 'Motorista não encontrado!'}), 404

                cursor.execute('UPDATE motoristas SET quantia = quantia - %s WHERE id = %s',
                               (viagem[2], viagem[1]))

                cursor.execute('UPDATE passageiros SET valor = valor + %s WHERE id = %s',
                               (viagem[2], viagem[0]))

                cursor.execute('DELETE FROM viagens WHERE id = %s', (registro[0],))

                logger.info(f'Registro de pagamento id={id} cancelado.')
                return jsonify(
                    {'mensagem': 'Registro de pagamento cancelado com sucesso!'}), 200

            if viagem[4] == 'pendente':
                logger.warning(f'Pagamento pendente para registro.')
                return jsonify(
                    {'erro': 'Pagamento pendente não pode ser registrado!'}), 400

            set_sql = ", ".join(f"{campo} = %s" for campo in enviados.keys())
            valores = list(enviados.values())
            valores.append(id)

            query = f'UPDATE registros_pagamento SET {set_sql} WHERE id = %s'
            cursor.execute(query, valores)

            logger.info(f'Registro de pagamento id={id} atualizado com sucesso.')
            return jsonify({
                'mensagem': 'Registro de pagamento',
                'atualizado': enviados
            }), 200

    except Exception as erro:
        logger.error(
            f'Erro inesperado ao atualizar registro de pagamento: {str(erro)}')
        return jsonify(
            {'erro': 'Erro inesperado ao atualizar registro de pagamento!'}), 500


@app4.route('/registros-pagamento/<int:id>', methods=['DELETE'])
@rota_protegida
def deletar_registro_pagamento(id):
    try:
        logger.info(f'Deletando registro de pagamento com id={id}...')
        with conexao() as cursor:
            cursor.execute(
                'DELETE FROM registros_pagamento WHERE id = %s', (id,))
            
            if cursor.rowcount == 0:
                logger.warning(f'Registro de pagamento id={id} não encontrado.')
                return jsonify({'erro': 'Registro de pagamento não encontrado!'}), 404

            logger.info('Recurso deletado com sucesso.')
            return '', 204

    except Exception as erro:
        logger.error(f'Erro inesperado ao deletar registro de pagamento: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao deletar registro de pagamento!'}), 500


register_erro_handlers(app4)


def start_api(app, port):
    app.run(debug=True, port=port, use_reloader=False)


if __name__ == '__main__':
    logger.info('Iniciando APIs...')
    apis = [(app1, 5001),
            (app2, 5002),
            (app3, 5003),
            (app4, 5004)]

    for app, port in apis:
        threading.Thread(target=start_api,
                          args=(app, port),
                          daemon=True).start()
