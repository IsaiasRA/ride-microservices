from flask import Blueprint, jsonify, request
from app1.auth import (rota_protegida, gerar_tokens,
                       validar_token, salvar_refresh,
                       refresh_valido, revogar_todos_refresh,
                       revogar_refresh, criar_usuario)
from app1.database import conexao
from app1.validation import validar_json
from app1.log import configurar_logging
from app1.brute_force import (ip_bloqueado, registrar_falha,
                               limpar_falhas, limiter)
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import logging
import bcrypt
import re


configurar_logging()
logger = logging.getLogger(__name__)


passageiros_bp = Blueprint('passageiros', __name__)


@passageiros_bp.route('/', methods=['GET'])
@limiter.limit('100 per hour')
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


@passageiros_bp.route('/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
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


@passageiros_bp.route('/register', methods=['POST'])
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


@passageiros_bp.route('/login', methods=['POST'])
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
                logger.warning('Senha inválida.')
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


@passageiros_bp.route('/refresh', methods=['POST'])
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


@passageiros_bp.route('/logout', methods=['POST'])
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


@passageiros_bp.route('/', methods=['POST'])
@limiter.limit('100 per hour')
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
            logger.info(f'Passageiro id={novo_id} adicionado com sucesso.')
            return jsonify({'mensagem': 'Passageiro adicionado com suceso!',
                            'id': novo_id}), 201

    except Exception as erro:
        logger.error(f'Erro inesperado ao adicionar passageiro: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao adicionar passageiro!'}), 500


@passageiros_bp.route('/<int:id>', methods=['PUT'])
@limiter.limit('100 per hour')
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


@passageiros_bp.route('/<int:id>', methods=['DELETE'])
@limiter.limit('100 per hour')
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
