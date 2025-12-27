from flask import Blueprint, jsonify, request
from app1.auth import rota_protegida
from app1.database import conexao
from app1.error import tratamento_erro_requests
from app1.validation import validar_json
from app1.log import configurar_logging
from app1.brute_force import limiter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import logging
import requests
import re


configurar_logging()
logger = logging.getLogger(__name__)


viagens_bp = Blueprint('viagens', __name__)


@viagens_bp.route('/', methods=['GET'])
@limiter.limit('100 per hour')
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


@viagens_bp.route('/<int:id>', methods=['GET'])
@limiter.limit('100 per hour')
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


@viagens_bp.route('/', methods=['POST'])
@limiter.limit('100 per hour')
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


@viagens_bp.route('/<int:id>', methods=['PUT'])
@limiter.limit('100 per hour')
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


@viagens_bp.route('/<int:id>', methods=['DELETE'])
@limiter.limit('100 per hour')
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
    