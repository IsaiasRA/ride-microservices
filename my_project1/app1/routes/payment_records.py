from flask import Blueprint, jsonify, request
from app1.auth import rota_protegida
from app1.database import conexao
from app1.error import tratamento_erro_requests
from app1.validation import validar_json
from app1.log import configurar_logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import logging
import requests


configurar_logging()
logger = logging.getLogger(__name__)


registros_pagamento_bp = Blueprint('registros-pagamento', __name__)


@registros_pagamento_bp.route('/registros-pagamento', methods=['GET'])
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


@registros_pagamento_bp.route('/registros-pagamento/<int:id>', methods=['GET'])
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


@registros_pagamento_bp.route('/registros-pagamento', methods=['POST'])
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


@registros_pagamento_bp.route('/registros-pagamento/<int:id>', methods=['PUT'])
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


@registros_pagamento_bp.route('/registros-pagamento/<int:id>', methods=['DELETE'])
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
    