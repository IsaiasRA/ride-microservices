from flask import Blueprint, jsonify
from app.auth import rota_protegida
from app.database import conexao
from app.validation import validar_json, formatar_nome
from app.log import configurar_logging
from app.brute_force import limiter
from decimal import Decimal, InvalidOperation
import logging


configurar_logging()
logger = logging.getLogger(__name__)


registros_pagamento_bp = Blueprint('registros-pagamento', __name__)


@registros_pagamento_bp.route('/', methods=['GET'])
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


@registros_pagamento_bp.route('/<int:id>', methods=['GET'])
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


@registros_pagamento_bp.route('/', methods=['POST'])
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


@registros_pagamento_bp.route('/<int:id>/cancelar', methods=['PATCH'])
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
                return '', 204

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
                return jsonify(
                    {'erro': 'Viagem precisa estar cancelada para realizar estorno!'}), 400
           
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