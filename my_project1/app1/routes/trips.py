from flask import Blueprint, jsonify
from app1.auth import rota_protegida
from app1.database import conexao
from app1.validation import validar_json, formatar_nome
from app1.log import configurar_logging
from app1.brute_force import limiter
from decimal import Decimal, InvalidOperation
import logging


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
            cursor.execute('''
                SELECT id, id_passageiro, id_motorista, nome_passageiro,
                       nome_motorista, endereco_rua, endereco_numero,
                       endereco_bairro, endereco_cidade, endereco_estado,
                       endereco_cep, valor_por_km, total_viagem,
                       metodo_pagamento, status, criado_em,
                       atualizado_em
                    FROM viagens''')
            
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
                'status': v[14],
                'criado_em': v[15],
                'atualizado_em': v[16]
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
            cursor.execute('''
                SELECT id, id_passageiro, id_motorista, nome_passageiro,
                       nome_motorista, endereco_rua, endereco_numero,
                       endereco_bairro, endereco_cidade, endereco_estado,
                       endereco_cep, valor_por_km, total_viagem,
                       metodo_pagamento, status, criado_em,
                       atualizado_em
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
                'status': dado[14],
                'criado_em': dado[15],
                'atualizado_em': dado[16]
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
            cursor.execute('''
                SELECT nome_passageiro, saldo, endereco_rua, endereco_numero,
                       endereco_bairro, endereco_cidade, endereco_estado,
                       endereco_cep, km, metodo_pagamento
                    FROM passageiros WHERE id = %s FOR UPDATE''',
                           (dados['id_passageiro'],))

            passageiro = cursor.fetchone()
            if not passageiro:
                logger.warning(
                    f"Passageiro {dados['id_passageiro']} não encontrado.")
                return jsonify({'erro': 'Passageiro não encontrado!'}), 404

            cursor.execute('''
                SELECT nome_motorista, valor_passagem, status
                    FROM motoristas WHERE id = %s''',
                    (dados['id_motorista'],))
            
            motorista = cursor.fetchone()

            if not motorista:
                logger.warning(
                    f"Motorista {dados['id_motorista']} não encontrado.")
                return jsonify({'erro': 'Motorista não encontrado!'}), 404

            try:
                nome_passageiro = formatar_nome(passageiro[0])
                nome_motorista = formatar_nome(motorista[0])
                endereco_rua = str(passageiro[2]).strip()
                endereco_numero = str(passageiro[3])
                endereco_bairro = str(passageiro[4])
                endereco_cidade = formatar_nome(passageiro[5])
                endereco_estado = str(passageiro[6]).strip().upper()
                endereco_cep = str(passageiro[7]).strip()
                saldo = Decimal(str(passageiro[1])).quantize(Decimal('0.01'))
                km = Decimal(str(passageiro[8])).quantize(Decimal('0.01'))
                valor_passagem = Decimal(str(motorista[1])).quantize(Decimal('0.01'))
                metodo_pagamento = str(passageiro[9]).strip().lower()
                status_moto = str(motorista[2]).strip().lower()
            except (ValueError, TypeError, InvalidOperation) as erro:
                logger.warning(f'Erro ao coletar dados em banco: {str(erro)}')
                return jsonify({'erro': 'Erro ao coletar dados em banco SQL!'}), 400

            if valor_passagem <= 0:
                logger.warning(f'Valor da passagem não pode ser negativo.')
                return jsonify({'erro': 'Valor da passagem não pode ser negativo!'}), 400

            if km <= 0:
                logger.warning(f'Km deve ser positivo.')
                return jsonify({'erro': 'Km deve ser positivo!'}), 400

            if status_moto != 'ativo':
                logger.warning(
                    f'Motorista suspenso ou bloqueado não pode fazer viagens.')
                return jsonify(
                    {'erro': 'Motorista suspenso ou bloqueado não pode fazer viagens!'}), 409

            total_viagem = (valor_passagem * km).quantize(Decimal('0.01'))

            if saldo < total_viagem:
                logger.warning('Saldo insuficiente.')
                return jsonify({'erro': 'Saldo insuficiente!'}), 400

            cursor.execute('UPDATE passageiros SET saldo = saldo - %s WHERE id = %s',
                           (total_viagem, dados['id_passageiro']))

            cursor.execute('UPDATE motoristas SET quantia = quantia + %s WHERE id = %s',
                           (total_viagem, dados['id_motorista']))

            cursor.execute('''
                    INSERT INTO viagens 
                        (id_passageiro, id_motorista, nome_passageiro,
                         nome_motorista, endereco_rua, endereco_numero, endereco_bairro,
                         endereco_cidade, endereco_estado, endereco_cep, valor_por_km,
                         total_viagem, metodo_pagamento) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s,
                                 %s, %s, %s, %s, %s, %s)
                    ''', (dados['id_passageiro'], dados['id_motorista'], nome_passageiro,
                          nome_motorista, endereco_rua, endereco_numero, endereco_bairro,
                          endereco_cidade, endereco_estado, endereco_cep, valor_passagem,
                          total_viagem, metodo_pagamento))
            
            novo_id = cursor.lastrowid

            logger.info(f'Viagem id={novo_id} adicionada com sucesso.')
            return jsonify({
                'mensagem': 'Viagem adicionada com sucesso!',
                'id': novo_id
                }), 201

    except Exception as erro:
        logger.error(f'Erro inesperado ao adicionar viagem: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao adicionar viagem!'}), 500


@viagens_bp.route('/<int:id>/cancelar', methods=['PATCH'])
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