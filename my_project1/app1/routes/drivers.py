from flask import Blueprint, jsonify
from app1.auth import rota_protegida
from app1.database import conexao
from app1.validation import validar_json
from app1.log import configurar_logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import logging
import re


configurar_logging
logger = logging.getLogger(__name__)


motoristas_bp = Blueprint('motoristas', __name__)


@motoristas_bp.route('/', methods=['GET'])
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


@motoristas_bp.route('/<int:id>', methods=['GET'])
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


@motoristas_bp.route('/', methods=['POST'])
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


@motoristas_bp.route('/<int:id>', methods=['PUT'])
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
                    try:
                        valor = Decimal(str(valor)).quantize(Decimal('0.01'))
                    except InvalidOperation:
                        raise ValueError

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


@motoristas_bp.route('/<int:id>', methods=['DELETE'])
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
