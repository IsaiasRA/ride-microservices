from flask import jsonify, request
from app.log import configurar_logging
from werkzeug.exceptions import BadRequest
import logging


configurar_logging()
logger = logging.getLogger(__name__)


def validar_json():
    try:
        if not request.is_json:
            logger.warning('Requisição deve ser Content_type: application/json.')
            return jsonify(
                {'erro': 'Requisição deve ser Content-type: application/json!'}), 400
        
        dados = request.get_json()
        if not dados:
            logger.warning('Dados ausentes ou inválidos no corpo da requisição.')
            return jsonify(
                {'erro': 'Dados ausentes ou inválidos no corpo da requisição!'}), 400
        
        return dados
    except BadRequest:
        logger.warning(f'JSON malformado! Dados inválidos no corpo da requisição.')
        return jsonify({'erro': 'JSON malformado. Dados inválidos!'}), 400
    
    except Exception as erro:
        logger.error(f'Erro inesperado ao válidar JSON: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao válidar JSON!'}), 500


PREPOSICOES = [
    'da', 'de', 'do', 'das', 'dos',
    'van', 'von', 'di', 'du'
]


def formatar_nome(nome: str):
    palavras = nome.strip().lower().split()

    nome_formatado = []
    for i, p in enumerate(palavras):
        if p in PREPOSICOES and i != 0:
            nome_formatado.append(p)
        else:
            nome_formatado.append(p.capitalize())
    
    return ' '.join(nome_formatado)