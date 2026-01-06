from flask import jsonify, request
from flask_limiter.errors import RateLimitExceeded
from mysql.connector import errors
from app.log import configurar_logging
import logging


configurar_logging()
logger = logging.getLogger(__name__)


def tratamento_erro_mysql(erro):
    if isinstance(erro, errors.IntegrityError):
        logger.warning(f'Violação de integridade ou chave duplicada: {str(erro)}')
        return jsonify({'erro': 'Violação de integridade ou chave duplicada!'}), 409
    
    if isinstance(erro, errors.DataError):
        logger.warning(f'Tipo de dado inválido no banco SQL: {str(erro)}')
        return jsonify({'erro': 'Tipo de dado inválido no banco SQL!'}), 400
    
    if isinstance(erro, errors.OperationalError):
        logger.error(f'Erro de operação no banco SQL: {str(erro)}')
        return jsonify({'erro': 'Erro de operação no banco SQL!'}), 500
    
    if isinstance(erro, errors.ProgrammingError):
        logger.error(f'Erro de uso incorreto do cursor ou SQL inválida: {str(erro)}')
        return jsonify({'erro': 'Erro de uso incorreto do cursor ou SQL inválida!'}), 500
    
    if isinstance(erro, errors.InterfaceError):
        logger.error(f'Erro de comunicação com banco SQL: {str(erro)}')
        return jsonify({'erro': 'Erro de comunicação com banco SQL!'}), 500
    
    if isinstance(erro, errors.NotSupportedError):
        logger.error(f'Operação SQL não suportada pelo MySQL: {str(erro)}')
        return jsonify({'erro': 'Operação SQL não suportado pelo MySQL!'}), 500
    
    if isinstance(erro, errors.InternalError):
        logger.critical(f'Erros internos no banco SQL: {str(erro)}')
        return jsonify({'erro': 'Erros internos no banco SQL!'}), 500
    
    if isinstance(erro, errors.PoolError):
        logger.critical(f'Pool de conexões esgotado: {str(erro)}')
        return jsonify({'erro': 'Serviço temporariamente indisponível!'}), 503
    
    if isinstance(erro, errors.DatabaseError):
        logger.critical(f'Erro grave no banco SQL: {str(erro)}')
        return jsonify({'erro': 'Erro grave no banco SQL!'}), 500

    logger.error(f'Erro inesperado no banco SQL: {str(erro)}')
    return jsonify({'erro': 'Erro inesperado no banco SQL!'}), 500


def register_erro_handlers(app):
    @app.errorhandler(404)
    def rota_nao_encontrado(erro):
        logger.warning(f'Rota não encontrada: {str(erro)}')
        return jsonify({'erro': 'Rota não encontrada!'}), 404
    
    @app.errorhandler(401)
    def rota_nao_autorizado(erro):
        logger.warning(f'Rota não autorizada: {str(erro)}')
        return jsonify({'erro': 'Rota não autorizada!'}), 401
    
    @app.errorhandler(RateLimitExceeded)
    def rate_limit_handler(e):
        logger.warning(
            f"RATE LIMIT excedido | IP={request.remote_addr} | rota={request.path}"
        )
        return jsonify({
            'erro': 'Muitas requisições. Tente novamente mais tarde.'
        }), 429
    
    @app.errorhandler(400)
    def dados_inválidos(erro):
        logger.warning(f'Dados inválidos na rota: {str(erro)}')
        return jsonify({'erro': 'Dados inválidos na rota!'}), 400
    
    @app.errorhandler(405)
    def metodo_errado(erro):
        logger.warning(f'Método HTTP não permitido nesta rota: {str(erro)}')
        return jsonify({'erro': 'Método HTTP não permitido nesta rota!'}), 405
    
    @app.errorhandler(422)
    def logica_errada(erro):
        logger.warning(f'Dados corretos, mas lógica errada: {str(erro)}')
        return jsonify({'erro': 'Dados corretos, mas lógica errada!'}), 422
    
    @app.errorhandler(Exception)
    def erro_interno(erro):
        logger.error(f'Erro inesperado ao acessar a rota: {str(erro)}')
        return jsonify({'erro': 'Erro inesperado ao acessar a rota!'}), 500
