from logging.handlers import RotatingFileHandler
import os
import logging


def configurar_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(
        'logs/app.log',
        maxBytes=2000000,
        backupCount=5
    )

    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] [%(levelname)s] - [%(message)s]'
    ))

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter(
        '%(levelname)s: %(message)s'
    ))

    logger.addHandler(file_handler)
    logger.addHandler(console)
