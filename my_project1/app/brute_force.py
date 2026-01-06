from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from collections import defaultdict
import time


tentativa_login = defaultdict(list)

MAX_TENTATIVAS = 5
JANELA_TEMPO = 300


def ip_bloqueado(ip):
    agora = time.time()

    tentativa_login[ip] = [
        t for t in tentativa_login[ip]
        if agora - t < JANELA_TEMPO
    ]

    return len(tentativa_login[ip]) >= MAX_TENTATIVAS


def registrar_falha(ip):
    tentativa_login[ip].append(time.time())


def limpar_falhas(ip):
    tentativa_login.pop(ip, None)


limiter = Limiter(
    key_func=get_remote_address,
    default_limits=['100 per hour']
)
