from flask import Flask
from app.routes.passengers import passageiros_bp
from app.routes.drivers import motoristas_bp
from app.routes.trips import viagens_bp
from app.routes.payment_records import registros_pagamento_bp
from app.database import inicializador_banco
from app.error import register_erro_handlers
from app.brute_force import limiter


def create_api1():
    app1 = Flask('API1')

    limiter.init_app(app1)

    inicializador_banco()

    app1.register_blueprint(passageiros_bp, url_prefix='/passageiros')

    register_erro_handlers(app1)

    return app1


def create_api2():
    app2 = Flask('API2')

    limiter.init_app(app2)

    inicializador_banco()

    app2.register_blueprint(motoristas_bp, url_prefix='/motoristas')

    register_erro_handlers(app2)

    return app2


def create_api3():
    app3 = Flask('API3')

    limiter.init_app(app3)

    inicializador_banco()

    app3.register_blueprint(viagens_bp, url_prefix='/viagens')

    register_erro_handlers(app3)

    return app3


def create_api4():
    app4 = Flask('API4')

    limiter.init_app(app4)

    inicializador_banco()

    app4.register_blueprint(registros_pagamento_bp,
                             url_prefix='/registros-pagamento')
    
    register_erro_handlers(app4)

    return app4
