# 🚕 Sistema de Transporte – APIs Backend em Python (Flask)

Projeto backend desenvolvido em **Python + Flask**, composto por **4 APIs independentes**, responsáveis por gerenciar passageiros, motoristas, viagens e registros de pagamento.

O sistema foi pensado com **separação de domínios**, regras de negócio bem definidas e arquitetura modular, simulando um ambiente próximo de **microserviços**.
---
## ⚙️ Tecnologias Utilizadas
- Python 3.11+
- Flask
- MySQL 8.0.44
- Flask-Limiter (Rate Limit)
- JWT (Autenticação)
- Logging
- Decimal (Precisão financeira)
- Threading (execução simultânia das APIs)
---

## 🧠 Visão Geral da Arquitetura

O projeto é dividido em quatro APIs:

| API | Responsabilidade | Porta |
|---|---|---|
| API Passageiros | Cadastro, saldo, dados pessoais | 5001 |
| API Motoristas | Cadastro, status, valores | 5002 |
| API Viagens | Criação e controle de viagens | 5003 |
| API Registros de Pagamento | Controle financeiro das viagens | 5004 |

Cada API:
- Possui rotas próprias
- Regras de negócio isoladas
- Validações robustas
- Controle de erros e logs

---

## 🧩 Execução Modular das APIs

As APIs são criadas utilizando o padrão **Application Factory** e podem ser executadas simultaneamente através de **threads**, cada uma em sua própria porta.

Essa abordagem:
- Facilita manutenção
- Permite escalar cada domínio separadamente
- Simula um cenário de microserviços
- Facilita futura migração para Docker/Kubernetes

### Exemplo de inicialização das APIs

```python
from app1 import (
    create_api1,
    create_api2,
    create_api3,
    create_api4
)
import threading

def start_api(app, port):
    app.run(debug=True, port=port, use_reloader=False)

def main():
    app1 = create_api1()
    app2 = create_api2()
    app3 = create_api3()
    app4 = create_api4()

    apis = [
        (app1, 5001),
        (app2, 5002),
        (app3, 5003),
        (app4, 5004)
    ]

    for app, port in apis:
        threading.Thread(
            target=start_api,
            args=(app, port),
            daemon=True
        ).start()

    input('APIs rodando. Pressione ENTER para sair.')

if __name__ == '__main__':
    main()
