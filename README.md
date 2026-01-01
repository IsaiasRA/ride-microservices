# Microsserviços de Transporte

# Sistema de Viagens – Backend

API backend desenvolvida em Python com Flask para gerenciamento de passageiros, motoristas, viagens e pagamentos.

## Tecnologias
- Python
- Flask
- MySQL
- Pytest

## Funcionalidades
- CRUD de passageiros
- CRUD de motoristas
- CRUD de viagens
- CRUD de pagamentos

## Como executar
```bash
python run.py


❌ Problema:  
- Genérico  
- Não explica decisões  
- Não mostra maturidade

---

```md
# 🚗 Sistema de Viagens – Backend em Flask

Este projeto é um sistema backend desenvolvido em Python com Flask, simulando um ambiente de **múltiplas APIs independentes** para gerenciamento de passageiros, motoristas, viagens e pagamentos.

O foco do projeto é demonstrar **arquitetura backend**, **regras de negócio**, **segurança** e **testes automatizados**, e não apenas CRUD simples.

---

## 🧱 Arquitetura

O sistema é composto por **4 APIs independentes**, cada uma rodando em uma porta diferente:

| API | Responsabilidade | Porta |
|----|------------------|-------|
| API 1 | Passageiros | 5001 |
| API 2 | Motoristas | 5002 |
| API 3 | Viagens | 5003 |
| API 4 | Pagamentos | 5004 |

As APIs são criadas por **factories (`create_apiX`)** e orquestradas por um arquivo principal (`run.py`), que apenas inicializa e executa as aplicações.

> ⚠️ As APIs são executadas em threads apenas para fins didáticos.  
> Em ambiente de produção, cada API seria executada como um processo ou container independente.

---

## 🔐 Autenticação

- Autenticação baseada em **JWT**
- Tokens de acesso e refresh
- Rotas protegidas por decorator
- Logout e invalidação de token

---

## 💼 Regras de Negócio

- Motoristas precisam estar ativos para aceitar viagens
- Passageiros precisam ter saldo suficiente
- Viagens podem ser canceladas com estorno financeiro
- Pagamentos pendentes ou cancelados bloqueiam operações
- Uso de transações e bloqueio (`FOR UPDATE`) para evitar inconsistências

---

## 🧪 Testes Automatizados

Os testes foram desenvolvidos com **Pytest**, focando em **testes unitários de rotas**.

### Estratégia de testes:
- Banco de dados **mockado** (MySQL)
- Conexão real com MySQL **não é utilizada**
- JWT mockado
- Chamadas externas (`requests`) mockadas

Isso garante testes:
- rápidos
- determinísticos
- independentes de infraestrutura

```bash
pytest
