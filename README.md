# 🚕 Transportation System – Backend APIs in Python (Flask)

Backend project developed using **Python + Flask**, composed of **four independent APIs** responsible for managing passengers, drivers, trips, and payment records.

The system was designed with **clear domain separation**, well-defined business rules, and a **modular architecture**, simulating an environment close to **microservices**.

---

## ⚙️ Technologies Used
- Python 3.11+
- Flask
- MySQL 8.0.44
- Flask-Limiter (Rate Limiting)
- JWT (Authentication)
- Logging
- Decimal (Financial Precision)
- Threading (Simultaneous API execution)

---
## 🧪 Automated Testing

The project includes **automated tests using Pytest**, ensuring the reliability of business rules and API behavior.

Tests cover:
- Route responses and HTTP status codes
- Business rule validations (e.g. cancellation rules, invalid states)
- Error handling for non-existent resources
- Idempotency of critical operations (e.g. canceling trips or payments more than once)

### Testing Tools
- Pytest
- Flask test client
- Isolated test database
- Transactional tests with rollback

### Running Tests

From the project root directory:

- cd my_project1
- pytest

## 🧠 Architecture Overview

The project is divided into four APIs:

| API | Responsibility | Port |
|---|---|---|
| Passengers API | Registration, balance, personal data | 5001 |
| Drivers API | Registration, status, fares | 5002 |
| Trips API | Trip creation and management | 5003 |
| Payment Records API | Financial control of trips | 5004 |

Each API:
- Has its own routes
- Contains isolated business rules
- Implements robust validations
- Provides error handling and logging

---

## 🧩 Modular API Execution

The APIs are created using the **Application Factory** pattern and can be executed simultaneously using **threads**, each running on its own port.

This approach:
- Improves maintainability
- Allows each domain to scale independently
- Simulates a microservices-oriented architecture
- Facilitates future migration to Docker/Kubernetes

### Example of API Initialization

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

    input('APIs running. Press ENTER to stop.')

if __name__ == '__main__':
    main()
