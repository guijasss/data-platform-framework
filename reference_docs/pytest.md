# Pytest com UV

## Instalar dependências de teste

```bash
uv add --dev pytest pytest-cov
```

---

# Estrutura recomendada

```text
project/
├── src/
│   └── ...
├── tests/
│   ├── test_database.py
│   ├── test_config.py
│   └── test_utils.py
├── pyproject.toml
└── uv.lock
```

---

# Criar um teste simples

```python
def test_sum():
    assert 1 + 1 == 2
```

---

# Rodar todos os testes

```bash
uv run pytest
```

---

# Rodar um arquivo específico

```bash
uv run pytest tests/test_database.py
```

---

# Rodar um teste específico

```bash
uv run pytest tests/test_database.py::test_table_exists
```

---

# Rodar com saída detalhada

```bash
uv run pytest -v
```

---

# Mostrar prints/logs

```bash
uv run pytest -s
```

---

# Parar no primeiro erro

```bash
uv run pytest -x
```

---

# Coverage básico

```bash
uv run pytest --cov=src
```

---

# Coverage com relatório detalhado

```bash
uv run pytest --cov=src --cov-report=term-missing
```

---

# Coverage HTML

```bash
uv run pytest --cov=src --cov-report=html
```

Relatório gerado em:

```text
htmlcov/index.html
```

---

# Exigir coverage mínimo

```bash
uv run pytest --cov=src --cov-fail-under=90
```

---

# Configuração recomendada no pyproject.toml

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]

[tool.coverage.run]
source = ["src"]

[tool.coverage.report]
omit = [
    "tests/*",
]
```

---

# Fixtures básicas

```python
import pytest


@pytest.fixture
def sample_data():
    return {
        "id": 1,
        "name": "Alice",
    }


def test_sample(sample_data):
    assert sample_data["id"] == 1
```

---

# Mock simples

```python
from unittest.mock import patch


@patch("src.database.table_exists")
def test_mock(mock_table_exists):
    mock_table_exists.return_value = True

    assert mock_table_exists() is True
```

---

# Rodar testes automaticamente ao salvar

Instalar:

```bash
uv add --dev pytest-watch
```

Executar:

```bash
uv run ptw
```

---

# Boas práticas

- Um comportamento por teste
- Nome descritivo
- Evitar dependência entre testes
- Mockar IO externo
- Não testar implementação interna
- Testar comportamento esperado