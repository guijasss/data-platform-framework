# Guia com `uv` para publicar um package no PyPI

`uv` suporta o fluxo completo: criar projeto, gerenciar dependências, gerar distribuições e publicar com `uv publish`.

A documentação oficial recomenda configurar um `[build-system]` no `pyproject.toml` antes de publicar.

## 1. Criar o projeto

```bash
uv init --package meu-package
cd meu-package
```

Estrutura esperada:

```text
meu-package/
├── pyproject.toml
├── README.md
└── src/
    └── meu_package/
        └── __init__.py
```

## 2. Fixar versão do Python

```bash
uv python pin 3.12
```

No `pyproject.toml`:

```toml
[project]
requires-python = ">=3.10"
```

## 3. Adicionar dependências de runtime

Essas entram no pacote publicado.

```bash
uv add requests
uv add "pydantic>=2,<3"
```

Isso atualiza:

```toml
[project]
dependencies = [
    "requests>=2.32.0",
    "pydantic>=2,<3",
]
```

Use ranges, não `==`, para bibliotecas publicadas.

## 4. Adicionar dependências de desenvolvimento

Essas não são publicadas como dependências do pacote; são locais para desenvolvimento. A própria documentação do `uv` diferencia dependências do projeto das dependências de desenvolvimento.

```bash
uv add --dev pytest ruff mypy
```

Fica algo como:

```toml
[dependency-groups]
dev = [
    "pytest>=8",
    "ruff>=0.8",
    "mypy>=1",
]
```

## 5. Adicionar extras opcionais

Para recursos opcionais:

```bash
uv add --optional postgres "psycopg[binary]>=3,<4"
uv add --optional redis "redis>=5,<6"
```

Resultado:

```toml
[project.optional-dependencies]
postgres = [
    "psycopg[binary]>=3,<4",
]
redis = [
    "redis>=5,<6",
]
```

Usuário instala assim:

```bash
pip install "meu-package[postgres]"
```

## 6. Usar um build backend

Exemplo com o backend nativo do `uv`, adequado para muitos projetos Python modernos:

```toml
[build-system]
requires = ["uv_build>=0.9.0,<0.10.0"]
build-backend = "uv_build"
```

Alternativa com `hatchling`:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

## 7. Exemplo completo de `pyproject.toml`

```toml
[build-system]
requires = ["uv_build>=0.9.0,<0.10.0"]
build-backend = "uv_build"

[project]
name = "meu-package"
version = "0.1.0"
description = "Minha biblioteca Python"
readme = "README.md"
requires-python = ">=3.10"
license = "MIT"
authors = [
    { name = "Guilherme Samuel", email = "guilherme.siqueira@evcomx.com.br" }
]
dependencies = [
    "requests>=2.32,<3",
    "pydantic>=2,<3",
]

[project.optional-dependencies]
postgres = [
    "psycopg[binary]>=3,<4",
]

[dependency-groups]
dev = [
    "pytest>=8",
    "ruff>=0.8",
    "mypy>=1",
]
```

## 8. Sincronizar ambiente local

```bash
uv sync
```

Com extras:

```bash
uv sync --extra postgres
```

Com todas as dependências opcionais:

```bash
uv sync --all-extras
```

## 9. Rodar testes e lint

```bash
uv run pytest
uv run ruff check .
uv run mypy src
```

## 10. Gerar o build

```bash
uv build
```

O `uv build` gera normalmente:

```text
dist/
├── meu_package-0.1.0.tar.gz
└── meu_package-0.1.0-py3-none-any.whl
```

O `uv` atua como frontend de build; detalhes de inclusão de arquivos e nomes finais dependem do backend configurado no `[build-system]`.

## 11. Publicar no TestPyPI primeiro

Crie um token em TestPyPI e rode:

```bash
uv publish \
  --publish-url https://test.pypi.org/legacy/ \
  --token pypi-SEU_TOKEN
```

Teste instalação:

```bash
uv pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  meu-package
```

## 12. Publicar no PyPI

Crie um token no PyPI e publique:

```bash
uv publish --token pypi-SEU_TOKEN
```

Ou usando variável de ambiente:

```bash
export UV_PUBLISH_TOKEN="pypi-SEU_TOKEN"
uv publish
```

## Regras práticas

| Tipo | Onde declarar | Publica no PyPI? |
| --- | --- | --- |
| Runtime | `[project].dependencies` | Sim |
| Extras opcionais | `[project.optional-dependencies]` | Sim, como extras |
| Dev/test/lint | `[dependency-groups].dev` | Não |
| Lockfile | `uv.lock` | Normalmente versionado no repo, mas não usado por quem instala via PyPI |

Para biblioteca publicada, o ponto principal é: não use o lockfile para limitar usuários finais. Use ranges no `pyproject.toml`; use `uv.lock` para desenvolvimento e CI.
