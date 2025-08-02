# :snake: Rinha de Backend 2025 - Python

Este repositório contém a minha participação na Rinha de Backend 2025, implementada em Python, tentando trazer algo bem simples, sem muitas dependências, utilizando o Bottle como framework web e Redis como banco de dados.

## Tecnologias Utilizadas

As principais tecnologias utilizadas neste projeto são:

- [Python](https://www.python.org/)
- [Bottle](https://bottlepy.org/docs/dev/)
- [Redis](https://redis.io/)

## :rocket: Como Executar o Projeto

Para executar o projeto, você precisa ter o Docker e o Docker Compose instalados. Com isso, você pode iniciar os serviços com o seguinte comando:

1. Clone o repositório:

Recomendo utilizar o [GitHub CLI](https://cli.github.com/) para clonar o repositório:

```bash
gh repo clone machadoah/rinha-de-backend-2025-python
```

2. Rode os serviços:

```bash
docker-compose -f docker-compose.yml up --build
```

## Estrutura do Projeto

A estrutura do projeto é a seguinte:

```bash
.
├── api
│   ├── Dockerfile
│   └── ...
├── worker
│   ├── Dockerfile
│   └── ...
├── docker-compose.yml # Utiliza as imagens do Docker Hub
├── compose.yaml # Builda as imagens localmente
└── README.md
```
