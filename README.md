# saas-data-platform

Pipeline Medallion multi-tenant (RAW → Bronze → Silver → Gold) sobre PySpark + Delta Lake,
siguiendo la arquitectura provista del proyecto SAAS.

> Documento en construcción: las secciones completas (estructura, onboarding de tenants,
> decisiones de alcance) se agregan a medida que avanza la implementación.

## Versiones

| Componente | Versión |
|---|---|
| Python | 3.11 |
| PySpark | 3.5.3 |
| delta-spark | 3.2.1 |
| OmegaConf | 2.3.0 |
| Java (requerido por Spark) | 17 |

## Entorno (Linux / WSL2)

```bash
sudo apt install -y openjdk-17-jdk make
curl -LsSf https://astral.sh/uv/install.sh | sh
make setup
```

En Windows se usa WSL2 (Ubuntu 24.04). El código puede vivir en el disco de Windows
(`/mnt/<unidad>/...`); para que el entorno virtual quede en el filesystem de Linux, que es más rápido:

```bash
export UV_PROJECT_ENVIRONMENT=$HOME/.venvs/saas-data-platform
```

## Uso rápido

```bash
make lint && make validate-config && make test
make run ENV=dev TENANT=sv START=2025-01-01 END=2025-06-30
```
