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

### Windows con WSL2

En Windows se usa WSL2 (Ubuntu 24.04). El código puede vivir en el disco de Windows
(`/mnt/<unidad>/...`) y editarse desde Windows, pero conviene que **el entorno virtual y los
datos generados** queden en el filesystem de Linux: Delta hace muchas escrituras pequeñas y
`/mnt/<unidad>` es bastante más lento (medido: ~114 s frente a ~56 s por tenant).

Agregar a `~/.bashrc`:

```bash
export UV_PROJECT_ENVIRONMENT=$HOME/.venvs/saas-data-platform
export SAAS_DATA_ROOT=$HOME/saas-data
```

## Dónde se escriben los datos

| Ruta | Contenido |
|---|---|
| `data/raw/` | Entradas (CSV). Versionadas en el repo y de solo lectura. |
| `<data_root>/<layer>/<tenant>/<table>/` | Bronze, Silver y Gold de `dev` |
| `<data_root>/<layer>_quarantine/<tenant>/<table>/` | Cuarentena de Bronze y Silver |
| `<data_root>/shared/quality_logs/` | Logs de calidad (todos los tenants) |
| `<data_root>/qa/...`, `<data_root>/main/...` | Lo mismo para `qa` y `main` |

`data_root` vale `./data` por defecto (la estructura de la arquitectura) y se mueve con la
variable de entorno `SAAS_DATA_ROOT` (resuelta por OmegaConf en `config/base.yaml`), sin tocar
código ni YAML. El CI y cualquier clon nuevo usan el valor por defecto. `make clean` borra solo las
capas generadas bajo `data_root`, nunca `data/raw`.

## Uso rápido

```bash
make lint && make validate-config && make test
make run ENV=dev TENANT=sv START=2025-01-01 END=2025-06-30
```
