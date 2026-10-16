# Infraestructura como código (Terraform)

Qué provisionaría Terraform para que un tenant nuevo pueda entrar al pipeline, siguiendo la
arquitectura provista (Unity Catalog con un schema por capa y tenant, ADLS Gen2 por capas).

> El snippet es ilustrativo, como pide el enunciado (7.2): no se ejecutó `terraform plan` contra
> una cuenta real. Sí se verificó con `terraform fmt -check` y `terraform validate` (Terraform
> 1.16, providers `azurerm` 4.x y `databricks` 1.x), que comprueban sintaxis, tipos y atributos
> contra los providers reales sin necesitar credenciales. Se repite con `make infra-validate`
> (requiere la CLI de Terraform).

## Qué se crea una vez por ambiente y qué una vez por tenant

| Alcance | Recurso | Para qué |
|---|---|---|
| Ambiente | Cuenta de almacenamiento ADLS Gen2 y contenedor de datos | Almacenamiento físico de las capas |
| Ambiente | *Storage credential* + *external location* en Unity Catalog | Que Databricks pueda leer y escribir en ADLS sin claves en el código |
| Ambiente | Catálogo `saas_<env>` | Raíz del gobierno de datos del ambiente |
| Ambiente | Schema `saas_<env>.shared` | `quality_logs` de todos los tenants (5.9, A14) |
| Ambiente | Service principal del pipeline | Identidad con la que corre el job |
| **Tenant** | Carpetas `<área>/<tenant>/` en ADLS para 5 áreas: `bronze`, `silver`, `gold`, `bronze_quarantine`, `silver_quarantine` | Ubicación física de sus tablas |
| **Tenant** | Un schema `<área>_<tenant>` por área (`bronze_sv`, …, `silver_quarantine_sv`) | Aislamiento lógico (5.2) y cuarentena (5.6); son los nombres que usa el pipeline con `storage.mode=catalog` |
| **Tenant** | Grants | El grupo del tenant solo lee Gold; el pipeline escribe todas las capas |
| **Tenant** | Secret scope respaldado por Key Vault | Credenciales de sus fuentes (por ejemplo, MongoDB) fuera del código |

Lo que se crea por ambiente vive en un módulo raíz y se aplica una vez; el **módulo `tenant`** se
instancia una vez por cada tenant.

## La lista de tenants sale de la misma configuración que usa el pipeline

El módulo raíz lee `config/tenants/*.yaml`: dar de alta un tenant es agregar un YAML, y ese mismo
archivo habilita tanto la infraestructura (`terraform apply`) como el pipeline (`--tenant all`).
No hay dos listas de tenants que mantener sincronizadas.

```hcl
# infra/main.tf (módulo raíz, por ambiente)
locals {
  tenant_files = fileset("${path.module}/../config/tenants", "*.yaml")
  tenants = {
    for f in local.tenant_files : trimsuffix(f, ".yaml") => yamldecode(file("${path.module}/../config/tenants/${f}")).tenant
    if yamldecode(file("${path.module}/../config/tenants/${f}")).tenant.enabled
  }
}

module "tenant" {
  source      = "./modules/tenant"
  for_each    = local.tenants
  env         = var.env
  tenant      = each.key
  storage     = var.storage
  pipeline_sp = var.pipeline_sp
  key_vault   = var.key_vault
}
```

## Módulo `tenant` (snippet principal)

`infra/modules/tenant/main.tf`: todo lo que necesita un tenant en un ambiente.

```hcl
variable "env" { type = string }         # dev | qa | main
variable "tenant" { type = string }      # lowercase code, e.g. "sv"
variable "pipeline_sp" { type = string } # application id of the pipeline service principal
variable "storage" { type = object({ id = string, name = string, container = string }) }
variable "key_vault" { type = object({ id = string, uri = string }) }

locals {
  areas   = toset(["bronze", "silver", "gold", "bronze_quarantine", "silver_quarantine"])
  adls    = "abfss://${var.storage.container}@${var.storage.name}.dfs.core.windows.net"
  readers = "saas-${var.env}-${var.tenant}-readers" # Entra ID group that consumes Gold
}

resource "azurerm_storage_data_lake_gen2_path" "area" {
  for_each           = local.areas
  storage_account_id = var.storage.id
  filesystem_name    = var.storage.container
  path               = "${each.key}/${var.tenant}"
  resource           = "directory"
}

resource "databricks_schema" "area" {
  for_each     = local.areas
  catalog_name = "saas_${var.env}"
  name         = "${each.key}_${var.tenant}"
  storage_root = "${local.adls}/${azurerm_storage_data_lake_gen2_path.area[each.key].path}"
}

resource "databricks_grants" "area" {
  for_each = local.areas
  schema   = databricks_schema.area[each.key].id
  grant {
    principal  = var.pipeline_sp
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  dynamic "grant" {
    for_each = each.key == "gold" ? [local.readers] : []
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
}

resource "databricks_secret_scope" "tenant" {
  name = "saas-${var.env}-${var.tenant}"
  keyvault_metadata {
    resource_id = var.key_vault.id
    dns_name    = var.key_vault.uri
  }
}
```

Los providers se fijan aparte, en el mismo módulo:

```hcl
# infra/modules/tenant/versions.tf
terraform {
  required_providers {
    azurerm    = { source = "hashicorp/azurerm", version = "~> 4.0" }
    databricks = { source = "databricks/databricks", version = "~> 1.50" }
  }
}
```

## Decisiones del snippet

- **Mismos nombres que el pipeline**: las 5 áreas del módulo (capas y cuarentenas) son exactamente
  los schemas y carpetas que usa el pipeline: por ruta (`<área>/<tenant>/`) y con
  `storage.mode=catalog` (`saas_<env>.<área>_<tenant>`, `observations.md` A15). Con Terraform
  creándolos, el `CREATE SCHEMA IF NOT EXISTS` del pipeline no hace nada.
- **Un solo conjunto para carpetas, schemas y grants**: agregar un área (por ejemplo `platinum`)
  es agregar un elemento a `areas`. El `storage_root` referencia la carpeta, así Terraform la
  crea antes que el schema.
- **Grants con mínimo privilegio**: solo el service principal del pipeline modifica tablas; el grupo
  del tenant lee únicamente Gold. Bronze y Silver no se exponen a consumidores.
- **Secretos fuera del código y del estado de Terraform**: el valor de la credencial se carga en Key
  Vault por un proceso aparte; Terraform solo conecta el secret scope.
- **Las carpetas de cuarentena se crean aunque no tengan schema propio**: mantienen el mismo layout
  que el pipeline (`<layer>_quarantine/<tenant>/`). Si se adoptara la propuesta D3 de
  `observations.md` (catálogo por tenant), el módulo crearía un `databricks_catalog` con su propia
  `storage_root` en lugar de tres schemas.

## Qué queda fuera del snippet

- Backend remoto del estado (`azurerm` backend en una cuenta de almacenamiento con bloqueo).
- Los recursos de ambiente (cuenta de almacenamiento, external location, catálogo, service
  principal), que se crean una sola vez.
- El job de Databricks que ejecuta el pipeline (propuesto con Asset Bundles en la mejora H5).
