# Infraestructura como código (Terraform)

Qué provisionaría Terraform para que un tenant nuevo pueda entrar al pipeline, siguiendo la
arquitectura provista (Unity Catalog con un schema por capa y tenant, ADLS Gen2 por capas).

> El snippet es ilustrativo, como pide el enunciado (7.2): no se ejecutó `terraform plan` contra
> una cuenta real.

## Qué se crea una vez por ambiente y qué una vez por tenant

| Alcance | Recurso | Para qué |
|---|---|---|
| Ambiente | Cuenta de almacenamiento ADLS Gen2 y contenedor de datos | Almacenamiento físico de las capas |
| Ambiente | *Storage credential* + *external location* en Unity Catalog | Que Databricks pueda leer y escribir en ADLS sin claves en el código |
| Ambiente | Catálogo `saas_<env>` | Raíz del gobierno de datos del ambiente |
| Ambiente | Service principal del pipeline | Identidad con la que corre el job |
| **Tenant** | Carpetas `<layer>/<tenant>/` y de cuarentena en ADLS | Ubicación física de sus tablas |
| **Tenant** | Schemas `bronze_<tenant>`, `silver_<tenant>`, `gold_<tenant>` | Aislamiento lógico (5.2) |
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
  source       = "./modules/tenant"
  for_each     = local.tenants
  env          = var.env
  tenant       = each.key
  storage      = var.storage
  pipeline_sp  = var.pipeline_sp
  tenant_group = "saas-${var.env}-${each.key}-readers"
  key_vault    = var.key_vault
}
```

## Módulo `tenant` (snippet principal)

```hcl
# infra/modules/tenant/main.tf: everything one tenant needs in one environment.
variable "env" { type = string }          # dev | qa | main
variable "tenant" { type = string }       # lowercase code, e.g. "sv"
variable "pipeline_sp" { type = string }  # application id of the pipeline service principal
variable "tenant_group" { type = string } # Entra ID group that consumes this tenant's Gold
variable "storage" { type = object({ id = string, name = string, container = string }) }
variable "key_vault" { type = object({ id = string, uri = string }) }

locals {
  layers  = toset(["bronze", "silver", "gold"])
  folders = toset(["bronze", "silver", "gold", "bronze_quarantine", "silver_quarantine"])
}

# ADLS Gen2: <layer>/<tenant>/, the same layout the pipeline writes locally under data/.
resource "azurerm_storage_data_lake_gen2_path" "folder" {
  for_each           = local.folders
  storage_account_id = var.storage.id
  filesystem_name    = var.storage.container
  path               = "${each.key}/${var.tenant}"
  resource           = "directory"
}

# Unity Catalog: saas_<env>.<layer>_<tenant>, managed tables stored in the tenant's folder.
resource "databricks_schema" "layer" {
  for_each     = local.layers
  catalog_name = "saas_${var.env}"
  name         = "${each.key}_${var.tenant}"
  storage_root = "abfss://${var.storage.container}@${var.storage.name}.dfs.core.windows.net/${each.key}/${var.tenant}"
  comment      = "${each.key} layer of tenant ${var.tenant} (managed by Terraform)"
}

# Least privilege: the pipeline writes every layer; the tenant group only reads Gold.
resource "databricks_grants" "layer" {
  for_each = local.layers
  schema   = databricks_schema.layer[each.key].id
  grant {
    principal  = var.pipeline_sp
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  dynamic "grant" {
    for_each = each.key == "gold" ? [var.tenant_group] : []
    content {
      principal  = grant.value
      privileges = ["USE_SCHEMA", "SELECT"]
    }
  }
}

# Source credentials live in Key Vault and reach Databricks through a secret scope.
resource "databricks_secret_scope" "tenant" {
  name = "saas-${var.env}-${var.tenant}"
  keyvault_metadata {
    resource_id = var.key_vault.id
    dns_name    = var.key_vault.uri
  }
}
```

## Decisiones del snippet

- **`for_each` sobre capas y carpetas**: agregar una capa (por ejemplo `platinum`) es agregar un
  elemento a la lista, no copiar un bloque.
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
