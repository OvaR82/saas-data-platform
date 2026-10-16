# Onboarding de un tenant nuevo

Un tenant es una unidad de negocio (en esta prueba, un país). Dar de alta uno **no requiere tocar
código**: alcanza con un archivo YAML. Ejemplo para Costa Rica (`cr`).

## 1. Crear el archivo de configuración

`config/tenants/cr.yaml`:

```yaml
tenant:
  code: cr          # minúscula; debe coincidir con el nombre del archivo
  name: Costa Rica
  enabled: true
```

Solo se declara lo que cambia respecto de `base.yaml` y del ambiente. Por ejemplo, si este tenant
tuviera cajas de 24 unidades y una tolerancia de cuarentena distinta:

```yaml
business:
  units_per_case: 24

quality:
  max_quarantine_rate: 0.10
```

## 2. Validar

```bash
make validate-config
```

Carga todas las combinaciones de ambiente y tenant. Falla si el código no está en minúscula, no
coincide con el nombre del archivo o si alguna fecha no es válida. Lo mismo corre en el CI.

## 3. Ejecutar

```bash
make run TENANT=cr START=2025-01-01 END=2025-06-30
```

El pipeline toma del CSV las filas con `pais = CR` (normalizado a minúscula), y crea las tablas en:

```
<data_root>/bronze/cr/deliveries/
<data_root>/bronze/cr/materials/
<data_root>/silver/cr/fact_deliveries/
<data_root>/silver/cr/dim_materials/
<data_root>/gold/cr/daily_metrics_by_delivery_type/
<data_root>/gold/cr/top_materials_by_month/
<data_root>/bronze_quarantine/cr/deliveries/
<data_root>/silver_quarantine/cr/fact_deliveries/
```

Los checks de calidad quedan en `<data_root>/shared/quality_logs/` con `tenant_id = 'cr'`.
Desde ese momento `--tenant all` también lo incluye.

## 4. Verificar

- Los logs de la corrida muestran la conciliación de Silver (leídas = duplicados + descartadas +
  cuarentena + escritas).
- En `quality_logs`, ningún check `critical` del tenant con `check_passed = false`.
- Si el tenant no tiene filas en el CSV, la corrida termina bien con 0 filas: no es un error.

## 5. En Databricks (producción)

El mismo archivo YAML alimenta a Terraform (ver [infra.md](infra.md)), que crea:

- los schemas `saas_<env>.bronze_cr`, `silver_cr`, `gold_cr`, `bronze_quarantine_cr` y
  `silver_quarantine_cr`, con sus carpetas en ADLS;
- los grants (el grupo `saas-<env>-cr-readers` solo lee Gold);
- el secret scope `saas-<env>-cr` para las credenciales de sus fuentes.

Con `storage.mode=catalog`, el pipeline escribe exactamente en esos schemas
(`saas_<env>.silver_cr.fact_deliveries`, …) y `quality_logs` en `saas_<env>.shared`.

Orden recomendado: `terraform apply` en `qa` → corrida del pipeline en `qa` → revisión de
`quality_logs` → `terraform apply` y corrida en `main`.

## Pausar o dar de baja

- **Pausar**: `enabled: false` en su YAML. Deja de entrar en `--tenant all`; sus datos no se tocan.
- **Dar de baja**: es una decisión de negocio (retención, auditoría). Se elimina el YAML y se
  archivan o borran sus schemas con un cambio explícito de Terraform, nunca desde el pipeline.
