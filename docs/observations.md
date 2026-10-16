# Observaciones a la arquitectura provista

> Borrador vivo: cada decisión se registra en el momento en que se toma. Formato tipo ADR:
> contexto → lo que dice la arquitectura → problema → decisión/propuesta → trade-offs.

## Ambigüedades resueltas en la implementación

### A1. Bronze y filas sin `fecha_proceso` válida
- **Arquitectura**: Bronze particionado por `fecha_proceso`; la cuarentena por fecha inválida figura en la política de Silver (5.6).
- **Problema**: 40 filas del CSV (20 nulas, 5 en formato `YYYY-MM-DD`, 15 imposibles: `20250230`, `20251332`, `00000000`) no pueden asignarse a ninguna partición, así que ni siquiera llegan a Silver.
- **Decisión**: Bronze las enruta a `bronze_quarantine/<tenant>/deliveries` con `_quarantine_reason` (`null_fecha_proceso` | `invalid_fecha_proceso`). Como no pertenecen a ningún rango de fechas, esa tabla se reconstruye completa en cada corrida (sigue siendo idempotente).
- **Trade-off**: Bronze deja de ser "todo lo que llegó" en una sola tabla. Se compensa porque Bronze + cuarentena de Bronze = 100% de las filas del tenant.

### A2. Formato ISO (`2025-03-15`) en `fecha_proceso`
- **Decisión**: se trata como inválida (cuarentena). El contrato de la fuente es `YYYYMMDD`; normalizar en silencio ocultaría un problema del productor.
- **Alternativa**: aceptar una lista de formatos tolerados declarada en config por tenant, con un check `warning` que cuente cuántas filas se corrigieron.

### A3. Particionado de Bronze por `tenant_id`
- **Arquitectura**: 5.4 pide particionar Bronze por `fecha_proceso` **y** `tenant_id`; 5.2 define el path `data/bronze/<tenant>/deliveries/fecha_proceso=YYYYMMDD/`.
- **Problema**: el tenant ya separa físicamente la tabla (path/schema), así que una partición por `_tenant_id` dentro de ella tendría un único valor y solo añadiría un nivel de directorios.
- **Decisión**: el aislamiento por tenant lo da el path (equivalente al schema `bronze_<tenant>` en Unity Catalog), la partición física es `fecha_proceso`, y `_tenant_id` se persiste como columna. `replaceWhere` actúa sobre el rango de fechas dentro de la tabla del tenant, que es "fecha + tenant".

### A4. Tipos en Bronze
- **Decisión**: Bronze guarda todas las columnas fuente como `string` (con `enforceSchema=false`, que falla si cambia el header). El tipado (`DECIMAL(38,18)`, `long`, `date`) ocurre en Silver.
- **Por qué**: un cast en Bronze convierte en `null` cualquier valor mal formado y se pierde la evidencia del dato original; con strings Bronze es una copia fiel y auditable de la fuente.

### A5. Fila con varias anomalías a la vez
- **Arquitectura**: 5.6 define una acción por tipo de anomalía, pero no qué pasa si una fila cumple varias.
- **Decisión**: la fila va una sola vez a cuarentena y `_quarantine_reason` lista **todas** las razones separadas por `;` (p. ej. `non_positive_cantidad;material_not_in_catalog`). Un orden de precedencia escondería problemas: al corregir el primero, la fila volvería a caer por el segundo.
- **Orden de reglas**: dedupe exacto → descarte de `tipo_entrega` fuera de alcance → cuarentena. Un `COBR` con cantidad negativa se descarta (regla de negocio), no se pone en cuarentena.

### A6. Unidades distintas de CS/ST
- **Problema**: la política no contempla una `unidad` desconocida, pero sin ella no hay `cantidad_normalizada_st` ni revenue.
- **Decisión**: regla de cuarentena adicional `invalid_unidad` (hoy 0 casos en el dataset), coherente con el criterio de la arquitectura: nada se pierde en silencio.

### A7. Precio unitario y unidad de medida
- **Problema**: si `unidad = CS`, ¿`precio` es por caja o por unidad? Cambia el revenue en un factor 20.
- **Evidencia**: el diccionario lo define como "precio unitario", y en los datos `precio` está en el mismo rango que `precio_base` del catálogo tanto en filas CS como ST.
- **Decisión**: `total_revenue = cantidad_normalizada_st × precio`, como pide 6.4. Lo dejo explícito porque es el supuesto con mayor impacto en las métricas.

### A8. Reprocesos y filas que salen de Silver
- **Arquitectura**: MERGE (update + insert) en `fact_deliveries`.
- **Problema**: un MERGE solo con update/insert nunca borra. Si una fila que estaba en Silver pasa a cuarentena (por ejemplo, porque se corrigió el catálogo), quedaría duplicada: en Silver y en cuarentena.
- **Decisión**: el MERGE se acota al rango reprocesado (`t.fecha_proceso BETWEEN start AND end`) y agrega `WHEN NOT MATCHED BY SOURCE ... DELETE` dentro de ese rango. Así el resultado de reprocesar un rango es exactamente el mismo que el de procesarlo por primera vez. `dim_materials` no lleva esa cláusula: su historia nunca se borra.

### A9. SCD2 cuando la fuente ya trae la historia
- **Contexto**: el CSV del catálogo ya trae las versiones con `valid_from`/`valid_to`.
- **Decisión**: MERGE sobre `(material, valid_from)`: versiones nuevas se insertan y las existentes se actualizan (cierre de vigencia o corrección). `is_current` se **recalcula** desde `valid_to = 9999-12-31`; el flag de la fuente no se usa (el enunciado lo define como "no fuente de verdad única").
- **Salvaguarda**: dos checks críticos validan las invariantes del SCD2 antes de Gold: una sola versión vigente por SKU y versiones sin solapamiento (un solapamiento duplicaría filas en el join temporal).
- **Límite conocido**: si la fuente enviara solo la versión nueva sin cerrar la anterior, el check de solapamiento lo detecta pero el pipeline no la cierra automáticamente. Ver mejora H1.

### A10. `dim_materials` por tenant
- **Arquitectura**: `saas_<env>.silver_<tenant>.dim_materials`.
- **Decisión**: se implementa tal cual: el catálogo (compartido) se materializa en el schema de cada tenant.
- **Observación**: es dato maestro corporativo; replicarlo por tenant multiplica almacenamiento y abre la puerta a versiones divergentes. Propuesta en D2.

### A11. Columnas de fecha en Silver
- **Arquitectura**: fechas de negocio con tipo `date`; `fecha_proceso` es la clave de partición con formato `YYYYMMDD`.
- **Decisión**: se conservan ambas: `fecha_proceso` (string `yyyymmdd`, partición y clave del MERGE, igual que en Bronze y en los paths) y `fecha_proceso_date` (`date`, usada en el join temporal y para consumo analítico).

## Desacuerdos con la arquitectura (propuesta alternativa)

### D1. Particionado de Gold por fecha
- Gold `daily_metrics_by_delivery_type` tiene ~4 filas por día y tenant (≈130 filas por tenant en el semestre). Particionar por fecha generaría cientos de archivos diminutos.
- **Implementado**: tabla no particionada; la idempotencia se mantiene con `replaceWhere` sobre los datos del rango (Delta lo soporta sin partición física).
- **Propuesta**: Liquid Clustering por `fecha_proceso` en Databricks, también para Silver: evita elegir la granularidad de partición por adelantado y el problema de archivos pequeños.

### D2. Dato maestro compartido en lugar de copiado por tenant
- **Propuesta**: `saas_<env>.shared.dim_materials` gobernado una sola vez, con vistas por tenant si se requiere aislamiento. Si en el futuro cada tenant tiene su propio catálogo, la columna `tenant_id` en la dimensión resuelve ambos casos.
- **Trade-off**: el schema `shared` requiere grants explícitos de lectura para cada tenant.

## Mejoras propuestas (Horizonte 2-3)

### H1. SCD2 a partir de snapshots o CDC
Generar las versiones en el pipeline (comparar el snapshot entrante contra la versión vigente, cerrar `valid_to` e insertar la nueva) en lugar de depender de que la fuente entregue la historia ya armada. En Databricks: `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` (DLT) hace esto de forma declarativa.
