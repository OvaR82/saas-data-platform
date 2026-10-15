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
