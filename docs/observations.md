# Observaciones a la arquitectura provista

Registro de las decisiones tomadas sobre la arquitectura provista (sección 5 del enunciado). Cada
entrada sigue un formato tipo ADR: contexto → lo que dice la arquitectura → problema →
decisión o propuesta → trade-offs. Ninguna discrepancia se implementó en contra de la arquitectura
sin dejarla registrada aquí.

| Tipo | Entradas | Qué contiene |
|---|---|---|
| Ambigüedades resueltas | A1–A12 | Huecos del enunciado y cómo se resolvieron en el código |
| Desacuerdos | D1–D5 | Decisiones provistas que cambiaría, con alternativa y trade-offs |
| Mejoras (Horizonte 2-3) | H1–H8 | Evolución tecnológica propuesta; H8 integra todas en una plataforma objetivo con criterio de costo |

Las tres más relevantes para discutir: **A8** (reprocesos que borran de Silver), **D3**
(aislamiento por schema frente a catálogo) y **D5** (la clave de negocio no identifica una entrega).

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

### A12. Recálculo de Gold por mes completo en la tabla mensual
- **Arquitectura**: 5.5 define el recálculo de Gold "por partición de fecha".
- **Problema**: la segunda tabla Gold (`top_materials_by_month`, bonus) es un ranking mensual.
  Recalcularla solo con las fechas del rango procesado daría un ranking incorrecto: reprocesar del
  10 al 20 de marzo dejaría un "top de marzo" calculado con 11 días.
- **Decisión**: el rango se amplía a meses completos (del primer día del mes inicial al último día
  del mes final), se leen de Silver esos meses y se reemplazan con `replaceWhere` sobre
  `month_start`. Se mantiene el principio de 5.5 (Gold es derivado y se recalcula entero para lo
  que toca), aplicado a la granularidad de la tabla. La tabla diaria sigue el recálculo por fecha.
- **Verificación**: `test_partial_range_recomputes_the_whole_month` reprocesa del 10 al 20 de
  marzo y comprueba que el ranking de marzo es idéntico al de la corrida completa y que los demás
  meses no se tocaron.

## Desacuerdos con la arquitectura (propuesta alternativa)

### D1. Particionado de Gold por fecha
- Gold `daily_metrics_by_delivery_type` tiene ~4 filas por día y tenant (entre 100 y 126 filas por tenant en el semestre). Particionar por fecha generaría cientos de archivos diminutos.
- **Implementado**: tabla no particionada; la idempotencia se mantiene con `replaceWhere` sobre los datos del rango (Delta lo soporta sin partición física).
- **Propuesta**: Liquid Clustering por `fecha_proceso` en Databricks, también para Silver: evita elegir la granularidad de partición por adelantado y el problema de archivos pequeños.

### D2. Dato maestro compartido en lugar de copiado por tenant
- **Propuesta**: `saas_<env>.shared.dim_materials` gobernado una sola vez, con vistas por tenant si se requiere aislamiento. Si en el futuro cada tenant tiene su propio catálogo, la columna `tenant_id` en la dimensión resuelve ambos casos.
- **Trade-off**: el schema `shared` requiere grants explícitos de lectura para cada tenant.

### D3. Aislamiento por schema dentro de un catálogo único
- **Arquitectura**: un catálogo por ambiente (`saas_<env>`) y un schema por tenant (5.2).
- **Problema**: el límite de aislamiento es el schema. Un `GRANT` mal dado a nivel de catálogo
  (por ejemplo `USE CATALOG` + `SELECT` heredado) expone a todos los tenants a la vez. Tampoco
  permite separar la ubicación de almacenamiento ni los costos por unidad de negocio de forma
  nativa, algo habitual cuando los tenants son empresas distintas del grupo (CBC, Beliv, BIA).
- **Propuesta**: catálogo por tenant y ambiente (`saas_<env>_<tenant>.bronze/silver/gold`), cada uno
  con su *managed location* en ADLS y su propio grupo de acceso; los análisis cross-tenant se hacen
  en un catálogo `saas_<env>_shared` con vistas que leen de los catálogos de cada tenant.
- **Trade-offs**: más objetos que gobernar (un catálogo por tenant) y un onboarding algo más largo,
  a cambio de un radio de impacto acotado, chargeback por tenant y aislamiento físico del
  almacenamiento. El código no cambia: solo cambia cómo se compone el nombre de la tabla.

### D4. `fail_on_critical = false` por defecto
- **Arquitectura**: 5.8 lo define como `false` por defecto.
- **Problema**: en producción se publicaría Gold sobre datos que fallaron una validación crítica
  (por ejemplo, una dimensión con versiones solapadas que duplica hechos).
- **Implementado**: `false` en `base.yaml` (respetando la arquitectura) y `true` en
  `config/env/main.yaml`.
- **Propuesta**: `true` por defecto también en `qa`; `false` solo en `dev` para poder depurar.

### D5. La clave de negocio no identifica una entrega
- **Arquitectura**: MERGE con clave `(tenant_id, fecha_proceso, transporte, ruta, material,
  tipo_entrega)`.
- **Problema**: dos entregas legítimas del mismo material, en la misma ruta, transporte y día (por
  ejemplo, dos visitas al mismo cliente) tendrían la misma clave: la segunda sobrescribiría a la
  primera y el MERGE fallaría si ambas llegan en el mismo lote. Además, si una corrección llega con
  otra `fecha_proceso`, se inserta como una fila nueva en vez de actualizar la original.
- **Evidencia**: en el dataset actual no ocurre (0 claves repetidas fuera de los duplicados
  exactos), y el check crítico `unique_business_key` lo detectaría.
- **Propuesta**: incorporar el identificador del documento de entrega de la fuente (número de
  pedido o línea) a la clave. Si la fuente no lo tiene, acordar con el dueño del dato que la
  combinación actual es única por contrato.

## Mejoras propuestas (Horizonte 2-3)

### H1. SCD2 a partir de snapshots o CDC
Generar las versiones en el pipeline (comparar el snapshot entrante contra la versión vigente, cerrar `valid_to` e insertar la nueva) en lugar de depender de que la fuente entregue la historia ya armada. En Databricks: `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` (DLT) hace esto de forma declarativa.

### H2. Ingesta incremental con Auto Loader y pipelines declarativos
Reemplazar la lectura batch del CSV por Auto Loader (`cloudFiles`): procesa solo los archivos
nuevos, guarda el estado en un checkpoint, captura columnas inesperadas en `_rescued_data` y
soporta evolución de esquema. Montado sobre Lakeflow Declarative Pipelines (antes DLT), las
reglas de calidad de este repo pasan a ser *expectations* (`expect_or_drop`, `expect_or_fail`) con
métricas integradas.

### H3. Contratos de datos por fuente
Versionar en el repo, por fuente, el esquema esperado y las reglas de calidad (hoy en código y en
`base.yaml`) como un contrato (por ejemplo, Open Data Contract Standard). El CI valida que el
pipeline lo cumple y un cambio de la fuente se negocia como un cambio de contrato, no se descubre
en producción.

### H4. Observabilidad sobre `quality_logs`
La tabla ya tiene el historial de cada check por tenant y corrida. Siguiente paso: un tablero
(Databricks SQL o Lakeview) con la tendencia de cuarentena y descartes, alertas SQL cuando un check
crítico falla o la tasa de cuarentena se dispara, y linaje de Unity Catalog para saber qué tablas
Gold se ven afectadas.

### H5. CI/CD hacia Databricks con Asset Bundles
Empaquetar el pipeline como Databricks Asset Bundle: un job por ambiente (`dev`, `qa`, `main`) con
los mismos YAML de configuración, despliegue desde GitHub Actions al mergear a `main`, y pruebas de
integración contra un workspace de `qa`. Los tenants se ejecutan en paralelo con una tarea
`for_each` en lugar del bucle secuencial actual.

### H6. Gold preparado para agentes de IA
La segunda fase del proyecto conecta agentes de IA a los datos core. Para que respondan bien:
comentarios de tabla y columna en Unity Catalog (son el contexto que lee el agente), *metric views*
o una capa semántica con las definiciones oficiales (qué es "revenue", qué es "entrega de rutina"),
vistas cross-tenant con *row filters* para que cada agente solo vea los tenants permitidos, y
acceso a través de herramientas gobernadas (Genie, funciones de UC) en lugar de SQL libre.

### H7. MongoDB como fuente operacional y Couchbase como capa de servicio
El stack de la arquitectura incluye MongoDB y Couchbase como "fuentes operacionales / serving",
pero la prueba trabaja con un CSV. Cómo encajarían sin cambiar el diseño por capas:

- **MongoDB → Bronze.** Lectura con el conector oficial de MongoDB para Spark: una carga inicial
  en batch y después *change streams* para traer solo los documentos nuevos o modificados (CDC),
  con el checkpoint en el almacenamiento del tenant. Las credenciales salen del secret scope del
  tenant (ver `infra.md`), nunca del código. Bronze guarda el documento como llega (JSON en una
  columna, más las columnas técnicas), igual que hoy guarda el CSV como strings: un campo nuevo en
  la fuente no rompe la ingesta, y el aplanado y tipado siguen siendo responsabilidad de Silver.
- **Gold → Couchbase.** Couchbase sirve lecturas de baja latencia a aplicaciones (por ejemplo, el
  top de materiales de un tenant en una app comercial). Se alimentaría con un paso de publicación
  *después* de Gold y solo con tablas Gold: un documento por clave de negocio (por ejemplo,
  `tenant::mes::material`), con *upsert* para que publicar dos veces dé el mismo resultado.
  Couchbase es una copia derivada: si se pierde, se reconstruye desde Gold.
- **Por qué así:** el pipeline sigue siendo la única fuente de verdad (Silver/Gold en Delta y Unity
  Catalog); las bases operacionales quedan en los bordes (entrada y salida) y no se mezclan con el
  procesamiento analítico.
- **Trade-offs:** el CDC desde MongoDB exige manejar borrados y reordenamientos de eventos (la
  clave de negocio y `_ingestion_timestamp` lo resuelven en el MERGE de Silver). Publicar a
  Couchbase agrega un sistema más que monitorear; si la latencia que piden las apps lo permite,
  una tabla Gold servida por un SQL warehouse evita esa copia.

### H8. Plataforma objetivo: cómo integraría todo lo anterior sobre Databricks, con criterio de costo
Las mejoras H1–H7 y los desacuerdos D1–D5 convergen en una misma plataforma. Resumen de cómo la
construiría, con tres criterios: una sola base de código multi-tenant, usar lo que la plataforma
ya resuelve antes de escribir o comprar algo, y pagar solo por el cómputo que hace trabajo.

**Arquitectura y stack.**
- *Ingesta*: Auto Loader (o Lakeflow Connect si hay conector gestionado) escribiendo Bronze como
  streaming tables, incremental y con checkpoint, en lugar del batch del archivo completo.
- *Transformación*: un único pipeline declarativo (Lakeflow Declarative Pipelines) parametrizado
  por tenant, que reutiliza las funciones puras de este repo. Las expectations reemplazan los
  checks propios (warning → `expect`, cuarentena → `expect_or_drop` más tabla de cuarentena,
  critical → `expect_or_fail`) y AUTO CDC reemplaza el MERGE SCD2 escrito a mano.
- *Gobierno*: catálogo por tenant y ambiente (`saas_<env>_<tenant>`) más un catálogo `shared`
  para el dato maestro y las vistas cross-tenant; grants por grupo, tags, row filters y linaje en
  Unity Catalog; Terraform crea cada catálogo desde el mismo YAML del tenant.
- *Orquestación*: Lakeflow Jobs con `for_each` sobre los tenants, reintentos y notificaciones; los
  tenants chicos agrupados en un job y los grandes con job y SLA propios.
- *Consumo*: SQL warehouse serverless, metric views como capa semántica, AI/BI Dashboards y Genie;
  los agentes de IA (fase 2) acceden a través de funciones de UC y heredan sus permisos. Una base
  de serving (Couchbase o Lakebase) solo cuando exista un caso de latencia demostrado.
- *Despliegue*: Asset Bundles con targets dev/qa/main, desplegados desde GitHub Actions con OIDC;
  pruebas unitarias en local (como hoy) e integración contra `qa`.

**Metodología.** Tenant como configuración y alta por GitOps (YAML + `terraform apply` + corrida
en `qa`); una tabla de control en UC para parámetros operativos que cambian seguido; productos de
datos con dueño y SLO de frescura; contratos de datos por fuente validados en CI; ADRs para toda
desviación de la arquitectura (como este archivo); revisión mensual de costos con las system
tables.

**Costo y recursos.** Serverless para jobs, pipelines y SQL (sin clusters encendidos); batch
programado según la frescura que el negocio paga, streaming solo con un caso real; Liquid
Clustering y Predictive Optimization en lugar de particiones y jobs de mantenimiento propios;
retención explícita (VACUUM, Bronze a almacenamiento frío a los N meses); dato maestro en `shared`
en vez de N copias; tags `tenant`/`producto`/`ambiente` y `system.billing` para ver el costo por
tenant desde el primer día. Para el volumen de esta prueba, un job diario serverless y un warehouse
pequeño con auto-stop cuestan decenas de dólares al mes por tenant; lo que dispara el gasto es
cómputo ocioso, streaming innecesario y copias de datos.

**Hoja de ruta.** H1 (0–3 meses): bundle, job `for_each` ejecutando el paquete actual como wheel,
catálogo por tenant, tags y serverless, sin reescribir nada. H2 (3–9 meses): pipeline declarativo,
Auto Loader, expectations, AUTO CDC, `dim_*` a `shared`, Monitoring y contratos. H3 (9+ meses):
capa semántica, Genie y agentes; serving operacional solo si hace falta.

**Trade-offs.** El pipeline declarativo ata el código a Databricks (el paquete actual es portable);
se mitiga manteniendo las reglas de negocio como funciones puras, que son las que tienen valor.
Un catálogo por tenant multiplica objetos que gobernar; se compensa con Terraform generándolos
desde el YAML. Serverless simplifica la operación pero conviene medir el costo por DBU frente a
job clusters en los tenants grandes.
